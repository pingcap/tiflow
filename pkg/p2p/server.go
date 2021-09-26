// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package p2p

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/workerpool"
	"github.com/pingcap/ticdc/proto/p2p"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	gRPCPeer "google.golang.org/grpc/peer"
)

const (
	maxTopicPendingCount = 256
	maxPendingTaskCount  = 102400
	sendChSize           = 16
	serverTickInterval   = time.Millisecond * 200
	workerPoolSize       = 4 // TODO add a config
)

// MessageServer is an implementation of the gRPC server for the peer-to-peer system
type MessageServer struct {
	serverID SenderID

	handlers map[string]*handler

	peerLock sync.RWMutex
	peers    map[string]*cdcPeer

	pendingMessages map[topicSenderPair][]pendingMessageEntry

	acksMapLock sync.RWMutex
	acksMap     map[SenderID]map[Topic]Seq

	taskQueue chan interface{}
	pool      workerpool.WorkerPool

	isRunning int32
	closeCh   chan struct{}
}

type taskOnMessageBatch struct {
	streamMeta     *p2p.StreamMeta
	messageEntries []*p2p.MessageEntry
}

type taskOnMessageBackFill struct {
	topic   string
	entries []pendingMessageEntry
}

type taskOnRegisterPeer struct {
	streamMeta *p2p.StreamMeta
	sender     *streamSender
	clientIP   string
}

type taskOnRegisterHandler struct {
	topic   string
	handler *handler
	done    chan struct{}
}

type taskOnDeregisterHandler struct {
	topic string
	done  chan struct{}
}

// NewMessageServer creates a new MessageServer
func NewMessageServer(serverID SenderID) *MessageServer {
	return &MessageServer{
		serverID:        serverID,
		handlers:        make(map[string]*handler),
		peers:           make(map[string]*cdcPeer),
		pendingMessages: make(map[topicSenderPair][]pendingMessageEntry),
		acksMap:         make(map[SenderID]map[Topic]Seq),
		taskQueue:       make(chan interface{}, maxPendingTaskCount),
		pool:            workerpool.NewDefaultWorkerPool(workerPoolSize),
		closeCh:         make(chan struct{}),
	}
}

// Run starts the MessageServer's worker goroutines.
// It must be running to provide the gRPC service.
func (m *MessageServer) Run(ctx context.Context) error {
	atomic.StoreInt32(&m.isRunning, 1)
	defer func() {
		atomic.StoreInt32(&m.isRunning, 0)
		close(m.closeCh)
	}()

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return errors.Trace(m.run(ctx))
	})

	errg.Go(func() error {
		return errors.Trace(m.pool.Run(ctx))
	})

	return errg.Wait()
}

func (m *MessageServer) run(ctx context.Context) error {
	ticker := time.NewTicker(serverTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := m.tick(ctx); err != nil {
				return errors.Trace(err)
			}
		case task := <-m.taskQueue:
			switch task := task.(type) {
			case taskOnMessageBatch:
				for _, entry := range task.messageEntries {
					if err := m.handleMessage(ctx, task.streamMeta.GetSenderId(), entry); err != nil {
						return errors.Trace(err)
					}
				}
			case taskOnRegisterHandler:
				// TODO think about error handling here
				if err := m.registerHandler(ctx, task.topic, task.handler, task.done); err != nil {
					return errors.Trace(err)
				}
				log.Debug("handler registered", zap.String("topic", task.topic))
			case taskOnDeregisterHandler:
				m.doRemoveHandler(task.topic)
				if task.done != nil {
					close(task.done)
				}
				log.Debug("handler deregistered", zap.String("topic", task.topic))
			case taskOnMessageBackFill:
				for _, entry := range task.entries {
					if err := m.handleMessage(ctx, entry.SenderID, entry.Entry); err != nil {
						return errors.Trace(err)
					}
				}
			case taskOnRegisterPeer:
				log.Debug("taskOnRegisterPeer",
					zap.String("sender", task.streamMeta.GetSenderId()),
					zap.Int64("epoch", task.streamMeta.GetEpoch()))
				if err := m.registerPeer(ctx, task.streamMeta, task.sender, task.clientIP); err != nil {
					if cerror.ErrPeerMessageStaleConnection.Equal(err) || cerror.ErrPeerMessageDuplicateConnection.Equal(err) {
						// These two errors should not affect other peers
						if err1 := task.sender.Send(ctx, errorToRPCResponse(err)); err1 != nil {
							return errors.Trace(err)
						}
						continue // to handling the next task
					}
					return errors.Trace(err)
				}
			}
		}
	}
}

func (m *MessageServer) tick(ctx context.Context) error {
	var peersToDeregister []*cdcPeer
	defer func() {
		for _, peer := range peersToDeregister {
			// err is nil because the peers are gone already, so sending errors will not succeed.
			m.deregisterPeer(ctx, peer, nil)
		}
	}()

	m.peerLock.RLock()
	defer m.peerLock.RUnlock()

	for _, peer := range m.peers {
		var acks []*p2p.Ack
		m.acksMapLock.RLock()
		for topic, ack := range m.acksMap[peer.SenderID] {
			acks = append(acks, &p2p.Ack{
				Topic:   topic,
				LastSeq: ack,
			})
		}
		m.acksMapLock.RUnlock()
		if len(acks) == 0 {
			// No topic to ack, skip.
			continue
		}

		err := peer.sender.Send(ctx, p2p.SendMessageResponse{
			Ack: acks,
		})
		if err != nil {
			log.Warn("sending response to peer failed", zap.Error(err))
			if cerror.ErrPeerMessageInternalSenderClosed.Equal(err) {
				peersToDeregister = append(peersToDeregister, peer)
			}
		}
	}
	return nil
}

func (m *MessageServer) deregisterPeer(ctx context.Context, peer *cdcPeer, err error) {
	log.Info("Deregistering peer", zap.String("sender", peer.PeerID),
		zap.Int64("epoch", peer.Epoch))
	m.peerLock.Lock()
	delete(m.peers, peer.PeerID)
	m.peerLock.Unlock()
	if err != nil {
		peer.abortWithError(ctx, err)
	}
}

// MustAddHandler registers a handler for messages in a given topic and waits for the operation
// to complete.
func (m *MessageServer) MustAddHandler(
	ctx context.Context,
	topic string,
	tpi interface{},
	fn func(string, interface{}) error,
) (<-chan error, error) {
	doneCh, errCh, err := m.AddHandler(ctx, topic, tpi, fn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case <-doneCh:
	}
	return errCh, nil
}

// AddHandler registers a handler for messages in a given topic.
func (m *MessageServer) AddHandler(
	ctx context.Context,
	topic string,
	tpi interface{},
	fn func(string, interface{}) error) (chan struct{}, <-chan error, error) {
	tp := reflect.TypeOf(tpi)

	poolHandle := m.pool.RegisterEvent(func(ctx context.Context, argsI interface{}) error {
		args := argsI.(poolEventArgs)
		senderID := args.senderID
		entry := args.entry
		e := reflect.New(tp.Elem()).Interface()

		m.acksMapLock.Lock()
		lastAck := m.getAck(SenderID(senderID), Topic(entry.GetTopic()))
		if lastAck >= Seq(entry.Sequence) {
			// TODO add metrics
			log.Debug("skipping peer message",
				zap.String("sender-id", senderID),
				zap.String("topic", topic),
				zap.Int64("skipped-Seq", entry.Sequence),
				zap.Int64("last-ack", lastAck))
			m.acksMapLock.Unlock()
			return nil
		}

		if lastAck != 0 && Seq(entry.Sequence) > lastAck+1 {
			log.Panic("seq is skipped. Bug?", zap.Int64("last-ack", int64(lastAck)))
		}

		m.acksMapLock.Unlock()

		if err := unmarshalMessage(entry.Content, e); err != nil {
			return cerror.WrapError(cerror.ErrPeerMessageDecodeError, err)
		}

		if err := fn(senderID, e); err != nil {
			return errors.Trace(err)
		}

		m.acksMapLock.Lock()
		m.setAck(senderID, entry.GetTopic(), entry.GetSequence())
		m.acksMapLock.Unlock()

		return nil
	}).OnExit(func(err error) {
		log.Debug("error caught by workerpool", zap.Error(err))
		_ = m.scheduleTask(ctx, taskOnDeregisterHandler{
			topic: topic,
		})
	})

	handler := wrapHandler(poolHandle)
	doneCh := make(chan struct{})

	if err := m.scheduleTask(ctx, taskOnRegisterHandler{
		topic:   topic,
		handler: handler,
		done:    doneCh,
	}); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return doneCh, poolHandle.ErrCh(), nil
}

// MustRemoveHandler removes the registered handler for the given topic and wait
// for the operation to complete.
func (m *MessageServer) MustRemoveHandler(ctx context.Context, topic string) error {
	doneCh, err := m.RemoveHandler(ctx, topic)
	if err != nil {
		return errors.Trace(err)
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-doneCh:
	}

	return nil
}

// RemoveHandler removes the registered handler for the given topic.
func (m *MessageServer) RemoveHandler(ctx context.Context, topic string) (chan struct{}, error) {
	doneCh := make(chan struct{})
	if err := m.scheduleTask(ctx, taskOnDeregisterHandler{
		topic: topic,
		done:  doneCh,
	}); err != nil {
		return nil, errors.Trace(err)
	}

	return doneCh, nil
}

func (m *MessageServer) doRemoveHandler(topic string) {
	if handler, ok := m.handlers[topic]; ok {
		handler.poolHandle.Unregister()
	}
	delete(m.handlers, topic)
}

func (m *MessageServer) registerHandler(ctx context.Context, topic string, handler *handler, doneCh chan struct{}) error {
	defer close(doneCh)

	if _, ok := m.handlers[topic]; ok {
		// allow replacing the handler here would result in behaviors difficult to define
		log.Panic("duplicate handlers",
			zap.String("topic", topic))
	}

	if err := m.handlePendingMessages(ctx, topic, handler); err != nil {
		return errors.Trace(err)
	}
	m.handlers[topic] = handler
	return nil
}

// handlePendingMessages must be called with `handlerLock` taken exclusively.
func (m *MessageServer) handlePendingMessages(ctx context.Context, topic string, handler *handler) error {
	for key, entries := range m.pendingMessages {
		if key.Topic != topic {
			continue
		}

		if err := m.scheduleTask(ctx, taskOnMessageBackFill{
			topic:   topic,
			entries: entries,
		}); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (m *MessageServer) registerPeer(
	ctx context.Context,
	streamMeta *p2p.StreamMeta,
	sender *streamSender,
	clientIP string) error {
	log.Info("peer connection received",
		zap.String("sender-id", streamMeta.GetSenderId()),
		zap.String("addr", clientIP),
		zap.Int64("epoch", streamMeta.GetEpoch()))

	m.peerLock.Lock()
	peer, ok := m.peers[streamMeta.SenderId]
	if !ok {
		// no existing peer
		m.peers[streamMeta.SenderId] = &cdcPeer{
			PeerID:   streamMeta.SenderId,
			Epoch:    streamMeta.Epoch,
			SenderID: SenderID(streamMeta.SenderId),
			sender:   sender,
		}
		m.peerLock.Unlock()
	} else {
		m.peerLock.Unlock()
		// there is an existing peer
		if peer.Epoch > streamMeta.Epoch {
			log.Warn("incoming connection is stale",
				zap.String("sender-id", streamMeta.GetSenderId()),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.GetEpoch()))

			// the current stream is stale
			return cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(streamMeta.Epoch /* old */, peer.Epoch /* new */)
		} else if peer.Epoch < streamMeta.Epoch {
			err := cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(peer.Epoch /* old */, streamMeta.Epoch /* new */)
			m.deregisterPeer(ctx, peer, err)
			m.peerLock.Lock()
			m.peers[streamMeta.SenderId] = &cdcPeer{
				PeerID:   streamMeta.SenderId,
				Epoch:    streamMeta.Epoch,
				SenderID: SenderID(streamMeta.SenderId),
				sender:   sender,
			}
			m.peerLock.Unlock()
		} else {
			log.Warn("incoming connection is duplicate",
				zap.String("sender-id", streamMeta.GetSenderId()),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.GetEpoch()))

			return cerror.ErrPeerMessageDuplicateConnection.GenWithStackByArgs(streamMeta.Epoch)
		}
	}

	return nil
}

func (m *MessageServer) scheduleTask(ctx context.Context, task interface{}) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.taskQueue <- task:
	default:
		return cerror.ErrPeerMessageTaskQueueCongested.GenWithStackByArgs()
	}
	return nil
}

// SendMessage implements the gRPC call SendMessage.
func (m *MessageServer) SendMessage(stream p2p.CDCPeerToPeer_SendMessageServer) error {
	sendCh := make(chan p2p.SendMessageResponse, sendChSize)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		sender := &streamSender{
			sendCh:  sendCh,
			closeCh: make(chan struct{}),
		}
		defer sender.Close()
		if err := m.receive(ctx, stream, sender); err != nil {
			log.Warn("peer-to-peer message handler error", zap.Error(err))
			select {
			case <-ctx.Done():
				log.Warn("error receiving from peer", zap.Error(ctx.Err()))
				return errors.Trace(ctx.Err())
			case sendCh <- errorToRPCResponse(err):
			default:
				log.Warn("sendCh congested, could not send error", zap.Error(err))
				return errors.Trace(err)
			}
		}
		return nil
	})
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case resp, ok := <-sendCh:
				if !ok {
					// cancel the stream when sendCh is closed
					cancel()
					return nil
				}
				if err := stream.Send(&resp); err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	select {
	case <-ctx.Done():
		return status.New(codes.Canceled, "context canceled").Err()
	case <-m.closeCh:
		return status.New(codes.Aborted, "CDC capture closing").Err()
	}

	// NB: `errg` will NOT be waited on because due to the limitation of grpc-go, we cannot cancel Send & Recv
	// with contexts, and the only reliable way to cancel these operations is to return the gRPC call handler,
	// namely this function.
}

func (m *MessageServer) receive(ctx context.Context, stream p2p.CDCPeerToPeer_SendMessageServer, sender *streamSender) error {
	var streamMeta *p2p.StreamMeta
	for {
		failpoint.Inject("ServerInjectServerRestart", func() {
			_ = stream.Send(&p2p.SendMessageResponse{
				ExitReason: p2p.ExitReason_CONGESTED,
			})
			failpoint.Return(errors.Trace(errors.New("injected error")))
		})

		packet, err := stream.Recv()
		if err != nil {
			return errors.Trace(err)
		}

		clientIP := "unknown"
		if p, ok := gRPCPeer.FromContext(stream.Context()); ok {
			clientIP = p.Addr.String()
		}

		if streamMeta == nil {
			// streamMeta has not been received
			if packet.GetStreamMeta() == nil {
				return cerror.ErrPeerMessageUnexpected.GenWithStackByArgs(clientIP, "no stream-meta")
			}
			streamMeta = packet.GetStreamMeta()
			if streamMeta.ReceiverId != string(m.serverID) {
				return cerror.ErrPeerMessageReceiverMismatch.GenWithStackByArgs(m.serverID /* expected */, streamMeta.ReceiverId /* got */)
			}

			if err := m.scheduleTask(stream.Context(), taskOnRegisterPeer{
				streamMeta: streamMeta,
				sender:     sender,
				clientIP:   clientIP,
			}); err != nil {
				return errors.Trace(err)
			}
		}

		log.Debug("received packet", zap.String("sender", streamMeta.GetSenderId()),
			zap.Int("num-entries", len(packet.GetEntries())))

		if len(packet.GetEntries()) > 0 {
			if err := m.scheduleTask(ctx, taskOnMessageBatch{
				streamMeta:     streamMeta,
				messageEntries: packet.GetEntries(),
			}); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *MessageServer) handleMessage(ctx context.Context, senderID string, entry *p2p.MessageEntry) error {
	topic := entry.GetTopic()
	pendingMessageKey := topicSenderPair{
		Topic:    topic,
		SenderID: senderID,
	}
	handler, ok := m.handlers[topic]
	if !ok {
		// handler not found
		pendingEntries := m.pendingMessages[pendingMessageKey]
		if len(pendingEntries) > maxTopicPendingCount {
			log.Warn("Topic congested because no handler has been registered", zap.String("topic", topic))
			delete(m.pendingMessages, pendingMessageKey)
			m.peerLock.RLock()
			peer, ok := m.peers[senderID]
			m.peerLock.RUnlock()
			if ok {
				m.deregisterPeer(ctx, peer, cerror.ErrPeerMessageTopicCongested.FastGenByArgs())
			}
			return nil
		}
		m.pendingMessages[pendingMessageKey] = append(pendingEntries, pendingMessageEntry{
			SenderID: senderID,
			Entry:    entry,
		})

		return nil
	}

	// handler is found
	if err := handler.poolHandle.AddEvent(ctx, poolEventArgs{
		senderID: senderID,
		entry:    entry,
	}); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// getAckStorage must be called with `acksMapLock` taken.
func (m *MessageServer) getAck(senderID SenderID, topic Topic) Seq {
	var senderMap map[Topic]Seq
	if senderMap = m.acksMap[senderID]; senderMap == nil {
		senderMap = make(map[Topic]Seq)
		m.acksMap[senderID] = senderMap
	}
	return senderMap[topic]
}

// setAck must be called with `acksMapLock` taken.
func (m *MessageServer) setAck(senderID SenderID, topic Topic, ack Seq) {
	var senderMap map[Topic]Seq
	if senderMap = m.acksMap[senderID]; senderMap == nil {
		senderMap = make(map[Topic]Seq)
		m.acksMap[senderID] = senderMap
	}
	senderMap[topic] = ack
}

type topicSenderPair struct {
	Topic    string
	SenderID string
}

type pendingMessageEntry struct {
	SenderID string
	Entry    *p2p.MessageEntry
}

type handler struct {
	poolHandle workerpool.EventHandle
}

func wrapHandler(poolHandle workerpool.EventHandle) *handler {
	return &handler{
		poolHandle: poolHandle,
	}
}

type streamSender struct {
	mu       sync.RWMutex
	isClosed bool
	sendCh   chan<- p2p.SendMessageResponse
	closeCh  chan struct{}
}

func (s *streamSender) Send(ctx context.Context, response p2p.SendMessageResponse) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isClosed {
		return cerror.ErrPeerMessageInternalSenderClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case s.sendCh <- response:
	case <-s.closeCh:
		return cerror.ErrPeerMessageInternalSenderClosed.GenWithStackByArgs()
	}

	return nil
}

func (s *streamSender) Close() {
	close(s.closeCh)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return
	}
	s.isClosed = true
	close(s.sendCh)
}

type cdcPeer struct {
	PeerID   string
	Epoch    int64
	SenderID SenderID
	sender   *streamSender
}

func (p *cdcPeer) abortWithError(ctx context.Context, err error) {
	if err1 := p.sender.Send(ctx, errorToRPCResponse(err)); err1 != nil {
		log.Warn("could not send error to peer", zap.Error(err))
		return
	}
	log.Debug("send error to peer", zap.Error(err))
}

func errorToRPCResponse(err error) p2p.SendMessageResponse {
	if cerror.ErrPeerMessageTopicCongested.Equal(err) ||
		cerror.ErrPeerMessageTaskQueueCongested.Equal(err) {

		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_CONGESTED,
			ErrorMessage: err.Error(),
		}
	} else if cerror.ErrPeerMessageStaleConnection.Equal(err) {
		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_STALE_CONNECTION,
			ErrorMessage: err.Error(),
		}
	} else if cerror.ErrPeerMessageReceiverMismatch.Equal(err) {
		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_CAPTURE_ID_MISMATCH,
			ErrorMessage: err.Error(),
		}
	} else {
		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_OTHER,
			ErrorMessage: err.Error(),
		}
	}
}

type poolEventArgs struct {
	senderID string
	entry    *p2p.MessageEntry
}
