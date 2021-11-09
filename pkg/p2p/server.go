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

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/ticdc/pkg/workerpool"
	"github.com/pingcap/ticdc/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	gRPCPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	waitUnregisterHandleTimeout = time.Second * 5
)

// MessageServerConfig stores configurations for the MessageServer
type MessageServerConfig struct {
	// The maximum number of entries to be cached for topics with no handler registered
	MaxTopicPendingCount int
	// The maximum number of unhandled internal tasks for the main thread.
	MaxPendingTaskCount int
	// The size of the channel for pending messages before sending them to gRPC.
	SendChannelSize int
	// The interval between ACKs.
	AckInterval time.Duration
	// The size of the goroutine pool for running the handlers.
	WorkerPoolSize int
}

// cdcPeer is used to store information on one connected client.
type cdcPeer struct {
	PeerID string // unique ID of the client
	Epoch  int64  // epoch is increased when the client retries.

	// necessary information on the stream.
	sender *streamHandle

	metricsAckCount prometheus.Counter
}

func newCDCPeer(senderID NodeID, epoch int64, sender *streamHandle) *cdcPeer {
	return &cdcPeer{
		PeerID: senderID,
		Epoch:  epoch,
		sender: sender,
		metricsAckCount: serverAckCount.With(prometheus.Labels{
			"to": senderID,
		}),
	}
}

func (p *cdcPeer) abortWithError(ctx context.Context, err error) {
	if err1 := p.sender.Send(ctx, errorToRPCResponse(err)); err1 != nil {
		log.Warn("could not send error to peer", zap.Error(err),
			zap.NamedError("send-err", err1))
		return
	}
	log.Debug("sent error to peer", zap.Error(err))
}

// MessageServer is an implementation of the gRPC server for the peer-to-peer system
type MessageServer struct {
	serverID NodeID

	// Each topic has at most one registered event handle,
	// registered with a WorkerPool.
	handlers map[Topic]workerpool.EventHandle

	peerLock sync.RWMutex
	peers    map[string]*cdcPeer // all currently connected clients

	// messages for topics with NO registered handle.
	// This can happen when the server is slow.
	// The upper limit of pending messages is restricted by
	// MaxTopicPendingCount in MessageServerConfig.
	pendingMessages map[topicSenderPair][]pendingMessageEntry

	// We store the last processed message's Sequence so that
	// we can periodically send ACK messages.
	acksMapLock sync.RWMutex
	acksMap     map[NodeID]map[Topic]Seq

	// taskQueue is used to store internal tasks MessageServer
	// needs to execute serially.
	taskQueue chan interface{}

	// The WorkerPool instance used to execute message handlers.
	pool workerpool.WorkerPool

	isRunning int32 // atomic
	closeCh   chan struct{}

	config *MessageServerConfig // read only
}

type taskOnMessageBatch struct {
	streamMeta     *p2p.StreamMeta
	messageEntries []*p2p.MessageEntry
}

type taskOnRegisterPeer struct {
	sender     *streamHandle
	clientAddr string // for logging
}

type taskOnRegisterHandler struct {
	topic   string
	handler workerpool.EventHandle
	done    chan struct{}
}

type taskOnDeregisterHandler struct {
	topic string
	done  chan struct{}
}

// taskDebugDelay is used in unit tests to artificially block the main
// goroutine of the server. It is not used in other places.
type taskDebugDelay struct {
	doneCh chan struct{}
}

// NewMessageServer creates a new MessageServer
func NewMessageServer(serverID NodeID, config *MessageServerConfig) *MessageServer {
	return &MessageServer{
		serverID:        serverID,
		handlers:        make(map[string]workerpool.EventHandle),
		peers:           make(map[string]*cdcPeer),
		pendingMessages: make(map[topicSenderPair][]pendingMessageEntry),
		acksMap:         make(map[NodeID]map[Topic]Seq),
		taskQueue:       make(chan interface{}, config.MaxPendingTaskCount),
		pool:            workerpool.NewDefaultWorkerPool(config.WorkerPoolSize),
		closeCh:         make(chan struct{}),
		config:          config,
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
	ticker := time.NewTicker(m.config.AckInterval)
	defer ticker.Stop()

	for {
		failpoint.Inject("ServerInjectTaskDelay", func() {
			log.Info("channel size", zap.Int("len", len(m.taskQueue)))
		})
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			m.tick(ctx)
		case task := <-m.taskQueue:
			switch task := task.(type) {
			case taskOnMessageBatch:
				for _, entry := range task.messageEntries {
					if err := m.handleMessage(ctx, task.streamMeta, entry); err != nil {
						return errors.Trace(err)
					}
				}
			case taskOnRegisterHandler:
				// FIXME better error handling here.
				// Notes: registering a handler is not expected to fail unless a context is cancelled.
				// The current error handling here will cause the server to exit, which is not ideal,
				// but will not cause service to be interrupted because the `ctx` involved here will not
				// be cancelled unless the server is exiting.
				if err := m.registerHandler(ctx, task.topic, task.handler, task.done); err != nil {
					log.Warn("registering handler failed", zap.Error(err))
					return errors.Trace(err)
				}
				log.Debug("handler registered", zap.String("topic", task.topic))
			case taskOnDeregisterHandler:
				if handler, ok := m.handlers[task.topic]; ok {
					delete(m.handlers, task.topic)
					go func() {
						err := handler.GracefulUnregister(ctx, waitUnregisterHandleTimeout)
						if err != nil {
							// This can only happen if `ctx` is cancelled or the workerpool
							// fails to unregister the handle in time, which can be caused
							// by inappropriate blocking inside the handler.
							// We use `DPanic` here so that any unexpected blocking can be
							// caught in tests, but in the same time we can provide better
							// resilience in production (`DPanic` does not panic in production).
							//
							// Note: Even if `GracefulUnregister` does fail, the handle is still
							// unregistered, only forcefully.
							log.L().DPanic("failed to gracefully unregister handle",
								zap.Error(err))
						}
						log.Debug("handler deregistered", zap.String("topic", task.topic))
						if task.done != nil {
							close(task.done)
						}
					}()
				}
			case taskOnRegisterPeer:
				log.Debug("taskOnRegisterPeer",
					zap.String("sender", task.sender.streamMeta.SenderId),
					zap.Int64("epoch", task.sender.streamMeta.Epoch))
				if err := m.registerPeer(ctx, task.sender, task.clientAddr); err != nil {
					if cerror.ErrPeerMessageStaleConnection.Equal(err) || cerror.ErrPeerMessageDuplicateConnection.Equal(err) {
						// These two errors should not affect other peers
						if err1 := task.sender.Send(ctx, errorToRPCResponse(err)); err1 != nil {
							return errors.Trace(err)
						}
						continue // to handling the next task
					}
					return errors.Trace(err)
				}
			case taskDebugDelay:
				log.Info("taskDebugDelay started")
				select {
				case <-ctx.Done():
					log.Info("taskDebugDelay canceled")
					return errors.Trace(ctx.Err())
				case <-task.doneCh:
				}
				log.Info("taskDebugDelay ended")
			}
		}
	}
}

func (m *MessageServer) tick(ctx context.Context) {
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
		for topic, ack := range m.acksMap[peer.PeerID] {
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

		peer.metricsAckCount.Inc()
		err := peer.sender.Send(ctx, p2p.SendMessageResponse{
			Ack:        acks,
			ExitReason: p2p.ExitReason_OK,
		})
		if err != nil {
			log.Warn("sending response to peer failed", zap.Error(err))
			if cerror.ErrPeerMessageInternalSenderClosed.Equal(err) {
				peersToDeregister = append(peersToDeregister, peer)
			}
		}
	}
}

func (m *MessageServer) deregisterPeer(ctx context.Context, peer *cdcPeer, err error) {
	log.Info("Deregistering peer",
		zap.String("sender", peer.PeerID),
		zap.Int64("epoch", peer.Epoch),
		zap.Error(err))

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
	case <-m.closeCh:
		return nil, cerror.ErrPeerMessageServerClosed.GenWithStackByArgs()
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

	metricsServerRepeatedMessageCount := serverRepeatedMessageCount.MustCurryWith(prometheus.Labels{
		"topic": topic,
	})

	poolHandle := m.pool.RegisterEvent(func(ctx context.Context, argsI interface{}) error {
		args := argsI.(poolEventArgs)
		sm := args.streamMeta
		entry := args.entry
		e := reflect.New(tp.Elem()).Interface()

		m.acksMapLock.Lock()
		lastAck := m.getAck(sm.SenderId, entry.GetTopic())
		if lastAck >= entry.Sequence {
			metricsServerRepeatedMessageCount.With(prometheus.Labels{
				"from": sm.SenderAdvertisedAddr,
			}).Inc()

			log.Debug("skipping peer message",
				zap.String("sender-id", sm.SenderId),
				zap.String("topic", topic),
				zap.Int64("skipped-Seq", entry.Sequence),
				zap.Int64("last-ack", lastAck))
			m.acksMapLock.Unlock()
			return nil
		}

		m.acksMapLock.Unlock()
		if lastAck != 0 && entry.Sequence > lastAck+1 {
			// We detected a message loss at seq = (lastAck+1).
			// This can only happen if the receiver's handler had failed to
			// unregister before the receiver restarted. This is expected to be
			// rare and indicates problems with the receiver's handler.
			// It is expected to happen only with extreme system latency or buggy code.
			//
			// Reports an error so that the receiver can gracefully exit.
			return cerror.ErrPeerMessageDataLost.GenWithStackByArgs(entry.Topic, lastAck+1)
		}

		if err := unmarshalMessage(entry.Content, e); err != nil {
			return cerror.WrapError(cerror.ErrPeerMessageDecodeError, err)
		}

		if err := fn(sm.SenderId, e); err != nil {
			return errors.Trace(err)
		}

		m.acksMapLock.Lock()
		m.setAck(sm.SenderId, entry.GetTopic(), entry.GetSequence())
		m.acksMapLock.Unlock()

		return nil
	}).OnExit(func(err error) {
		log.Warn("topic handler returned error", zap.Error(err))
		_ = m.scheduleTask(ctx, taskOnDeregisterHandler{
			topic: topic,
		})
	})

	doneCh := make(chan struct{})

	if err := m.scheduleTask(ctx, taskOnRegisterHandler{
		topic:   topic,
		handler: poolHandle,
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
	case <-m.closeCh:
		log.Debug("message server is closed while a handler is being removed",
			zap.String("topic", topic))
		return nil
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

func (m *MessageServer) registerHandler(ctx context.Context, topic string, handler workerpool.EventHandle, doneCh chan struct{}) error {
	defer close(doneCh)

	if _, ok := m.handlers[topic]; ok {
		// allow replacing the handler here would result in behaviors difficult to define.
		// Continuing the program when there is a risk of duplicate handlers will likely
		// result in undefined behaviors, so we panic here.
		log.Panic("duplicate handlers",
			zap.String("topic", topic))
	}

	m.handlers[topic] = handler
	if err := m.handlePendingMessages(ctx, topic); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// handlePendingMessages must be called with `handlerLock` taken exclusively.
func (m *MessageServer) handlePendingMessages(ctx context.Context, topic string) error {
	for key, entries := range m.pendingMessages {
		if key.Topic != topic {
			continue
		}

		for _, entry := range entries {
			if err := m.handleMessage(ctx, entry.StreamMeta, entry.Entry); err != nil {
				return errors.Trace(err)
			}
		}

		delete(m.pendingMessages, key)
	}

	return nil
}

func (m *MessageServer) registerPeer(
	ctx context.Context,
	sender *streamHandle,
	clientIP string) error {

	streamMeta := sender.streamMeta

	log.Info("peer connection received",
		zap.String("sender-id", streamMeta.SenderId),
		zap.String("addr", clientIP),
		zap.Int64("epoch", streamMeta.Epoch))

	m.peerLock.Lock()
	peer, ok := m.peers[streamMeta.SenderId]
	if !ok {
		// no existing peer
		m.peers[streamMeta.SenderId] = newCDCPeer(streamMeta.SenderId, streamMeta.Epoch, sender)
		m.peerLock.Unlock()
	} else {
		m.peerLock.Unlock()
		// there is an existing peer
		if peer.Epoch > streamMeta.Epoch {
			log.Warn("incoming connection is stale",
				zap.String("sender-id", streamMeta.SenderId),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.Epoch))

			// the current stream is stale
			return cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(streamMeta.Epoch /* old */, peer.Epoch /* new */)
		} else if peer.Epoch < streamMeta.Epoch {
			err := cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(peer.Epoch /* old */, streamMeta.Epoch /* new */)
			m.deregisterPeer(ctx, peer, err)
			m.peerLock.Lock()
			m.peers[streamMeta.SenderId] = newCDCPeer(streamMeta.SenderId, streamMeta.Epoch, sender)
			m.peerLock.Unlock()
		} else {
			log.Warn("incoming connection is duplicate",
				zap.String("sender-id", streamMeta.SenderId),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.Epoch))

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

func (m *MessageServer) scheduleTaskBlocking(ctx context.Context, task interface{}) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.taskQueue <- task:
	}
	return nil
}

// SendMessage implements the gRPC call SendMessage.
func (m *MessageServer) SendMessage(stream p2p.CDCPeerToPeer_SendMessageServer) error {
	packet, err := stream.Recv()
	if err != nil {
		return errors.Trace(err)
	}

	if err := m.verifyStreamMeta(packet.Meta); err != nil {
		msg := errorToRPCResponse(err)
		_ = stream.Send(&msg)
		return errors.Trace(err)
	}

	metricsServerStreamCount := serverStreamCount.With(prometheus.Labels{
		"from": packet.Meta.SenderAdvertisedAddr,
	})
	metricsServerStreamCount.Add(1)
	defer metricsServerStreamCount.Sub(1)

	streamHandle := m.newStreamHandle(packet.Meta)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		defer streamHandle.Close()
		if err := m.receive(stream, streamHandle); err != nil {
			log.Warn("peer-to-peer message handler error", zap.Error(err))
			select {
			case <-ctx.Done():
				log.Warn("error receiving from peer", zap.Error(ctx.Err()))
				return errors.Trace(ctx.Err())
			case streamHandle.sendCh <- errorToRPCResponse(err):
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
			case resp, ok := <-streamHandle.sendCh:
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

func (m *MessageServer) receive(stream p2p.CDCPeerToPeer_SendMessageServer, sender *streamHandle) error {
	clientIP := "unknown"
	if p, ok := gRPCPeer.FromContext(stream.Context()); ok {
		clientIP = p.Addr.String()
	}
	// We use scheduleTaskBlocking because blocking here is acceptable.
	// Blocking here will cause grpc-go to back propagate the pressure
	// to the client, which is what we want.
	if err := m.scheduleTaskBlocking(stream.Context(), taskOnRegisterPeer{
		sender:     sender,
		clientAddr: clientIP,
	}); err != nil {
		return errors.Trace(err)
	}

	metricsServerMessageCount := serverMessageCount.With(prometheus.Labels{
		"from": sender.streamMeta.SenderAdvertisedAddr,
	})
	metricsServerMessageBatchHistogram := serverMessageBatchHistogram.With(prometheus.Labels{
		"from": sender.streamMeta.SenderAdvertisedAddr,
	})

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

		batchSize := len(packet.GetEntries())
		log.Debug("received packet", zap.String("sender", sender.streamMeta.SenderId),
			zap.Int("num-entries", batchSize))
		metricsServerMessageBatchHistogram.Observe(float64(batchSize))

		if len(packet.GetEntries()) > 0 {
			metricsServerMessageCount.Inc()
			// See the comment above on why use scheduleTaskBlocking.
			if err := m.scheduleTaskBlocking(stream.Context(), taskOnMessageBatch{
				streamMeta:     sender.streamMeta,
				messageEntries: packet.GetEntries(),
			}); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *MessageServer) handleMessage(ctx context.Context, streamMeta *p2p.StreamMeta, entry *p2p.MessageEntry) error {
	topic := entry.GetTopic()
	pendingMessageKey := topicSenderPair{
		Topic:    topic,
		SenderID: streamMeta.SenderId,
	}
	handler, ok := m.handlers[topic]
	if !ok {
		// handler not found
		pendingEntries := m.pendingMessages[pendingMessageKey]
		if len(pendingEntries) > m.config.MaxTopicPendingCount {
			log.Warn("Topic congested because no handler has been registered", zap.String("topic", topic))
			delete(m.pendingMessages, pendingMessageKey)
			m.peerLock.RLock()
			peer, ok := m.peers[streamMeta.SenderId]
			m.peerLock.RUnlock()
			if ok {
				m.deregisterPeer(ctx, peer, cerror.ErrPeerMessageTopicCongested.FastGenByArgs())
			}
			return nil
		}
		m.pendingMessages[pendingMessageKey] = append(pendingEntries, pendingMessageEntry{
			StreamMeta: streamMeta,
			Entry:      entry,
		})

		return nil
	}

	// handler is found
	if err := handler.AddEvent(ctx, poolEventArgs{
		streamMeta: streamMeta,
		entry:      entry,
	}); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// getAckStorage must be called with `acksMapLock` taken.
func (m *MessageServer) getAck(senderID NodeID, topic Topic) Seq {
	var senderMap map[Topic]Seq
	if senderMap = m.acksMap[senderID]; senderMap == nil {
		senderMap = make(map[Topic]Seq)
		m.acksMap[senderID] = senderMap
	}
	return senderMap[topic]
}

// setAck must be called with `acksMapLock` taken.
func (m *MessageServer) setAck(senderID NodeID, topic Topic, ack Seq) {
	var senderMap map[Topic]Seq
	if senderMap = m.acksMap[senderID]; senderMap == nil {
		senderMap = make(map[Topic]Seq)
		m.acksMap[senderID] = senderMap
	}
	senderMap[topic] = ack
}

func (m *MessageServer) verifyStreamMeta(streamMeta *p2p.StreamMeta) error {
	if streamMeta == nil {
		return cerror.ErrPeerMessageIllegalMeta.GenWithStackByArgs()
	}

	if streamMeta.ReceiverId != m.serverID {
		return cerror.ErrPeerMessageReceiverMismatch.GenWithStackByArgs(
			m.serverID,            // expected
			streamMeta.ReceiverId, // actual
		)
	}

	if version.ReleaseSemver() == "" {
		// skip checking versions for some unit test cases
		return nil
	}

	clientVer, err := semver.NewVersion(streamMeta.ClientVersion)
	if err != nil {
		log.Error("MessageServer: semver failed to parse",
			zap.String("ver", streamMeta.ClientVersion),
			zap.Error(err))
		return cerror.ErrPeerMessageIllegalClientVersion.GenWithStackByArgs(streamMeta.ClientVersion)
	}

	serverVer := semver.New(version.ReleaseSemver())

	// Only allow clients with the same Major and Minor.
	if serverVer.Major != clientVer.Major || serverVer.Minor != clientVer.Minor {
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(version.ReleaseSemver())
	}

	return nil
}

type topicSenderPair struct {
	Topic    string
	SenderID string
}

type pendingMessageEntry struct {
	StreamMeta *p2p.StreamMeta
	Entry      *p2p.MessageEntry
}

type streamHandle struct {
	mu       sync.RWMutex
	isClosed bool
	sendCh   chan p2p.SendMessageResponse
	closeCh  chan struct{}

	streamMeta *p2p.StreamMeta
}

func (m *MessageServer) newStreamHandle(meta *p2p.StreamMeta) *streamHandle {
	return &streamHandle{
		sendCh:     make(chan p2p.SendMessageResponse, m.config.SendChannelSize),
		closeCh:    make(chan struct{}),
		streamMeta: meta,
	}
}

func (s *streamHandle) Send(ctx context.Context, response p2p.SendMessageResponse) error {
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

func (s *streamHandle) Close() {
	// Must close `s.closeCh` while not holding `s.mu`.
	close(s.closeCh)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return
	}
	s.isClosed = true
	close(s.sendCh)
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
			ExitReason:   p2p.ExitReason_UNKNOWN,
			ErrorMessage: err.Error(),
		}
	}
}

type poolEventArgs struct {
	streamMeta *p2p.StreamMeta
	entry      *p2p.MessageEntry
}
