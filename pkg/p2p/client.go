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
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/proto/p2p"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)


// MessageClientConfig is used to configure MessageClient
type MessageClientConfig struct {
	// The size of the sending channel used to buffer
	// messages before they go to gRPC.
	SendChannelSize         int
	// The maximum duration for which messages wait to be batched.
	BatchSendInterval       time.Duration
	// The maximum size of a batch.
	MaxBatchBytes           int
	// The limit of the rate at which the connection to the server is retried.
	RetryRateLimitPerSecond float64
	// The dial timeout for the gRPC client
	DialTimeout             time.Duration
}

// MessageClient is a client used to send peer messages.
// `Run` must be running before sending any message.
type MessageClient struct {
	sendCh chan *messageEntry

	topicMu sync.RWMutex
	topics  map[string]*topicEntry

	senderID SenderID

	closeCh  chan struct{}
	isClosed int32

	config *MessageClientConfig // read only
}

type clientStream = p2p.CDCPeerToPeer_SendMessageClient

type topicEntry struct {
	sentMessageMu sync.Mutex
	sentMessages  deque.Deque

	nextSeq int64
	ack     int64

	// debug only
	lastSent Seq
}

type messageEntry struct {
	topic   Topic
	content []byte
	seq     Seq
}

// NewMessageClient creates a new MessageClient
// senderID is an identifier for the local node.
func NewMessageClient(senderID SenderID, config *MessageClientConfig) *MessageClient {
	return &MessageClient{
		sendCh:   make(chan *messageEntry, config.SendChannelSize),
		topics:   make(map[string]*topicEntry),
		senderID: senderID,
		closeCh:  make(chan struct{}),
		config:   config,
	}
}

// Run launches background goroutines for MessageClient to work.
func (c *MessageClient) Run(ctx context.Context, network, addr string, receiverID SenderID, credential *security.Credential) (ret error) {
	defer func() {
		atomic.StoreInt32(&c.isClosed, 1)
		close(c.closeCh)

		log.Info("peer message client exited",
			zap.String("addr", addr),
			zap.String("capture-id", receiverID),
			zap.Error(ret))
	}()

	securityOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	rl := rate.NewLimiter(rate.Limit(c.config.RetryRateLimitPerSecond), 1)
	epoch := int64(0)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		if err := rl.Wait(ctx); err != nil {
			return errors.Trace(err)
		}

		conn, err := grpc.Dial(
			addr,
			securityOption,
			grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return net.DialTimeout(network, s, c.config.DialTimeout)
			}))
		if err != nil {
			log.Warn("gRPC dial error, retrying", zap.Error(err))
			continue
		}

		grpcClient := p2p.NewCDCPeerToPeerClient(conn)

		cancelCtx, cancelStream := context.WithCancel(ctx)
		clientStream, err := grpcClient.SendMessage(cancelCtx)
		if err != nil {
			cancelStream()
			_ = conn.Close()
			log.Warn("establish stream to peer failed, retrying", zap.Error(err))
			continue
		}

		epoch++
		err = clientStream.Send(&p2p.MessagePacket{StreamMeta: &p2p.StreamMeta{
			SenderId:   c.senderID,
			ReceiverId: receiverID,
			Epoch:      epoch,
		}})
		if err != nil {
			cancelStream()
			_ = conn.Close()
			log.Warn("send metadata to peer failed, retrying", zap.Error(err))
			continue
		}

		if err := c.run(ctx, clientStream, cancelStream); err != nil {
			cancelStream()
			_ = conn.Close()
			if cerrors.ErrPeerMessageClientPermanentFail.Equal(err) {
				return errors.Trace(err)
			}
			log.Warn("peer message client detected error, restarting", zap.Error(err))
			continue
		}
		panic("unreachable")
	}
}

func (c *MessageClient) run(ctx context.Context, stream clientStream, cancel func()) error {
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		defer cancel()
		return c.runTx(ctx, stream)
	})

	errg.Go(func() error {
		defer cancel()
		return c.runRx(ctx, stream)
	})

	return errors.Trace(errg.Wait())
}

func (c *MessageClient) runTx(ctx context.Context, stream clientStream) error {
	if err := c.retrySending(ctx, stream); err != nil {
		return errors.Trace(err)
	}

	var (
		batchedMessages []*messageEntry
		batchedBytes    int
	)

	ticker := time.NewTicker(c.config.BatchSendInterval)
	defer ticker.Stop()

	batchSendFn := func() error {
		if len(batchedMessages) == 0 {
			return nil
		}

		var entries []*p2p.MessageEntry
		for _, msg := range batchedMessages {
			entries = append(entries, &p2p.MessageEntry{
				Topic:    msg.topic,
				Content:  msg.content,
				Sequence: msg.seq,
			})

			log.Debug("sending msg",
				zap.String("topic", msg.topic),
				zap.Int64("seq", msg.seq))
		}

		failpoint.Inject("ClientInjectStreamFailure", func() {
			log.Info("ClientInjectStreamFailure failpoint triggered")
			failpoint.Return(io.EOF)
		})

		if err := stream.Send(&p2p.MessagePacket{
			Entries: entries,
		}); err != nil {
			return errors.Trace(err)
		}

		batchedMessages = batchedMessages[:0]
		batchedBytes = 0

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case msg, ok := <-c.sendCh:
			if !ok {
				// sendCh has been closed
				return nil
			}

			c.topicMu.RLock()
			tpk, ok := c.topics[msg.topic]
			c.topicMu.RUnlock()
			if !ok {
				// This line should never be reachable unless there is a bug in this file.
				log.Panic("topic not found. Report a bug", zap.String("topic", string(msg.topic)))
			}

			if old := atomic.SwapInt64(&tpk.lastSent, msg.seq); old != 0 && msg.seq > old+1 {
				log.Panic("seq is skipped, bug?", zap.String("topic", msg.topic), zap.Int64("seq", msg.seq))
			}

			tpk.sentMessageMu.Lock()
			tpk.sentMessages.PushBack(msg)
			tpk.sentMessageMu.Unlock()

			batchedMessages = append(batchedMessages, msg)
			batchedBytes += len(msg.content) + len(msg.topic)
			if batchedBytes >= c.config.MaxBatchBytes {
				if err := batchSendFn(); err != nil {
					return errors.Trace(err)
				}
			}
		case <-ticker.C:
			if err := batchSendFn(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// retrySending retries sending messages when the gRPC stream is re-established.
func (c *MessageClient) retrySending(ctx context.Context, stream clientStream) error {
	topicsCloned := make(map[string]*topicEntry)
	c.topicMu.RLock()
	for k, v := range c.topics {
		topicsCloned[k] = v
	}
	c.topicMu.RUnlock()

	for _, tpk := range topicsCloned {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		var entries []*p2p.MessageEntry
		tpk.sentMessageMu.Lock()
		for i := 0; i < tpk.sentMessages.Len(); i++ {
			msg := tpk.sentMessages.Peek(i).(*messageEntry)
			log.Debug("retry sending msg",
				zap.String("topic", string(msg.topic)),
				zap.Int64("seq", int64(msg.seq)))
			entries = append(entries, &p2p.MessageEntry{
				Topic:    msg.topic,
				Content:  msg.content,
				Sequence: msg.seq,
			})
		}

		if err := stream.Send(&p2p.MessagePacket{
			Entries: entries,
		}); err != nil {
			tpk.sentMessageMu.Unlock()
			return errors.Trace(err)
		}

		tpk.sentMessageMu.Unlock()
	}

	return nil
}

func (c *MessageClient) runRx(ctx context.Context, stream clientStream) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		resp, err := stream.Recv()
		if err != nil {
			return errors.Trace(err)
		}
		switch resp.GetExitReason() {
		case p2p.ExitReason_NONE:
			break
		case p2p.ExitReason_CAPTURE_ID_MISMATCH:
			return cerrors.ErrPeerMessageClientPermanentFail.GenWithStackByArgs(resp.GetErrorMessage())
		default:
			return cerrors.ErrPeerMessageServerClosed.GenWithStackByArgs(resp.GetErrorMessage())
		}

		for _, ack := range resp.GetAck() {
			c.topicMu.RLock()
			tpk, ok := c.topics[ack.GetTopic()]
			c.topicMu.RUnlock()
			if !ok {
				log.Panic("Received ACK for unknown topic", zap.String("topic", ack.GetTopic()))
			}

			atomic.StoreInt64(&tpk.ack, ack.GetLastSeq())
			tpk.sentMessageMu.Lock()
			for !tpk.sentMessages.Empty() && tpk.sentMessages.Front().(*messageEntry).seq <= Seq(ack.GetLastSeq()) {
				tpk.sentMessages.PopFront()
			}
			tpk.sentMessageMu.Unlock()
		}
	}
}

// SendMessage sends a message. It will block if the client is not ready to
// accept the message for now. Once the function returns without an error,
// the client will try its best to send the message, until `Run` is canceled.
func (c *MessageClient) SendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error) {
	return c.sendMessage(ctx, topic, value, false)
}

// TrySendMessage tries to send a message. It will return ErrPeerMessageSendTryAgain
// if the client is not ready to accept the message.
func (c *MessageClient) TrySendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error) {
	return c.sendMessage(ctx, topic, value, true)
}

func (c *MessageClient) sendMessage(ctx context.Context, topic Topic, value interface{}, nonblocking bool) (seq Seq, ret error) {
	if atomic.LoadInt32(&c.isClosed) == 1 {
		return 0, cerrors.ErrPeerMessageClientClosed.GenWithStackByArgs()
	}

	c.topicMu.RLock()
	tpk, ok := c.topics[topic]
	c.topicMu.RUnlock()

	if !ok {
		tpk = &topicEntry{
			sentMessages: deque.NewDeque(),
			nextSeq:      1,
		}
		c.topicMu.Lock()
		c.topics[string(topic)] = tpk
		c.topicMu.Unlock()
	}

	nextSeq := atomic.LoadInt64(&tpk.nextSeq)

	data, err := marshalMessage(value)
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrPeerMessageEncodeError, err)
	}

	msg := &messageEntry{
		topic:   topic,
		content: data,
		seq:     nextSeq,
	}

	if nonblocking {
		select {
		case <-ctx.Done():
			return 0, errors.Trace(ctx.Err())
		case c.sendCh <- msg:
		default:
			return 0, cerrors.ErrPeerMessageSendTryAgain.GenWithStackByArgs()
		}
	} else {
		// blocking
		select {
		case <-ctx.Done():
			return 0, errors.Trace(ctx.Err())
		case <-c.closeCh:
			return 0, cerrors.ErrPeerMessageClientClosed.GenWithStackByArgs()
		case c.sendCh <- msg:
		}
	}

	atomic.AddInt64(&tpk.nextSeq, 1)
	return nextSeq, nil
}

// CurrentAck returns (s, true) if all messages with sequence less than or
// equal to s have been processed by the receiver. It returns (0, false) if
// no message for `topic` has been sent.
func (c *MessageClient) CurrentAck(topic Topic) (Seq, bool) {
	c.topicMu.RLock()
	defer c.topicMu.RUnlock()

	tpk, ok := c.topics[topic]
	if !ok {
		return 0, false
	}

	return atomic.LoadInt64(&tpk.ack), true
}
