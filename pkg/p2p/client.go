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
	"encoding/json"
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
	"google.golang.org/grpc"
)

const (
	clientSendChanSize      = 128
	clientBatchSendInterval = time.Millisecond * 200
	clientSendMaxBatchBytes = 8192
)

type MessageClient struct {
	sendCh chan *messageEntry

	topicMu sync.RWMutex
	topics  map[string]*topicEntry

	senderID SenderID

	closeCh  chan struct{}
	isClosed int32
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

func NewMessageClient(senderID SenderID) *MessageClient {
	return &MessageClient{
		sendCh:   make(chan *messageEntry, clientSendChanSize),
		topics:   make(map[string]*topicEntry),
		senderID: senderID,
		closeCh:  make(chan struct{}),
	}
}

func (c *MessageClient) Run(ctx context.Context, network, addr string, receiverID SenderID, credential *security.Credential) error {
	defer func() {
		atomic.StoreInt32(&c.isClosed, 1)
		close(c.closeCh)
	}()

	securityOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	epoch := int64(0)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(err)
		default:
		}

		// TODO add configuration Dial timeout
		conn, err := grpc.Dial(
			addr,
			securityOption,
			grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return net.Dial(network, s)
			}))
		if err != nil {
			return errors.Trace(err)
		}

		grpcClient := p2p.NewCDCPeerToPeerClient(conn)
		clientStream, err := grpcClient.SendMessage(ctx)
		if err != nil {
			_ = conn.Close()
			return errors.Trace(err)
		}

		epoch++
		err = clientStream.Send(&p2p.MessagePacket{StreamMeta: &p2p.StreamMeta{
			SenderId:   string(c.senderID),
			ReceiverId: string(receiverID),
			Epoch:      epoch,
		}})
		if err != nil {
			_ = conn.Close()
			return errors.Trace(err)
		}

		if err := c.run(ctx, clientStream); err != nil {
			if cerrors.ErrPeerMessageTopicCongested.Equal(err) || errors.Cause(err) == io.EOF {
				log.Warn("client detected recoverable error, restarting", zap.Error(err))
				continue
			}
			return errors.Trace(err)
		}
	}
}

func (c *MessageClient) run(ctx context.Context, stream clientStream) error {
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return c.runTx(ctx, stream)
	})

	errg.Go(func() error {
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

	ticker := time.NewTicker(clientBatchSendInterval)
	defer ticker.Stop()

	batchSendFn := func() error {
		if len(batchedMessages) == 0 {
			return nil
		}

		var entries []*p2p.MessageEntry
		for _, msg := range batchedMessages {
			entries = append(entries, &p2p.MessageEntry{
				Topic:    string(msg.topic),
				Content:  msg.content,
				Sequence: int64(msg.seq),
			})

			log.Debug("sending msg",
				zap.String("topic", string(msg.topic)),
				zap.Int64("seq", int64(msg.seq)))
		}

		failpoint.Inject("ClientInjectStreamFailure", func() {
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
			tpk, ok := c.topics[string(msg.topic)]
			c.topicMu.RUnlock()
			if !ok {
				// This line should never be reachable unless there is a bug in this file.
				log.Panic("topic not found. Report a bug", zap.String("topic", string(msg.topic)))
			}

			if old := atomic.SwapInt64((*int64)(&tpk.lastSent), int64(msg.seq)); old != 0 && int64(msg.seq) > old+1 {
				log.Panic("seq is skipped, bug?", zap.String("topic", string(msg.topic)), zap.Int64("seq", int64(msg.seq)))
			}

			tpk.sentMessageMu.Lock()
			tpk.sentMessages.PushBack(msg)
			tpk.sentMessageMu.Unlock()

			batchedMessages = append(batchedMessages, msg)
			batchedBytes += len(msg.content) + len(msg.topic)
			if batchedBytes >= clientSendMaxBatchBytes {
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
				Topic:    string(msg.topic),
				Content:  msg.content,
				Sequence: int64(msg.seq),
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
		case p2p.ExitReason_CONGESTED:
			return cerrors.ErrPeerMessageTopicCongested.GenWithStackByArgs()
		case p2p.ExitReason_STALE_CONNECTION:
			return cerrors.ErrPeerMessageStaleConnection.GenWithStackByArgs()
		case p2p.ExitReason_DUPLICATE_CONNECTION:
			return cerrors.ErrPeerMessageDuplicateConnection.GenWithStackByArgs()
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

func (c *MessageClient) SendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error) {
	return c.sendMessage(ctx, topic, value, false)
}

func (c *MessageClient) TrySendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error) {
	return c.sendMessage(ctx, topic, value, true)
}

func (c *MessageClient) sendMessage(ctx context.Context, topic Topic, value interface{}, nonblocking bool) (seq Seq, ret error) {
	if atomic.LoadInt32(&c.isClosed) == 1 {
		return 0, cerrors.ErrPeerMessageClientClosed.GenWithStackByArgs()
	}

	c.topicMu.RLock()
	tpk, ok := c.topics[string(topic)]
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

	jsonStr, err := json.Marshal(value)
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrPeerMessageEncodeError, err)
	}

	msg := &messageEntry{
		topic:   topic,
		content: jsonStr,
		seq:     Seq(nextSeq),
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
	return Seq(nextSeq), nil
}

func (c *MessageClient) CurrentAck(topic Topic) (Seq, bool) {
	c.topicMu.RLock()
	defer c.topicMu.RUnlock()

	tpk, ok := c.topics[string(topic)]
	if !ok {
		return 0, false
	}

	return Seq(atomic.LoadInt64(&tpk.ack)), true
}
