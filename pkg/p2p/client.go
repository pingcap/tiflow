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
	"sync"
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	gRPCPeer "google.golang.org/grpc/peer"
)

// MessageClientConfig is used to configure MessageClient
type MessageClientConfig struct {
	// The size of the sending channel used to buffer
	// messages before they go to gRPC.
	SendChannelSize int
	// The maximum duration for which messages wait to be batched.
	BatchSendInterval time.Duration
	// The maximum size in bytes of a batch.
	MaxBatchBytes int
	// The maximum number of messages in a batch.
	MaxBatchCount int
	// The limit of the rate at which the connection to the server is retried.
	RetryRateLimitPerSecond float64
	// The dial timeout for the gRPC client
	DialTimeout time.Duration
	// The advertised address of this node. Used for logging and monitoring purposes.
	AdvertisedAddr string
	// The version of the client for compatibility check.
	// It should be in semver format. Empty string means no check.
	ClientVersion string
}

// MessageClient is a client used to send peer messages.
// `Run` must be running before sending any message.
type MessageClient struct {
	sendCh chan *p2p.MessageEntry

	topicMu sync.RWMutex
	topics  map[string]*topicEntry

	senderID NodeID

	closeCh  chan struct{}
	isClosed atomic.Bool

	// config is read only
	config *MessageClientConfig

	// newSender is used to create a new sender.
	// It can be replaced to unit test MessageClient.
	newSenderFn func(clientStream) clientBatchSender

	connector clientConnector
}

type topicEntry struct {
	sentMessageMu sync.Mutex
	sentMessages  deque.Deque

	nextSeq  atomic.Int64
	ack      atomic.Int64
	lastSent atomic.Int64
}

// NewMessageClient creates a new MessageClient
// senderID is an identifier for the local node.
func NewMessageClient(senderID NodeID, config *MessageClientConfig) *MessageClient {
	return &MessageClient{
		sendCh:   make(chan *p2p.MessageEntry, config.SendChannelSize),
		topics:   make(map[string]*topicEntry),
		senderID: senderID,
		closeCh:  make(chan struct{}),
		config:   config,
		newSenderFn: func(stream clientStream) clientBatchSender {
			return newClientBatchSender(stream, config.MaxBatchBytes, config.MaxBatchCount)
		},
		connector: newClientConnector(),
	}
}

// Run launches background goroutines for MessageClient to work.
func (c *MessageClient) Run(
	ctx context.Context, network, addr string,
	receiverID NodeID,
	credential *security.Credential,
) (ret error) {
	defer func() {
		c.isClosed.Store(true)
		close(c.closeCh)

		log.Info("peer message client exited",
			zap.String("addr", addr),
			zap.String("capture-id", receiverID),
			zap.Error(ret))
	}()

	metricsClientCount := clientCount.With(prometheus.Labels{
		"to": addr,
	})
	metricsClientCount.Inc()
	defer metricsClientCount.Dec()

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

		gRPCClient, release, err := c.connector.Connect(clientConnectOptions{
			network:    network,
			addr:       addr,
			credential: credential,
			timeout:    c.config.DialTimeout,
		})
		if err != nil {
			log.Warn("peer-message client: failed to connect to server",
				zap.Error(err))
			continue
		}

		epoch++
		streamMeta := &p2p.StreamMeta{
			SenderId:             c.senderID,
			ReceiverId:           receiverID,
			Epoch:                epoch,
			ClientVersion:        c.config.ClientVersion,
			SenderAdvertisedAddr: c.config.AdvertisedAddr,
		}

		err = c.launchStream(ctx, gRPCClient, streamMeta)
		if cerrors.ErrPeerMessageClientPermanentFail.Equal(err) {
			release()
			return errors.Trace(err)
		}
		log.Warn("peer message client detected error, restarting", zap.Error(err))
		release()
		continue
	}
}

func (c *MessageClient) launchStream(ctx context.Context, gRPCClient p2p.CDCPeerToPeerClient, meta *p2p.StreamMeta) error {
	cancelCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	clientStream, err := gRPCClient.SendMessage(cancelCtx)
	if err != nil {
		return errors.Trace(err)
	}

	err = clientStream.Send(&p2p.MessagePacket{Meta: meta})
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.run(ctx, clientStream, cancelStream))
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

	peerAddr := unknownPeerLabel
	peer, ok := gRPCPeer.FromContext(stream.Context())
	if ok {
		peerAddr = peer.Addr.String()
	}
	metricsClientMessageCount := clientMessageCount.With(prometheus.Labels{
		"to": peerAddr,
	})

	ticker := time.NewTicker(c.config.BatchSendInterval)
	defer ticker.Stop()

	batchSender := c.newSenderFn(stream)

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
			tpk, ok := c.topics[msg.Topic]
			c.topicMu.RUnlock()
			if !ok {
				// This line should never be reachable unless there is a bug in this file.
				log.Panic("topic not found. Report a bug", zap.String("topic", msg.Topic))
			}

			// We want to assert that `msg.Sequence` is continuous within a topic.
			if old := tpk.lastSent.Swap(msg.Sequence); old != initAck && msg.Sequence != old+1 {
				log.Panic("unexpected seq of message",
					zap.String("topic", msg.Topic),
					zap.Int64("seq", msg.Sequence))
			}

			tpk.sentMessageMu.Lock()
			tpk.sentMessages.PushBack(msg)
			tpk.sentMessageMu.Unlock()

			metricsClientMessageCount.Inc()

			log.Debug("Sending Message",
				zap.String("topic", msg.Topic),
				zap.Int64("seq", msg.Sequence))
			if err := batchSender.Append(msg); err != nil {
				return errors.Trace(err)
			}
		case <-ticker.C:
			// The implementation of batchSender guarantees that
			// an empty flush does not send any message.
			if err := batchSender.Flush(); err != nil {
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

	batcher := c.newSenderFn(stream)
	for topic, tpk := range topicsCloned {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		tpk.sentMessageMu.Lock()

		if !tpk.sentMessages.Empty() {
			retryFromSeq := tpk.sentMessages.Front().(*p2p.MessageEntry).Sequence
			log.Info("peer-to-peer client retrying",
				zap.String("topic", topic),
				zap.Int64("from-seq", retryFromSeq))
		}

		for i := 0; i < tpk.sentMessages.Len(); i++ {
			msg := tpk.sentMessages.Peek(i).(*p2p.MessageEntry)
			log.Debug("retry sending msg",
				zap.String("topic", msg.Topic),
				zap.Int64("seq", msg.Sequence))

			err := batcher.Append(&p2p.MessageEntry{
				Topic:    msg.Topic,
				Content:  msg.Content,
				Sequence: msg.Sequence,
			})
			if err != nil {
				tpk.sentMessageMu.Unlock()
				return errors.Trace(err)
			}
		}

		if err := batcher.Flush(); err != nil {
			tpk.sentMessageMu.Unlock()
			return errors.Trace(err)
		}

		tpk.sentMessageMu.Unlock()
	}

	return nil
}

func (c *MessageClient) runRx(ctx context.Context, stream clientStream) error {
	peerAddr := unknownPeerLabel
	peer, ok := gRPCPeer.FromContext(stream.Context())
	if ok {
		peerAddr = peer.Addr.String()
	}
	metricsClientAckCount := clientAckCount.With(prometheus.Labels{
		"from": peerAddr,
	})

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
		case p2p.ExitReason_OK:
			break
		case p2p.ExitReason_CAPTURE_ID_MISMATCH:
			return cerrors.ErrPeerMessageClientPermanentFail.GenWithStackByArgs(resp.GetErrorMessage())
		default:
			return cerrors.ErrPeerMessageServerClosed.GenWithStackByArgs(resp.GetErrorMessage())
		}

		metricsClientAckCount.Inc()

		for _, ack := range resp.GetAck() {
			c.topicMu.RLock()
			tpk, ok := c.topics[ack.GetTopic()]
			c.topicMu.RUnlock()
			if !ok {
				log.Warn("Received ACK for unknown topic", zap.String("topic", ack.GetTopic()))
				continue
			}

			tpk.ack.Store(ack.GetLastSeq())
			tpk.sentMessageMu.Lock()
			for !tpk.sentMessages.Empty() && tpk.sentMessages.Front().(*p2p.MessageEntry).Sequence <= ack.GetLastSeq() {
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
	if c.isClosed.Load() {
		return 0, cerrors.ErrPeerMessageClientClosed.GenWithStackByArgs()
	}

	c.topicMu.RLock()
	tpk, ok := c.topics[topic]
	c.topicMu.RUnlock()

	if !ok {
		tpk = &topicEntry{
			sentMessages: deque.NewDeque(),
		}
		// the sequence = 0 is reserved.
		tpk.nextSeq.Store(1)
		c.topicMu.Lock()
		c.topics[topic] = tpk
		c.topicMu.Unlock()
	}

	nextSeq := tpk.nextSeq.Load()

	data, err := marshalMessage(value)
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrPeerMessageEncodeError, err)
	}

	msg := &p2p.MessageEntry{
		Topic:    topic,
		Content:  data,
		Sequence: nextSeq,
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

	tpk.nextSeq.Add(1)
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

	return tpk.ack.Load(), true
}
