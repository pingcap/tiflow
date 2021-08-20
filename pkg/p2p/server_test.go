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
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/ticdc/proto/p2p"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultTimeout                  = time.Second * 10
	defaultMessageBatchSizeSmall    = 128
	defaultMessageBatchSizeLarge    = 2048
	defaultMultiClientCount         = 16
	defaultMultiTopicCount          = 64
	defaultAddHandlerDelay          = time.Millisecond * 200
	defaultAddHandlerDelayVariation = time.Millisecond * 100
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type serverSuite struct{}

var _ = check.SerialSuites(&serverSuite{})

func newServerForTesting(c *check.C, serverID string) (server *MessageServer, newClient func() (p2p.CDCPeerToPeerClient, func()), cancel func()) {
	addr := c.MkDir() + "/p2p-testing.sock"
	lis, err := net.Listen("unix", addr)
	c.Assert(err, check.IsNil)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	server = NewMessageServer(SenderID(serverID))
	p2p.RegisterCDCPeerToPeerServer(grpcServer, server)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	cancel = func() {
		grpcServer.Stop()
		wg.Wait()
	}

	newClient = func() (p2p.CDCPeerToPeerClient, func()) {
		conn, err := grpc.Dial(
			addr,
			grpc.WithInsecure(),
			grpc.WithContextDialer(func(_ context.Context, s string) (net.Conn, error) {
				return net.Dial("unix", addr)
			}))
		c.Assert(err, check.IsNil)

		cancel := func() {
			_ = conn.Close()
		}
		return p2p.NewCDCPeerToPeerClient(conn), cancel
	}
	return
}

func mustAddHandler(ctx context.Context, c *check.C, server *MessageServer, topic string, tpi interface{}, f func(string, interface{}) error) <-chan error {
	doneCh, errCh, err := server.AddHandler(ctx, topic, tpi, f)
	c.Assert(err, check.IsNil)

	select {
	case <-ctx.Done():
		c.Fatalf("AddHandler timed out")
	case <-doneCh:
	}

	return errCh
}

type testTopicContent struct {
	Index int64
}

// TODO refactor these tests to reuse more code

func (s *serverSuite) TestServerSingleClientSingleTopic(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	var lastIndex int64
	errCh := mustAddHandler(ctx, c, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		c.Assert(senderID, check.Equals, "test-client-1")
		c.Assert(i, check.FitsTypeOf, &testTopicContent{})
		content := i.(*testTopicContent)
		swapped := atomic.CompareAndSwapInt64(&lastIndex, content.Index-1, content.Index)
		c.Assert(swapped, check.IsTrue)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			c.Assert(err, check.IsNil)
		}
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	c.Assert(err, check.IsNil)

	err = stream.Send(&p2p.MessagePacket{
		StreamMeta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	c.Assert(err, check.IsNil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastAck int64
		for {
			resp, err := stream.Recv()
			c.Assert(err, check.IsNil)
			c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_NONE)

			acks := resp.Ack
			c.Assert(acks, check.HasLen, 1)
			c.Assert(acks[0].Topic, check.Equals, "test-topic-1")
			c.Assert(acks[0].LastSeq, check.GreaterEqual, lastAck)
			lastAck = acks[0].LastSeq
			log.Debug("ack received", zap.Int64("seq", lastAck))
			if lastAck == defaultMessageBatchSizeLarge {
				closer()
				cancel()
				return
			}
		}
	}()

	for i := 0; i < defaultMessageBatchSizeLarge; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		c.Assert(err, check.IsNil)

		err = stream.Send(&p2p.MessagePacket{
			Entries: []*p2p.MessageEntry{
				{
					Topic:    "test-topic-1",
					Content:  bytes,
					Sequence: int64(i + 1),
				},
			},
		})
		c.Assert(err, check.IsNil)
	}

	wg.Wait()
}

func (s *serverSuite) TestServerMultiClientSingleTopic(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-2")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	lastIndices := make(map[string]int64)
	errCh := mustAddHandler(ctx, c, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		c.Assert(senderID, check.Matches, "test-client-.*")
		c.Assert(i, check.FitsTypeOf, &testTopicContent{})
		content := i.(*testTopicContent)
		c.Assert(lastIndices[senderID], check.Equals, content.Index-1)
		lastIndices[senderID] = content.Index
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			c.Assert(err, check.IsNil)
		}
	}()

	var ackWg sync.WaitGroup
	for i := 0; i < defaultMultiClientCount; i++ {
		wg.Add(1)
		ackWg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			client, closeClient := newClient()
			stream, err := client.SendMessage(ctx)
			c.Assert(err, check.IsNil)

			go func() {
				defer closeClient()
				defer ackWg.Done()
				var lastAck int64
				for {
					resp, err := stream.Recv()
					c.Assert(err, check.IsNil)
					c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_NONE)

					acks := resp.Ack
					c.Assert(acks, check.HasLen, 1)
					c.Assert(acks[0].Topic, check.Equals, "test-topic-1")
					c.Assert(acks[0].LastSeq, check.GreaterEqual, lastAck)
					lastAck = acks[0].LastSeq
					log.Debug("ack received", zap.Int64("seq", lastAck))
					if lastAck == defaultMessageBatchSizeLarge {
						return
					}
				}
			}()

			err = stream.Send(&p2p.MessagePacket{
				StreamMeta: &p2p.StreamMeta{
					SenderId:   fmt.Sprintf("test-client-%d", i),
					ReceiverId: "test-server-2",
					Epoch:      0,
				},
			})
			c.Assert(err, check.IsNil)

			for j := 0; j < defaultMessageBatchSizeLarge; j++ {
				content := &testTopicContent{Index: int64(j + 1)}
				bytes, err := json.Marshal(content)
				c.Assert(err, check.IsNil)

				err = stream.Send(&p2p.MessagePacket{
					Entries: []*p2p.MessageEntry{
						{
							Topic:    "test-topic-1",
							Content:  bytes,
							Sequence: int64(j + 1),
						},
					},
				})
				c.Assert(err, check.IsNil)
			}
		}()
	}

	ackWg.Wait()
	closer()
	cancel()

	wg.Wait()
}

func (s *serverSuite) TestServerSingleClientMultiTopic(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	lastIndices := make([]int64, defaultMultiTopicCount)
	for i := 0; i < defaultMultiTopicCount; i++ {
		i := i
		topic := fmt.Sprintf("test-topic-%d", i)

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(defaultAddHandlerDelay + time.Duration(rand.Float64()*float64(defaultAddHandlerDelayVariation)))
			errCh := mustAddHandler(ctx, c, server, topic, &testTopicContent{}, func(senderID string, value interface{}) error {
				c.Assert(senderID, check.Equals, "test-client-1")
				c.Assert(value, check.FitsTypeOf, &testTopicContent{})
				content := value.(*testTopicContent)
				swapped := atomic.CompareAndSwapInt64(&lastIndices[i], content.Index-1, content.Index)
				c.Assert(swapped, check.IsTrue)
				return nil
			})

			select {
			case <-ctx.Done():
			case err := <-errCh:
				c.Assert(err, check.IsNil)
			}
		}()
	}

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	c.Assert(err, check.IsNil)

	err = stream.Send(&p2p.MessagePacket{
		StreamMeta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	c.Assert(err, check.IsNil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		defer closer()
		lastAcks := make(map[string]int64, defaultMultiTopicCount)
		for {
			resp, err := stream.Recv()
			c.Assert(err, check.IsNil)
			c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_NONE)

			var finishedCount int
			acks := resp.Ack
			for _, ack := range acks {
				c.Assert(ack.LastSeq, check.GreaterEqual, lastAcks[ack.Topic])
				lastAcks[ack.Topic] = ack.LastSeq
				log.Debug("ack received", zap.String("topic", ack.Topic),
					zap.Int64("seq", lastAcks[ack.Topic]))
				if ack.LastSeq == defaultMessageBatchSizeSmall {
					finishedCount++
				}
			}

			if finishedCount == defaultMultiTopicCount {
				return
			}
		}
	}()

	for i := 0; i < defaultMessageBatchSizeSmall; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		c.Assert(err, check.IsNil)

		var entries []*p2p.MessageEntry
		for j := 0; j < defaultMultiTopicCount; j++ {
			topic := fmt.Sprintf("test-topic-%d", j)
			entries = append(entries, &p2p.MessageEntry{
				Topic:    topic,
				Content:  bytes,
				Sequence: int64(i + 1),
			})
		}
		err = stream.Send(&p2p.MessagePacket{
			Entries: entries,
		})
		c.Assert(err, check.IsNil)
	}

	wg.Wait()
}

func (s *serverSuite) TestServerDeregisterHandler(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	var (
		lastIndex int64
		removed   int32
	)
	errCh := mustAddHandler(ctx, c, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		c.Assert(atomic.LoadInt32(&removed), check.Equals, int32(0))
		c.Assert(senderID, check.Equals, "test-client-1")
		c.Assert(i, check.FitsTypeOf, &testTopicContent{})
		content := i.(*testTopicContent)
		swapped := atomic.CompareAndSwapInt64(&lastIndex, content.Index-1, content.Index)
		c.Assert(swapped, check.IsTrue)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			c.Assert(err, check.ErrorMatches, ".*ErrWorkerPoolHandleCancelled.*")
		}
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	c.Assert(err, check.IsNil)

	err = stream.Send(&p2p.MessagePacket{
		StreamMeta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	c.Assert(err, check.IsNil)

	removeHandler := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doneCh, err := server.RemoveHandler(ctx, "test-topic-1")
			c.Assert(err, check.IsNil)
			select {
			case <-ctx.Done():
				c.Fatalf("RemoveHandler timed out")
			case <-doneCh:
			}
			atomic.StoreInt32(&removed, 1)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		defer closer()
		var lastAck int64
		for {
			resp, err := stream.Recv()
			c.Assert(err, check.IsNil)
			c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_NONE)
			acks := resp.Ack
			c.Assert(acks, check.HasLen, 1)
			c.Assert(acks[0].Topic, check.Equals, "test-topic-1")
			c.Assert(acks[0].LastSeq, check.GreaterEqual, lastAck)
			lastAck = acks[0].LastSeq
			log.Debug("ack received", zap.Int64("seq", lastAck))
			if lastAck >= defaultMessageBatchSizeSmall/3 {
				removeHandler()
				time.Sleep(time.Millisecond * 500)
				c.Assert(atomic.LoadInt32(&removed), check.Equals, int32(1))
				time.Sleep(time.Millisecond * 500)
				return
			}
		}
	}()

	for i := 0; i < defaultMessageBatchSizeSmall; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		c.Assert(err, check.IsNil)

		err = stream.Send(&p2p.MessagePacket{
			Entries: []*p2p.MessageEntry{
				{
					Topic:    "test-topic-1",
					Content:  bytes,
					Sequence: int64(i + 1),
				},
			},
		})
		c.Assert(err, check.IsNil)
	}

	wg.Wait()
}

func (s *serverSuite) TestServerSingleClientReconnection(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	var lastIndex int64
	errCh := mustAddHandler(ctx, c, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		c.Assert(senderID, check.Equals, "test-client-1")
		c.Assert(i, check.FitsTypeOf, &testTopicContent{})
		content := i.(*testTopicContent)
		log.Debug("handler called", zap.Int64("index", content.Index))
		c.Assert(atomic.LoadInt64(&lastIndex), check.Equals, content.Index-1)
		atomic.StoreInt64(&lastIndex, content.Index)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			c.Assert(err, check.IsNil)
		}
	}()

	var lastAck int64
	for i := 0; ; i++ {
		i := i
		client, closeClient := newClient()
		stream, err := client.SendMessage(ctx)
		c.Assert(err, check.IsNil)

		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			defer closeClient()

			err = stream.Send(&p2p.MessagePacket{
				StreamMeta: &p2p.StreamMeta{
					SenderId:   "test-client-1",
					ReceiverId: "test-server-1",
					Epoch:      int64(i),
				},
			})
			c.Assert(err, check.IsNil)

			startSeq := atomic.LoadInt64(&lastAck) + 1
			log.Debug("connection established", zap.Int64("start-seq", startSeq))
			for j := startSeq; j < startSeq+defaultMessageBatchSizeSmall; j++ {
				content := &testTopicContent{Index: j}
				bytes, err := json.Marshal(content)
				c.Assert(err, check.IsNil)

				err = stream.Send(&p2p.MessagePacket{
					Entries: []*p2p.MessageEntry{
						{
							Topic:    "test-topic-1",
							Content:  bytes,
							Sequence: j,
						},
					},
				})
				c.Assert(err, check.IsNil)
			}
			log.Debug("closing connection")
			// Makes sure that some ACKs have been sent
			time.Sleep(serverTickInterval * 2)
		}()

		subWg.Add(1)
		go func() {
			defer subWg.Done()
			for {
				resp, err := stream.Recv()
				if err != nil {
					log.Warn("Recv returned error", zap.Error(err), zap.Int64("lastAck", atomic.LoadInt64(&lastAck)))
					return
				}

				c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_NONE)

				acks := resp.Ack
				c.Assert(acks, check.HasLen, 1)
				c.Assert(acks[0].Topic, check.Equals, "test-topic-1")
				c.Assert(acks[0].LastSeq, check.GreaterEqual, lastAck)
				atomic.StoreInt64(&lastAck, acks[0].LastSeq)
				log.Debug("ack received", zap.Int64("last-ack", acks[0].LastSeq))
				if lastAck == defaultMessageBatchSizeLarge {
					return
				}
			}
		}()

		subWg.Wait()
		closeClient()

		if atomic.LoadInt64(&lastAck) == defaultMessageBatchSizeLarge {
			log.Info("test finished")
			break
		}
	}

	closer()
	cancel()

	wg.Wait()
}

func (s *serverSuite) TestServerTopicCongestedDueToNoHandler(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(c, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		c.Assert(err, check.ErrorMatches, ".*context canceled.*")
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	c.Assert(err, check.IsNil)

	err = stream.Send(&p2p.MessagePacket{
		StreamMeta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	c.Assert(err, check.IsNil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			resp, err := stream.Recv()
			c.Assert(err, check.IsNil)
			if resp.ExitReason == p2p.ExitReason_NONE {
				continue
			}
			c.Assert(resp.ExitReason, check.Equals, p2p.ExitReason_CONGESTED)
			cancel()
			return
		}
	}()

	for i := 0; i < defaultMessageBatchSizeLarge; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		c.Assert(err, check.IsNil)

		err = stream.Send(&p2p.MessagePacket{
			Entries: []*p2p.MessageEntry{
				{
					Topic:    "test-topic-1",
					Content:  bytes,
					Sequence: int64(i + 1),
				},
			},
		})
		if err != nil {
			c.Assert(err, check.ErrorMatches, "EOF")
			return
		}
	}

	wg.Wait()
}
