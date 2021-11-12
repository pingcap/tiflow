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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/proto/p2p"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultTimeout                  = time.Second * 10
	defaultMessageBatchSizeSmall    = 128
	defaultMessageBatchSizeMedium   = 512
	defaultMessageBatchSizeLarge    = 1024
	defaultMultiClientCount         = 16
	defaultMultiTopicCount          = 64
	defaultAddHandlerDelay          = time.Millisecond * 200
	defaultAddHandlerDelayVariation = time.Millisecond * 100
)

// read only
var defaultServerConfig4Testing = &MessageServerConfig{
	MaxTopicPendingCount: 256,
	MaxPendingTaskCount:  102400,
	SendChannelSize:      16,
	AckInterval:          time.Millisecond * 200,
	WorkerPoolSize:       4,
	MaxPeerCount:         1024,
}

func newServerForTesting(t *testing.T, serverID string) (server *MessageServer, newClient func() (p2p.CDCPeerToPeerClient, func()), cancel func()) {
	addr := t.TempDir() + "/p2p-testing.sock"
	lis, err := net.Listen("unix", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	server = NewMessageServer(serverID, defaultServerConfig4Testing)
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
		require.NoError(t, err)

		cancel := func() {
			_ = conn.Close()
		}
		return p2p.NewCDCPeerToPeerClient(conn), cancel
	}
	return
}

func mustAddHandler(ctx context.Context, t *testing.T, server *MessageServer, topic string, tpi interface{}, f func(string, interface{}) error) <-chan error {
	doneCh, errCh, err := server.AddHandler(ctx, topic, tpi, f)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatalf("AddHandler timed out")
	case <-doneCh:
	}

	return errCh
}

type testTopicContent struct {
	Index int64
}

// TODO refactor these tests to reuse more code
func TestServerSingleClientSingleTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var lastIndex int64
	errCh := mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		swapped := atomic.CompareAndSwapInt64(&lastIndex, content.Index-1, content.Index)
		require.True(t, swapped)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			require.NoError(t, err)
		}
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      1,
		},
	})
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastAck int64
		for {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, p2p.ExitReason_OK, resp.ExitReason)

			acks := resp.Ack
			require.Len(t, acks, 1)
			require.Equal(t, "test-topic-1", acks[0].Topic)
			require.GreaterOrEqual(t, acks[0].LastSeq, lastAck)
			lastAck = acks[0].LastSeq
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
		require.NoError(t, err)

		err = stream.Send(&p2p.MessagePacket{
			Entries: []*p2p.MessageEntry{
				{
					Topic:    "test-topic-1",
					Content:  bytes,
					Sequence: int64(i + 1),
				},
			},
		})
		require.NoError(t, err)
	}

	wg.Wait()
}

func TestServerMultiClientSingleTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-2")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err)
	}()

	lastIndices := make(map[string]int64)
	errCh := mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Regexp(t, "test-client-.*", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index-1, lastIndices[senderID])
		lastIndices[senderID] = content.Index
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			require.NoError(t, err)
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
			require.NoError(t, err)

			err = stream.Send(&p2p.MessagePacket{
				Meta: &p2p.StreamMeta{
					SenderId:   fmt.Sprintf("test-client-%d", i),
					ReceiverId: "test-server-2",
					Epoch:      0,
				},
			})
			require.NoError(t, err)

			go func() {
				defer closeClient()
				defer ackWg.Done()
				var lastAck int64
				for {
					resp, err := stream.Recv()
					require.NoError(t, err)
					require.Equal(t, p2p.ExitReason_OK, resp.ExitReason)

					acks := resp.Ack
					require.Len(t, acks, 1)
					require.Equal(t, "test-topic-1", acks[0].Topic)
					require.GreaterOrEqual(t, acks[0].LastSeq, lastAck)
					lastAck = acks[0].LastSeq
					if lastAck == defaultMessageBatchSizeLarge {
						return
					}
				}
			}()

			for j := 0; j < defaultMessageBatchSizeLarge; j++ {
				content := &testTopicContent{Index: int64(j + 1)}
				bytes, err := json.Marshal(content)
				require.NoError(t, err)

				err = stream.Send(&p2p.MessagePacket{
					Entries: []*p2p.MessageEntry{
						{
							Topic:    "test-topic-1",
							Content:  bytes,
							Sequence: int64(j + 1),
						},
					},
				})
				require.NoError(t, err)
			}
		}()
	}

	ackWg.Wait()
	closer()
	cancel()

	wg.Wait()
}

func TestServerSingleClientMultiTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	lastIndices := make([]int64, defaultMultiTopicCount)
	for i := 0; i < defaultMultiTopicCount; i++ {
		i := i
		topic := fmt.Sprintf("test-topic-%d", i)

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(defaultAddHandlerDelay + time.Duration(rand.Float64()*float64(defaultAddHandlerDelayVariation)))
			errCh := mustAddHandler(ctx, t, server, topic, &testTopicContent{}, func(senderID string, value interface{}) error {
				require.Equal(t, "test-client-1", senderID)
				require.IsType(t, &testTopicContent{}, value)
				content := value.(*testTopicContent)
				old := atomic.SwapInt64(&lastIndices[i], content.Index)
				require.Equal(t, content.Index-1, old)
				return nil
			})

			select {
			case <-ctx.Done():
			case err := <-errCh:
				require.NoError(t, err)
			}
		}()
	}

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		defer closer()
		lastAcks := make(map[string]int64, defaultMultiTopicCount)
		for {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, p2p.ExitReason_OK, resp.ExitReason)

			var finishedCount int
			acks := resp.Ack
			for _, ack := range acks {
				require.GreaterOrEqual(t, ack.LastSeq, lastAcks[ack.Topic])
				lastAcks[ack.Topic] = ack.LastSeq
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
		require.NoError(t, err)

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
		require.NoError(t, err)
	}

	wg.Wait()
}

func TestServerDeregisterHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var (
		lastIndex int64
		removed   int32
	)
	errCh := mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, int32(0), atomic.LoadInt32(&removed))
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		swapped := atomic.CompareAndSwapInt64(&lastIndex, content.Index-1, content.Index)
		require.True(t, swapped)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			require.Regexp(t, ".*ErrWorkerPoolHandleCancelled.*", err.Error())
		}
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	removeHandler := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doneCh, err := server.RemoveHandler(ctx, "test-topic-1")
			require.NoError(t, err)
			select {
			case <-ctx.Done():
				require.FailNow(t, "RemoveHandler timed out")
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
			require.NoError(t, err)
			require.Equal(t, p2p.ExitReason_OK, resp.ExitReason)
			acks := resp.Ack
			require.Len(t, acks, 1)
			require.Equal(t, "test-topic-1", acks[0].Topic)
			require.GreaterOrEqual(t, acks[0].LastSeq, lastAck)
			lastAck = acks[0].LastSeq
			if lastAck >= defaultMessageBatchSizeSmall/3 {
				removeHandler()
				time.Sleep(time.Millisecond * 500)
				require.Equal(t, int32(1), atomic.LoadInt32(&removed))
				time.Sleep(time.Millisecond * 500)
				return
			}
		}
	}()

	for i := 0; i < defaultMessageBatchSizeSmall; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		require.NoError(t, err)

		err = stream.Send(&p2p.MessagePacket{
			Entries: []*p2p.MessageEntry{
				{
					Topic:    "test-topic-1",
					Content:  bytes,
					Sequence: int64(i + 1),
				},
			},
		})
		require.NoError(t, err)
	}

	wg.Wait()
}

func TestServerSingleClientReconnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var lastIndex int64
	errCh := mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index-1, atomic.LoadInt64(&lastIndex))
		atomic.StoreInt64(&lastIndex, content.Index)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			require.NoError(t, err)
		}
	}()

	var lastAck int64
	for i := 0; ; i++ {
		i := i
		client, closeClient := newClient()

		stream, err := client.SendMessage(ctx)
		require.NoError(t, err)

		err = stream.Send(&p2p.MessagePacket{
			Meta: &p2p.StreamMeta{
				SenderId:   "test-client-1",
				ReceiverId: "test-server-1",
				Epoch:      int64(i),
			},
		})
		require.NoError(t, err)

		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			defer closeClient()

			startSeq := atomic.LoadInt64(&lastAck) + 1
			for j := startSeq; j < startSeq+defaultMessageBatchSizeSmall; j++ {
				content := &testTopicContent{Index: j}
				bytes, err := json.Marshal(content)
				require.NoError(t, err)

				err = stream.Send(&p2p.MessagePacket{
					Entries: []*p2p.MessageEntry{
						{
							Topic:    "test-topic-1",
							Content:  bytes,
							Sequence: j,
						},
					},
				})
				require.NoError(t, err)
			}
			// Makes sure that some ACKs have been sent
			time.Sleep(defaultServerConfig4Testing.AckInterval * 2)
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

				require.Equal(t, p2p.ExitReason_OK, resp.ExitReason)

				acks := resp.Ack
				require.Len(t, acks, 1)
				require.Equal(t, "test-topic-1", acks[0].Topic)
				require.GreaterOrEqual(t, acks[0].LastSeq, atomic.LoadInt64(&lastAck))
				atomic.StoreInt64(&lastAck, acks[0].LastSeq)
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

func TestServerClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	cctx, cancelServer := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(cctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	cancelServer()

	var clientErr error
	require.Eventually(t, func() bool {
		_, clientErr = stream.Recv()
		return clientErr != nil
	}, time.Second*1, time.Millisecond*10)
	require.Regexp(t, ".*CDC capture closing.*", clientErr.Error())

	wg.Wait()
}

func TestServerTopicCongestedDueToNoHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			resp, err := stream.Recv()
			require.NoError(t, err)
			if resp.ExitReason == p2p.ExitReason_OK {
				continue
			}
			require.Equal(t, p2p.ExitReason_CONGESTED, resp.ExitReason)
			cancel()
			return
		}
	}()

	for i := 0; i < defaultMessageBatchSizeMedium; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		require.NoError(t, err)

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
			require.Regexp(t, "EOF", err.Error())
			return
		}
	}

	wg.Wait()
}

func TestServerIncomingConnectionStale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	client, closeClient := newClient()
	defer closeClient()

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := client.SendMessage(streamCtx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      5, // a larger epoch
		},
	})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, resp.ExitReason, p2p.ExitReason_OK)

	stream, err = client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      4, // a smaller epoch
		},
	})
	require.NoError(t, err)

	for {
		resp, err := stream.Recv()
		require.NoError(t, err)
		if resp.ExitReason == p2p.ExitReason_OK {
			continue
		}
		require.Equal(t, resp.ExitReason, p2p.ExitReason_STALE_CONNECTION)
		break
	}

	cancel()
	wg.Wait()
}

func TestServerRepeatedMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var lastIndex int64
	_ = mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index-1, atomic.LoadInt64(&lastIndex))
		atomic.StoreInt64(&lastIndex, content.Index)
		return nil
	})

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(
				t,
				p2p.ExitReason_OK,
				resp.ExitReason,
				"unexpected error: %s",
				resp.ErrorMessage,
			)
			if resp.Ack[0].LastSeq == defaultMessageBatchSizeSmall {
				return
			}
		}
	}()

	for i := 0; i < defaultMessageBatchSizeSmall; i++ {
		content := &testTopicContent{Index: int64(i + 1)}
		bytes, err := json.Marshal(content)
		require.NoError(t, err)

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
			require.Regexp(t, "EOF", err.Error())
			return
		}

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
			require.Regexp(t, "EOF", err.Error())
			return
		}
	}

	wg.Wait()
}

func TestServerExitWhileAddingHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, _, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(serverCtx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	waitCh := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.scheduleTask(ctx, taskDebugDelay{
			doneCh: waitCh,
		})
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		_, err := server.SyncAddHandler(ctx, "test-topic", &testTopicContent{}, func(s string, i interface{}) error {
			return nil
		})
		require.Error(t, err)
		require.Regexp(t, ".*ErrPeerMessageServerClosed.*", err.Error())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 300)
		cancelServer()
	}()

	wg.Wait()
}

func TestServerExitWhileRemovingHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, _, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(serverCtx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	waitCh := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := server.SyncAddHandler(ctx, "test-topic", &testTopicContent{}, func(s string, i interface{}) error {
			return nil
		})
		require.NoError(t, err)
		err = server.scheduleTask(ctx, taskDebugDelay{
			doneCh: waitCh,
		})
		require.NoError(t, err)
		time.Sleep(time.Millisecond * 100)
		err = server.SyncRemoveHandler(ctx, "test-topic")
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 500)
		cancelServer()
	}()

	wg.Wait()
}

func TestServerVersionsIncompatible(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	// enables version check
	server.config.ServerVersion = "5.2.0"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:      "test-client-1",
			ReceiverId:    "test-server-1",
			Epoch:         0,
			ClientVersion: "5.1.0",
		},
	})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, p2p.ExitReason_UNKNOWN, resp.ExitReason)
	require.Regexp(t, ".*illegal version.*", resp.String())

	cancel()
	wg.Wait()
}
