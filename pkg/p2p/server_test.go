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
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	defaultTimeout                = time.Second * 10
	defaultMessageBatchSizeSmall  = 128
	defaultMessageBatchSizeMedium = 512
	defaultMessageBatchSizeLarge  = 1024
	defaultMultiClientCount       = 16
)

// read only
var defaultServerConfig4Testing = &MessageServerConfig{
	MaxPendingMessageCountPerTopic:       256,
	MaxPendingTaskCount:                  102400,
	SendChannelSize:                      16,
	SendRateLimitPerStream:               1024,
	AckInterval:                          time.Millisecond * 200,
	WorkerPoolSize:                       4,
	MaxPeerCount:                         1024,
	WaitUnregisterHandleTimeoutThreshold: time.Millisecond * 100,
}

func newServerForTesting(t *testing.T, serverID string) (server *MessageServer, newClient func() (p2p.CDCPeerToPeerClient, func()), cancel func()) {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	config := *defaultServerConfig4Testing
	server = NewMessageServer(serverID, &config)
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
				return net.Dial("tcp", addr)
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

func TestServerMultiClientSingleTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	serverID := "test-server-1"
	server, newClient, closer := newServerForTesting(t, serverID)
	defer closer()

	// Avoids server returning error due to congested topic.
	server.config.MaxPendingMessageCountPerTopic = math.MaxInt64

	localCh := make(chan RawMessageEntry, defaultMessageBatchSizeMedium)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx, localCh)
		require.Regexp(t, ".*context canceled.*", err)
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
					ReceiverId: serverID,
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
					if lastAck == defaultMessageBatchSizeMedium {
						return
					}
				}
			}()

			for j := 0; j < defaultMessageBatchSizeMedium; j++ {
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

	time.Sleep(1 * time.Second)

	lastIndices := sync.Map{}
	errCh := mustAddHandler(ctx, t, server, "test-topic-1", &testTopicContent{}, func(senderID string, i interface{}) error {
		if strings.Contains(senderID, "server") {
			require.Equal(t, serverID, senderID)
		} else {
			require.Regexp(t, "test-client-.*", senderID)
		}
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)

		v, ok := lastIndices.Load(senderID)
		if !ok {
			require.Equal(t, int64(1), content.Index)
		} else {
			require.Equal(t, content.Index-1, v.(int64))
		}
		lastIndices.Store(senderID, content.Index)
		return nil
	})

	// test local client
	wg.Add(1)
	ackWg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < defaultMessageBatchSizeLarge; j++ {
			content := &testTopicContent{Index: int64(j + 1)}
			select {
			case <-ctx.Done():
				t.Fail()
			case localCh <- RawMessageEntry{
				topic: "test-topic-1",
				value: content,
			}:
			}
		}
		go func() {
			defer ackWg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					t.Fail()
				case <-ticker.C:
					v, ok := lastIndices.Load(serverID)
					if !ok {
						continue
					}
					idx := v.(int64)
					if idx == defaultMessageBatchSizeLarge {
						return
					}
				}
			}
		}()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case err := <-errCh:
			require.Error(t, err)
			require.Regexp(t, ".*ErrWorkerPoolHandleCancelled.*", err.Error())
		}
	}()

	ackWg.Wait()

	err := server.SyncRemoveHandler(ctx, "test-topic-1")
	require.NoError(t, err)

	// double remove to test idempotency.
	err = server.SyncRemoveHandler(ctx, "test-topic-1")
	require.NoError(t, err)

	closer()
	cancel()

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
		err := server.Run(ctx, nil)
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
		err := server.Run(cctx, nil)
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
	require.Regexp(t, ".*message server is closing.*", clientErr.Error())

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
		err := server.Run(ctx, nil)
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
		err := server.Run(ctx, nil)
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
	_ = stream.CloseSend()

	require.Eventually(t, func() bool {
		server.peerLock.RLock()
		defer server.peerLock.RUnlock()
		_, ok := server.peers["test-client-1"]
		return ok
	}, time.Millisecond*100, time.Millisecond*10)

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

func TestServerOldConnectionStale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx, nil)
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
			Epoch:      4, // a smaller epoch
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		server.peerLock.RLock()
		defer server.peerLock.RUnlock()
		_, ok := server.peers["test-client-1"]
		return ok
	}, time.Millisecond*100, time.Millisecond*10)

	stream1, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream1.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-1",
			Epoch:      5, // a larger epoch
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
		err := server.Run(ctx, nil)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var lastIndex int64
	_ = mustAddHandler(ctx, t, server, "test-topic-1",
		&testTopicContent{}, func(senderID string, i interface{}) error {
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
		err := server.Run(serverCtx, nil)
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
		err := server.Run(serverCtx, nil)
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

func TestReceiverIDMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx, nil)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	client, closeClient := newClient()
	defer closeClient()

	stream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = stream.Send(&p2p.MessagePacket{
		Meta: &p2p.StreamMeta{
			SenderId:   "test-client-1",
			ReceiverId: "test-server-2", // mismatch
			Epoch:      0,
		},
	})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, p2p.ExitReason_CAPTURE_ID_MISMATCH, resp.ExitReason)

	cancel()
	wg.Wait()
}

func TestServerDataLossAfterUnregisterHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*20)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-1")
	defer closer()

	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(serverCtx, nil)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var called int32
	mustAddHandler(ctx, t, server, "blocking-handler", &testTopicContent{}, func(_ string, _ interface{}) error {
		if atomic.AddInt32(&called, 1) == 2 {
			time.Sleep(1 * time.Second)
		}
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

	err = stream.Send(&p2p.MessagePacket{
		Entries: []*p2p.MessageEntry{
			{
				Topic:    "blocking-handler",
				Content:  []byte(`{"index":1}`), // just a dummy message
				Sequence: 1,
			},
			{
				Topic:    "blocking-handler",
				Content:  []byte(`{"index":2}`), // just a dummy message
				Sequence: 2,
			},
			{
				// This message will be lost
				Topic:    "blocking-handler",
				Content:  []byte(`{"index":3}`), // just a dummy message
				Sequence: 3,
			},
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&called) == 2
	}, time.Millisecond*500, time.Millisecond*10)

	err = server.SyncRemoveHandler(ctx, "blocking-handler")

	require.NoError(t, err)

	// re-registers the handler
	errCh := mustAddHandler(ctx, t, server, "blocking-handler", &testTopicContent{}, func(_ string, msg interface{}) error {
		require.Fail(t, "should not have been called", "value: %v", msg)
		return nil
	})

	err = stream.Send(&p2p.MessagePacket{
		Entries: []*p2p.MessageEntry{
			{
				// This message should never have reached the handler because the
				// message with Sequence = 2 has been lost.
				Topic:    "blocking-handler",
				Content:  []byte(`{"index":4}`), // just a dummy message
				Sequence: 4,
			},
		},
	})
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.Fail(t, "test timed out: TestServerDataLossAfterUnregisterHandle")
	case err = <-errCh:
	}
	require.Error(t, err)
	require.Regexp(t, "ErrPeerMessageDataLost", err.Error())

	cancel()
	wg.Wait()
}

func TestServerDeregisterPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	server, newClient, closer := newServerForTesting(t, "test-server-2")
	defer closer()

	// Avoids server returning error due to congested topic.
	server.config.MaxPendingMessageCountPerTopic = math.MaxInt64

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx, nil)
		require.Regexp(t, ".*context canceled.*", err)
	}()

	var sendGroup sync.WaitGroup
	sendGroup.Add(1)
	client, closeClient := newClient()
	go func() {
		defer sendGroup.Done()
		stream, err := client.SendMessage(ctx)
		require.NoError(t, err)

		err = stream.Send(&p2p.MessagePacket{
			Meta: &p2p.StreamMeta{
				SenderId:   "test-client-1",
				ReceiverId: "test-server-2",
				Epoch:      0,
			},
		})
		require.NoError(t, err)
	}()
	sendGroup.Wait()
	time.Sleep(1 * time.Second)

	server.peerLock.Lock()
	require.Equal(t, 1, len(server.peers))
	server.peerLock.Unlock()
	require.Nil(t, server.ScheduleDeregisterPeerTask(ctx, "test-client-1"))
	time.Sleep(1 * time.Second)
	server.peerLock.Lock()
	require.Equal(t, 0, len(server.peers))
	server.peerLock.Unlock()

	closeClient()
	closer()
	cancel()
	wg.Wait()
}
