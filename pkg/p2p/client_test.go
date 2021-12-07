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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/proto/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockClientBatchSender struct {
	mock.Mock

	sendCnt int32 // atomic
}

func (s *mockClientBatchSender) Append(msg messageEntry) error {
	args := s.Called(msg)
	atomic.AddInt32(&s.sendCnt, 1)
	return args.Error(0)
}

func (s *mockClientBatchSender) Flush() error {
	args := s.Called()
	return args.Error(0)
}

type mockClientConnector struct {
	mock.Mock
}

func (c *mockClientConnector) Connect(opts clientConnectOptions) (p2p.CDCPeerToPeerClient, cancelFn, error) {
	args := c.Called(opts)
	return args.Get(0).(p2p.CDCPeerToPeerClient), args.Get(1).(cancelFn), args.Error(2)
}

type testMessage struct {
	Value int `json:"value"`
}

var clientConfigForUnitTesting = &MessageClientConfig{
	SendChannelSize:         0, // unbuffered channel to make tests more reliable
	BatchSendInterval:       time.Second,
	MaxBatchBytes:           math.MaxInt64,
	MaxBatchCount:           math.MaxInt64,
	RetryRateLimitPerSecond: 999.0,
	ClientVersion:           "v5.4.0", // a fake version
	AdvertisedAddr:          "fake-addr:8300",
}

func TestMessageClientBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	client := NewMessageClient("node-1", clientConfigForUnitTesting)
	sender := &mockClientBatchSender{}
	client.newSenderFn = func(stream clientStream) clientBatchSender {
		return sender
	}
	connector := &mockClientConnector{}
	grpcClient := &mockCDCPeerToPeerClient{}
	client.connector = connector
	var closed int32
	connector.On("Connect", mock.Anything).Return(
		grpcClient,
		func() {
			require.Equal(t, int32(0), atomic.LoadInt32(&closed))
			atomic.StoreInt32(&closed, 1)
		},
		nil,
	).Run(func(_ mock.Arguments) {
		atomic.StoreInt32(&closed, 0)
	})

	// Test point 1: Connecting to the server and sends the meta
	grpcStream := newMockSendMessageClient(ctx)
	grpcClient.On("SendMessage", mock.Anything, []grpc.CallOption(nil)).Return(
		grpcStream,
		nil,
	)
	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		packet := args.Get(0).(*p2p.MessagePacket)
		require.EqualValues(t, &p2p.StreamMeta{
			SenderId:             "node-1",
			ReceiverId:           "node-2",
			Epoch:                1, // 1 is the initial epoch
			ClientVersion:        "v5.4.0",
			SenderAdvertisedAddr: "fake-addr:8300",
		}, packet.Meta)
	})
	grpcStream.On("Recv").Return(nil, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx, "", "", "node-2", &security.Credential{})
		require.Error(t, err)
		require.Regexp(t, "context canceled", err.Error())
	}()

	// wait for the stream meta to be received
	require.Eventuallyf(t, func() bool {
		return atomic.LoadInt32(&grpcStream.msgCount) > 0
	}, time.Second*1, time.Millisecond*10, "meta should have been received")

	connector.AssertExpectations(t)
	grpcClient.AssertExpectations(t)

	// Test point 2: Send a message
	sender.On("Append", &p2p.MessageEntry{
		Topic:    "topic-1",
		Content:  []byte(`{"value":1}`),
		Sequence: 1,
	}).Return(nil)
	seq, err := client.TrySendMessage(ctx, "topic-1", &testMessage{Value: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), seq)
	require.Eventuallyf(t, func() bool {
		return atomic.LoadInt32(&sender.sendCnt) == 1
	}, time.Second*1, time.Millisecond*10, "message should have been received")
	sender.AssertExpectations(t)

	// Test point 3: CurrentAck works for a known topic
	ack, ok := client.CurrentAck("topic-1")
	require.True(t, ok)
	require.Equal(t, int64(0), ack) // we have not ack'ed the message, so the current ack is 0.

	// Test point 4: CurrentAck works for an unknown topic
	_, ok = client.CurrentAck("topic-2" /* unknown topic */)
	require.False(t, ok)

	// Test point 5: CurrentAck should advance as expected
	grpcStream.replyCh <- &p2p.SendMessageResponse{
		ExitReason: p2p.ExitReason_OK,
		Ack: []*p2p.Ack{{
			Topic:   "topic-1",
			LastSeq: 1,
		}},
	}
	require.Eventually(t, func() bool {
		ack, ok := client.CurrentAck("topic-1")
		require.True(t, ok)
		return ack == 1
	}, time.Second*1, time.Millisecond*10)

	// Test point 6: Send another message (blocking)
	sender.On("Append", &p2p.MessageEntry{
		Topic:    "topic-1",
		Content:  []byte(`{"value":2}`),
		Sequence: 2,
	}).Return(nil)
	seq, err = client.SendMessage(ctx, "topic-1", &testMessage{Value: 2})
	require.NoError(t, err)
	require.Equal(t, int64(2), seq)
	require.Eventuallyf(t, func() bool {
		return atomic.LoadInt32(&sender.sendCnt) == 2
	}, time.Second*1, time.Millisecond*10, "message should have been received")
	sender.AssertExpectations(t)

	// Test point 7: Interrupt the connection
	grpcStream.ExpectedCalls = nil
	grpcStream.Calls = nil

	sender.ExpectedCalls = nil
	sender.Calls = nil

	// Test point 8: We expect the message to be resent
	sender.On("Append", &p2p.MessageEntry{
		Topic:    "topic-1",
		Content:  []byte(`{"value":2}`),
		Sequence: 2,
	}).Return(nil)
	// We should flush the sender after resending the messages.
	sender.On("Flush").Return(nil)

	grpcStream.On("Recv").Return(nil, errors.New("fake error")).Once()
	grpcStream.On("Recv").Return(nil, nil)
	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		packet := args.Get(0).(*p2p.MessagePacket)
		require.EqualValues(t, &p2p.StreamMeta{
			SenderId:             "node-1",
			ReceiverId:           "node-2",
			Epoch:                2, // the epoch should be increased
			ClientVersion:        "v5.4.0",
			SenderAdvertisedAddr: "fake-addr:8300",
		}, packet.Meta)
	}).Once()

	// Resets the sentCnt
	atomic.StoreInt32(&sender.sendCnt, 0)
	grpcStream.replyCh <- &p2p.SendMessageResponse{
		ExitReason: p2p.ExitReason_OK,
		Ack: []*p2p.Ack{{
			Topic:   "topic-1",
			LastSeq: 1,
		}},
	}
	// We expect the message to be resent
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&sender.sendCnt) == 1
	}, time.Second*1, time.Millisecond*10, "message should have been resent")
	sender.AssertExpectations(t)

	cancel()
	wg.Wait()
}

func TestClientPermanentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	configCloned := *clientConfigForUnitTesting
	configCloned.BatchSendInterval = time.Hour // disables flushing

	client := NewMessageClient("node-1", &configCloned)
	sender := &mockClientBatchSender{}
	client.newSenderFn = func(stream clientStream) clientBatchSender {
		return sender
	}
	connector := &mockClientConnector{}
	grpcClient := &mockCDCPeerToPeerClient{}
	client.connector = connector
	connector.On("Connect", mock.Anything).Return(grpcClient, func() {}, nil)

	grpcStream := newMockSendMessageClient(ctx)
	grpcClient.On("SendMessage", mock.Anything, []grpc.CallOption(nil)).Return(
		grpcStream,
		nil,
	)
	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		packet := args.Get(0).(*p2p.MessagePacket)
		require.EqualValues(t, &p2p.StreamMeta{
			SenderId:             "node-1",
			ReceiverId:           "node-2",
			Epoch:                1, // 1 is the initial epoch
			ClientVersion:        "v5.4.0",
			SenderAdvertisedAddr: "fake-addr:8300",
		}, packet.Meta)
	})

	grpcStream.On("Recv").Return(&p2p.SendMessageResponse{
		ExitReason:   p2p.ExitReason_CAPTURE_ID_MISMATCH,
		ErrorMessage: "test message",
	}, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx, "", "", "node-2", &security.Credential{})
		require.Error(t, err)
		require.Regexp(t, ".*ErrPeerMessageClientPermanentFail.*", err.Error())
	}()

	wg.Wait()

	connector.AssertExpectations(t)
	grpcStream.AssertExpectations(t)
	sender.AssertExpectations(t)
}

func TestClientSendAnomalies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	client := NewMessageClient("node-1", clientConfigForUnitTesting)
	sender := &mockClientBatchSender{}

	runCtx, closeClient := context.WithCancel(ctx)
	defer closeClient()

	client.newSenderFn = func(stream clientStream) clientBatchSender {
		<-runCtx.Done()
		return sender
	}
	connector := &mockClientConnector{}
	grpcClient := &mockCDCPeerToPeerClient{}
	client.connector = connector
	connector.On("Connect", mock.Anything).Return(grpcClient, func() {}, nil)

	grpcStream := newMockSendMessageClient(ctx)
	grpcClient.On("SendMessage", mock.Anything, []grpc.CallOption(nil)).Return(
		grpcStream,
		nil,
	)

	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		packet := args.Get(0).(*p2p.MessagePacket)
		require.EqualValues(t, &p2p.StreamMeta{
			SenderId:             "node-1",
			ReceiverId:           "node-2",
			Epoch:                1, // 1 is the initial epoch
			ClientVersion:        "v5.4.0",
			SenderAdvertisedAddr: "fake-addr:8300",
		}, packet.Meta)
		closeClient()
	})

	grpcStream.On("Recv").Return(nil, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(runCtx, "", "", "node-2", &security.Credential{})
		require.Error(t, err)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	// Test point 1: ErrPeerMessageSendTryAgain
	_, err := client.TrySendMessage(ctx, "test-topic", &testMessage{Value: 1})
	require.Error(t, err)
	require.Regexp(t, ".*ErrPeerMessageSendTryAgain.*", err.Error())

	// Test point 2: close the client while SendMessage is blocking.
	_, err = client.SendMessage(ctx, "test-topic", &testMessage{Value: 1})
	require.Error(t, err)
	require.Regexp(t, ".*ErrPeerMessageClientClosed.*", err.Error())

	closeClient()
	wg.Wait()

	// Test point 3: call SendMessage after the client is closed.
	_, err = client.SendMessage(ctx, "test-topic", &testMessage{Value: 1})
	require.Error(t, err)
	require.Regexp(t, ".*ErrPeerMessageClientClosed.*", err.Error())
}
