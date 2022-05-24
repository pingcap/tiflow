// Copyright 2022 PingCAP, Inc.
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

package dm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/lib/master"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMessageIDAllocator(t *testing.T) {
	t.Parallel()

	allocator := &messageIDAllocator{}
	require.Equal(t, messageID(1), allocator.alloc())
	require.Equal(t, messageID(2), allocator.alloc())
	require.Equal(t, messageID(3), allocator.alloc())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				allocator.alloc()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, messageID(1004), allocator.alloc())
}

func TestMessagePair(t *testing.T) {
	t.Parallel()

	messagePair := newMessagePair()
	mockSender := &MockSender{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageErr := errors.New("message error")
	// synchronous send
	mockSender.On("SendMessage").Return(messageErr).Once()
	resp, err := messagePair.sendRequest(ctx, "topic", "command", "request", mockSender)
	require.EqualError(t, err, messageErr.Error())
	require.Nil(t, resp)
	// deadline exceeded
	mockSender.On("SendMessage").Return(nil).Once()
	ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	resp, err = messagePair.sendRequest(ctx2, "topic", "command", "request", mockSender)
	require.EqualError(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, resp)
	// late response
	require.EqualError(t, messagePair.onResponse(2, "response"), "request 2 not found")

	resp2 := "response"
	go func() {
		mockSender.On("SendMessage").Return(nil).Once()
		ctx3, cancel3 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel3()
		resp3, err := messagePair.sendRequest(ctx3, "request-topic", "command", "request", mockSender)
		require.NoError(t, err)
		require.Equal(t, "response", resp3)
	}()

	// send response
	time.Sleep(time.Second)
	mockSender.On("SendMessage").Return(nil).Once()
	require.NoError(t, messagePair.sendResponse(ctx, "response-topic", 3, "command", resp2, mockSender))
	require.NoError(t, messagePair.onResponse(3, resp2))

	// duplicate response
	require.Eventually(t, func() bool {
		err := messagePair.onResponse(3, resp2)
		return err != nil && err.Error() == fmt.Sprintf("request %d not found", 3)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestUpdateSender(t *testing.T) {
	messageAgent := NewMessageAgent(context.Background(), "", nil, nil)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 0)
	workerHandle1 := &master.MockHandle{WorkerID: "worker1"}
	workerHandle2 := &master.MockHandle{WorkerID: "worker2"}

	// add sender
	messageAgent.UpdateSender("task1", workerHandle1)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 1)
	sender, err := messageAgent.getSender("task1")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle1)
	sender, err = messageAgent.getSender("task2")
	require.EqualError(t, err, "sender task2 not found")
	require.Equal(t, sender, nil)
	messageAgent.UpdateSender("task2", workerHandle2)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 2)
	sender, err = messageAgent.getSender("task1")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle1)
	sender, err = messageAgent.getSender("task2")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle2)

	// remove sender
	messageAgent.UpdateSender("task3", nil)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 2)
	sender, err = messageAgent.getSender("task1")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle1)
	sender, err = messageAgent.getSender("task2")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle2)
	messageAgent.UpdateSender("task2", nil)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 1)
	sender, err = messageAgent.getSender("task1")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle1)

	// mock init with map
	initWorkerHandleMap := make(map[string]Sender)
	messageAgent.senders.Range(func(key, value interface{}) bool {
		initWorkerHandleMap[key.(string)] = value.(Sender)
		return true
	})
	messageAgent = NewMessageAgent(context.Background(), "", initWorkerHandleMap, nil)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 1)
	sender, err = messageAgent.getSender("task1")
	require.NoError(t, err)
	require.Equal(t, sender, workerHandle1)
}

func TestMessageAgent(t *testing.T) {
	messageAgent := NewMessageAgent(context.Background(), "id", nil, nil)
	require.Equal(t, lenSyncMap(&messageAgent.senders), 0)
	senderID := "sender-id"
	mockSender := &MockSender{}
	messageAgent.UpdateSender(senderID, mockSender)

	require.Error(t, messageAgent.SendMessage(context.Background(), "wrong-id", "command", "msg"), "sender wrong-id not found")
	require.Error(t, messageAgent.SendResponse(context.Background(), "wrong-id", 1, "command", "resp"), "sender wrong-id not found")
	ret, err := messageAgent.SendRequest(context.Background(), "wrong-id", "command", "request")
	require.EqualError(t, err, "sender wrong-id not found")
	require.Nil(t, ret)

	mockSender.On("SendMessage").Return(nil).Once()
	require.NoError(t, messageAgent.SendMessage(context.Background(), senderID, "command", "msg"))

	resp := "response"
	go func() {
		mockSender.On("SendMessage").Return(nil).Once()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp2, err := messageAgent.SendRequest(ctx, senderID, "command", "request")
		require.NoError(t, err)
		require.Equal(t, resp, resp2)
	}()

	time.Sleep(time.Second)
	// send response
	mockSender.On("SendMessage").Return(nil).Once()
	require.NoError(t, messageAgent.SendResponse(context.Background(), senderID, 2, "command", "response"))
	messageAgent.OnMessage("DM---Sender---Receiver", message{ID: 2, Type: responseTp, Payload: resp})
}

func TestMessageHandler(t *testing.T) {
	senderID := "sender-id"
	topic := "DM---" + senderID + "---receive-id"
	command := "command"

	// mock no handler
	messageAgent := NewMessageAgent(context.Background(), "id", nil, &MockNothing{})
	require.EqualError(t, messageAgent.OnMessage(topic, "msg"), "unknown message type of msg")
	// register handler
	messageAgent.RegisterHandler(command, func(interface{}) error { return nil })
	require.NoError(t, messageAgent.OnMessage(command, message{ID: 1, Type: requestTp, Command: command, Payload: "msg"}))

	// mock no handler
	msg := message{ID: 0, Type: messageTp, Command: messageAPI, Payload: MessageAPIMessage{}}
	require.EqualError(t, messageAgent.OnMessage(topic, msg), "message handler for command MessageAPI not found")
	req := message{ID: 1, Type: requestTp, Command: requestAPI, Payload: RequestAPIRequest{}}
	require.EqualError(t, messageAgent.OnMessage(topic, req), "request handler for command RequestAPI not found")

	// mock has handler
	mockHandler := &MockHanlder{}
	messageAgent = NewMessageAgent(context.Background(), "id", nil, mockHandler)
	mockSender := &MockSender{}
	messageAgent.UpdateSender(senderID, mockSender)
	mockSender.On("SendMessage").Return(nil).Once()
	// handle message
	mockHandler.On(messageAPI).Return(nil).Once()
	require.NoError(t, messageAgent.OnMessage(topic, msg))
	mockHandler.On(messageAPI).Return(errors.New("error")).Once()
	require.EqualError(t, messageAgent.OnMessage(topic, msg), "error")
	// handle request
	mockHandler.On(requestAPI).Return(RequestAPIResponse{}, nil).Once()
	require.NoError(t, messageAgent.OnMessage(topic, req))
	mockHandler.On(requestAPI).Return(RequestAPIResponse{}, errors.New("error")).Once()
	require.EqualError(t, messageAgent.OnMessage(topic, req), "error")
	// handle response
	res := message{ID: 1, Type: responseTp, Payload: RequestAPIResponse{}}
	require.EqualError(t, messageAgent.OnMessage(topic, res), "request 1 not found")
}

const (
	messageAPI p2p.Topic = "MessageAPI"
	requestAPI p2p.Topic = "RequestAPI"
)

type (
	MessageAPIMessage  struct{}
	RequestAPIRequest  struct{}
	RequestAPIResponse struct{}
)

type MockNothing struct{}

type MockHanlder struct {
	sync.Mutex
	mock.Mock
}

func (m *MockHanlder) MessageAPI(ctx context.Context, msg MessageAPIMessage) error {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Error(0)
}

func (m *MockHanlder) RequestAPI(ctx context.Context, req RequestAPIRequest) (RequestAPIResponse, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(RequestAPIResponse), args.Error(1)
}

func lenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

type MockSender struct {
	sync.Mutex
	mock.Mock
}

func (s *MockSender) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called()
	return args.Error(0)
}
