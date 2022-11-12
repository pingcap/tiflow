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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAllocID(t *testing.T) {
	t.Parallel()

	messageMatcher := newMessageMatcher()
	require.Equal(t, messageID(1), messageMatcher.allocID())
	require.Equal(t, messageID(2), messageMatcher.allocID())
	require.Equal(t, messageID(3), messageMatcher.allocID())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				messageMatcher.allocID()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, messageID(1004), messageMatcher.allocID())
}

func TestMessageMatcher(t *testing.T) {
	t.Parallel()

	messageMatcher := newMessageMatcher()
	mockClient := &MockClient{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientCtx := context.Background()

	messageErr := errors.New("message error")
	// synchronous send
	mockClient.On("SendMessage").Return(messageErr).Once()
	resp, err := messageMatcher.sendRequest(ctx, clientCtx, "topic", "command", "request", mockClient)
	require.EqualError(t, err, messageErr.Error())
	require.Nil(t, resp)
	// deadline exceeded
	mockClient.On("SendMessage").Return(nil).Once()
	ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	resp, err = messageMatcher.sendRequest(ctx2, clientCtx, "topic", "command", "request", mockClient)
	require.EqualError(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, resp)
	// late response
	require.EqualError(t, messageMatcher.onResponse(2, "response"), "request 2 not found")

	resp2 := "response"
	go func() {
		mockClient.On("SendMessage").Return(nil).Once()
		ctx3, cancel3 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel3()
		resp3, err := messageMatcher.sendRequest(ctx3, clientCtx, "request-topic", "command", "request", mockClient)
		require.NoError(t, err)
		require.Equal(t, "response", resp3)
	}()

	// send response
	time.Sleep(time.Second)
	mockClient.On("SendMessage").Return(nil).Once()
	require.NoError(t, messageMatcher.sendResponse(ctx, "response-topic", 3, "command", resp2, mockClient))
	require.NoError(t, messageMatcher.onResponse(3, resp2))

	// duplicate response
	require.Eventually(t, func() bool {
		err := messageMatcher.onResponse(3, resp2)
		return err != nil && err.Error() == fmt.Sprintf("request %d not found", 3)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestUpdateClient(t *testing.T) {
	messageAgent := NewMessageAgentImpl("", nil, p2p.NewMockMessageHandlerManager(), log.L()).(*MessageAgentImpl)
	workerHandle1 := &framework.MockHandle{WorkerID: "worker1"}
	workerHandle2 := &framework.MockHandle{WorkerID: "worker2"}

	// add client
	messageAgent.UpdateClient("task1", workerHandle1.Unwrap())
	require.Len(t, messageAgent.clients.clients, 1)
	client, err := messageAgent.getClient("task1")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle1.Unwrap())
	client, err = messageAgent.getClient("task2")
	require.EqualError(t, err, "client task2 not found")
	require.Equal(t, client, nil)
	messageAgent.UpdateClient("task2", workerHandle2.Unwrap())
	require.Len(t, messageAgent.clients.clients, 2)
	client, err = messageAgent.getClient("task1")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle1.Unwrap())
	client, err = messageAgent.getClient("task2")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle2.Unwrap())

	// remove client
	messageAgent.UpdateClient("task3", nil)
	require.Len(t, messageAgent.clients.clients, 2)
	client, err = messageAgent.getClient("task1")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle1.Unwrap())
	client, err = messageAgent.getClient("task2")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle2.Unwrap())
	messageAgent.RemoveClient("task2")
	require.Len(t, messageAgent.clients.clients, 1)
	client, err = messageAgent.getClient("task1")
	require.NoError(t, err)
	require.Equal(t, client, workerHandle1.Unwrap())
}

func TestMessageAgent(t *testing.T) {
	messageAgent := NewMessageAgentImpl("id", nil, p2p.NewMockMessageHandlerManager(), log.L()).(*MessageAgentImpl)
	clientID := "client-id"
	mockClient := &MockClient{}
	messageAgent.UpdateClient(clientID, mockClient)

	require.Error(t, messageAgent.SendMessage(context.Background(), "wrong-id", "command", "msg"), "client wrong-id not found")
	require.Error(t, messageAgent.sendResponse(context.Background(), "wrong-id", 1, "command", "resp"), "client wrong-id not found")
	ret, err := messageAgent.SendRequest(context.Background(), "wrong-id", "command", "request")
	require.EqualError(t, err, "client wrong-id not found")
	require.Nil(t, ret)

	mockClient.On("SendMessage").Return(nil).Once()
	require.NoError(t, messageAgent.SendMessage(context.Background(), clientID, "command", "msg"))

	resp := "response"
	go func() {
		mockClient.On("SendMessage").Return(nil).Once()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp2, err := messageAgent.SendRequest(ctx, clientID, "command", "request")
		require.NoError(t, err)
		require.Equal(t, resp, resp2)
	}()

	time.Sleep(time.Second)
	// send response
	mockClient.On("SendMessage").Return(nil).Once()
	require.NoError(t, messageAgent.sendResponse(context.Background(), clientID, 2, "command", "response"))
	messageAgent.onMessage(generateTopic("Client", "Receiver"), message{ID: 2, Type: responseTp, Payload: resp})
}

func TestMessageHandler(t *testing.T) {
	var (
		clientID           = "client-id"
		receiveID          = "receiver-id"
		topic              = generateTopic(clientID, receiveID)
		msg                = message{ID: 0, Type: messageTp, Command: messageAPI, Payload: &MessageAPIMessage{Msg: "msg"}}
		req                = message{ID: 1, Type: requestTp, Command: requestAPI, Payload: &RequestAPIRequest{Req: "req"}}
		resp               = message{ID: 1, Type: responseTp, Command: requestAPI, Payload: &RequestAPIResponse{Resp: "resp"}}
		wrongMsg           = message{ID: 0, Type: messageTp, Command: wrongAPI, Payload: &MessageAPIMessage{Msg: "msg"}}
		wrongReq           = message{ID: 0, Type: requestTp, Command: wrongAPI, Payload: &MessageAPIMessage{Msg: "msg"}}
		wrongResp          = message{ID: 0, Type: responseTp, Command: wrongAPI, Payload: &MessageAPIMessage{Msg: "msg"}}
		serializeMsg       = &message{}
		serializeReq       = &message{}
		serializeResp      = &message{}
		serializeWrongMsg  = &message{}
		serializeWrongReq  = &message{}
		serializeWrongResp = &message{}
	)
	// mock serializeMessage
	serialize(t, msg, serializeMsg)
	serialize(t, req, serializeReq)
	serialize(t, resp, serializeResp)
	serialize(t, wrongMsg, serializeWrongMsg)
	serialize(t, wrongReq, serializeWrongReq)
	serialize(t, wrongResp, serializeWrongResp)

	// mock no handler
	messageAgent := NewMessageAgentImpl("id", &MockNothing{}, p2p.NewMockMessageHandlerManager(), log.L()).(*MessageAgentImpl)
	require.EqualError(t, messageAgent.onMessage(topic, serializeMsg), "message handler for command MessageAPI not found")
	require.EqualError(t, messageAgent.onMessage(topic, serializeReq), "request handler for command RequestAPI not found")
	require.EqualError(t, messageAgent.onMessage(topic, serializeResp), "response handler for command RequestAPI not found")

	// mock has handler
	mockHandler := &MockHanlder{}
	messageAgent = NewMessageAgentImpl("id", mockHandler, p2p.NewMockMessageHandlerManager(), log.L()).(*MessageAgentImpl)
	mockClient := &MockClient{}
	messageAgent.UpdateClient(clientID, mockClient)
	mockClient.On("SendMessage").Return(nil).Once()
	// wrong handler type
	require.Error(t, messageAgent.onMessage(topic, serializeWrongMsg), "wrong message handler type for command WrongAPI")
	require.Error(t, messageAgent.onMessage(topic, serializeWrongReq), "wrong request handler type for command WrongAPI")
	require.Error(t, messageAgent.onMessage(topic, serializeWrongResp), "wrong response handler type for command WrongAPI")

	// handle message
	mockHandler.On(messageAPI).Return(nil).Once()
	require.NoError(t, messageAgent.onMessage(topic, serializeMsg))
	mockHandler.On(messageAPI).Return(errors.New("error")).Once()
	require.EqualError(t, messageAgent.onMessage(topic, serializeMsg), "error")
	// handle request
	mockHandler.On(requestAPI).Return(&RequestAPIResponse{}, nil).Once()
	require.NoError(t, messageAgent.onMessage(topic, serializeReq))
	mockHandler.On(requestAPI).Return(&RequestAPIResponse{}, errors.New("error")).Once()
	require.EqualError(t, messageAgent.onMessage(topic, serializeReq), "error")
	// handle response
	require.EqualError(t, messageAgent.onMessage(topic, serializeResp), "request 1 not found")
}

func TestMessageHandlerLifeCycle(t *testing.T) {
	messageAgent := NewMessageAgentImpl("id", nil, p2p.NewMockMessageHandlerManager(), log.L())
	messageAgent.Tick(context.Background())
	messageAgent.UpdateClient("client-id", &framework.MockWorkerHandler{})
	messageAgent.UpdateClient("client-id", nil)
	messageAgent.Close(context.Background())
}

func serialize(t *testing.T, m message, mPtr *message) {
	bytes, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(bytes, mPtr))
}

const (
	wrongAPI   p2p.Topic = "WrongAPI"
	messageAPI p2p.Topic = "MessageAPI"
	requestAPI p2p.Topic = "RequestAPI"
)

type (
	WrongAPIMessage struct {
		Msg string
	}
	MessageAPIMessage struct {
		Msg string
	}
	RequestAPIRequest struct {
		Req string
	}
	RequestAPIResponse struct {
		Resp string
	}
)

type MockNothing struct{}

type MockHanlder struct {
	sync.Mutex
	mock.Mock
}

func (m *MockHanlder) WrongAPI() {}

func (m *MockHanlder) MessageAPI(ctx context.Context, msg *MessageAPIMessage) error {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Error(0)
}

func (m *MockHanlder) RequestAPI(ctx context.Context, req *RequestAPIRequest) (*RequestAPIResponse, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(*RequestAPIResponse), args.Error(1)
}

type MockClient struct {
	sync.Mutex
	mock.Mock
}

func (s *MockClient) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	s.Lock()
	defer s.Unlock()
	args := s.Called()
	return args.Error(0)
}
