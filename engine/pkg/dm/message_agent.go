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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var (
	defaultMessageTimeOut  = time.Second * 2
	defaultRequestTimeOut  = time.Second * 30
	defaultResponseTimeOut = time.Second * 2
)

// generateTopic generate dm message topic.
func generateTopic(sendID string, receiverID string) string {
	return fmt.Sprintf("DM---%s---%s", sendID, receiverID)
}

// extractTopic extract dm message topic.
// TODO: handle special case.
func extractTopic(topic string) (string, string) {
	parts := strings.Split(topic, "---")
	return parts[1], parts[2]
}

type messageID uint64

type messageType int

const (
	messageTp messageType = iota
	requestTp
	responseTp
)

// message use for asynchronous message and synchronous request/response.
type message struct {
	ID      messageID
	Type    messageType
	Command string
	Payload interface{}
}

// messageIDAllocator is an id allocator for p2p message system
type messageIDAllocator struct {
	mu sync.Mutex
	id messageID
}

// alloc allocs a new message id
func (a *messageIDAllocator) alloc() messageID {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.id++
	return a.id
}

// Sender defines an interface that supports send message
type Sender interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error
}

// messagePair implement a simple synchronous request/response message pattern since the lib currently only support asynchronous message.
// Caller should persist the request message if needed.
// Caller should add retry mechanism if needed.
type messagePair struct {
	// messageID -> response channel
	// TODO: limit the MaxPendingMessageCount if needed.
	pendings    sync.Map
	idAllocator *messageIDAllocator
}

// newMessagePair creates a new MessagePair instance
func newMessagePair() *messagePair {
	return &messagePair{
		idAllocator: &messageIDAllocator{},
	}
}

// sendRequest sends a request message and wait for response.
func (m *messagePair) sendRequest(ctx context.Context, topic p2p.Topic, command string, req interface{}, sender Sender) (interface{}, error) {
	msg := message{ID: m.idAllocator.alloc(), Type: requestTp, Command: command, Payload: req}
	respCh := make(chan interface{}, 1)
	m.pendings.Store(msg.ID, respCh)
	defer m.pendings.Delete(msg.ID)

	if err := sender.SendMessage(ctx, topic, msg, false /* block */); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp, nil
	}
}

// sendResponse sends a response with message ID.
func (m *messagePair) sendResponse(ctx context.Context, topic p2p.Topic, id messageID, command string, resp interface{}, sender Sender) error {
	msg := message{ID: id, Type: responseTp, Command: command, Payload: resp}
	return sender.SendMessage(ctx, topic, msg, true /* nonblock */)
}

// onResponse receives and pairs a response message.
func (m *messagePair) onResponse(id messageID, resp interface{}) error {
	respCh, ok := m.pendings.Load(id)
	if !ok {
		return errors.Errorf("request %d not found", id)
	}

	select {
	case respCh.(chan interface{}) <- resp:
		return nil
	default:
	}
	return errors.Errorf("duplicated response of request %d, and the last response is not consumed", id)
}

// HandlerFunc defines handler func type.
type HandlerFunc func(interface{}) error

// MessageAgent defines interface for message communication.
type MessageAgent interface {
	RegisterCommandHandler(command string, handler HandlerFunc)
	UpdateSender(senderID string, sender Sender)
	SendMessage(ctx context.Context, senderID string, command string, msg interface{}) error
	SendRequest(ctx context.Context, senderID string, command string, req interface{}) (interface{}, error)
	SendResponse(ctx context.Context, senderID string, id messageID, command string, resp interface{}) error
	OnMessage(topic string, msg interface{}) error
	RegisterTopic(ctx context.Context, senderID string) error
	UnregisterTopic(ctx context.Context, senderID string) error
}

// MessageAgentImpl implements the message processing mechanism.
type MessageAgentImpl struct {
	ctx                   context.Context
	messagePair           *messagePair
	messageHandlerManager p2p.MessageHandlerManager
	// sender-id -> Sender
	senders sync.Map
	// topic -> handler
	handlers              sync.Map
	defaultCommandHandler interface{}
	id                    string
}

// NewMessageAgent creates a new MessageAgent instance.
func NewMessageAgent(ctx context.Context, id string, initSenders map[string]Sender,
	defaultCommandHandler interface{}, messageHandlerManager p2p.MessageHandlerManager,
) *MessageAgentImpl {
	messageAgent := &MessageAgentImpl{
		ctx:                   ctx,
		messagePair:           newMessagePair(),
		defaultCommandHandler: defaultCommandHandler,
		messageHandlerManager: messageHandlerManager,
		id:                    id,
	}
	for senderID, sender := range initSenders {
		messageAgent.UpdateSender(senderID, sender)
	}
	return messageAgent
}

// UpdateSender adds or deletes the sender by sender-id.
func (agent *MessageAgentImpl) UpdateSender(senderID string, sender Sender) {
	if sender == nil {
		agent.senders.Delete(senderID)
	} else {
		agent.senders.Store(senderID, sender)
	}
}

// getSender gets sender by senderID.
func (agent *MessageAgentImpl) getSender(senderID string) (Sender, error) {
	sender, ok := agent.senders.Load(senderID)
	if !ok {
		return nil, errors.Errorf("sender %s not found", senderID)
	}
	return sender.(Sender), nil
}

// SendMessage send message asynchronously.
func (agent *MessageAgentImpl) SendMessage(ctx context.Context, senderID string, command string, msg interface{}) error {
	sender, err := agent.getSender(senderID)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultMessageTimeOut)
	defer cancel()
	return sender.SendMessage(ctx2, generateTopic(agent.id, senderID), message{ID: 0, Type: messageTp, Command: command, Payload: msg}, true)
}

// SendRequest send request synchronously.
func (agent *MessageAgentImpl) SendRequest(ctx context.Context, senderID string, command string, req interface{}) (interface{}, error) {
	sender, err := agent.getSender(senderID)
	if err != nil {
		return nil, err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultRequestTimeOut)
	defer cancel()
	return agent.messagePair.sendRequest(ctx2, generateTopic(agent.id, senderID), command, req, sender)
}

// SendResponse send response asynchronously.
func (agent *MessageAgentImpl) SendResponse(ctx context.Context, senderID string, msgID messageID, command string, resp interface{}) error {
	sender, err := agent.getSender(senderID)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultResponseTimeOut)
	defer cancel()
	return agent.messagePair.sendResponse(ctx2, generateTopic(agent.id, senderID), msgID, command, resp, sender)
}

// RegisterCommandHandler register command handler.
func (agent *MessageAgentImpl) RegisterCommandHandler(command string, handler HandlerFunc) {
	agent.handlers.Store(command, handler)
}

// OnMessage receive message/request/response.
// Forward the response to the corresponding request request.
// According to the command, the corresponding message processing function is called.
// According to the command, the corresponding request processing function is called, and send the response to caller.
// NOTE: processing function name should same as topic name.
// MessageFuncType: func(ctx context.Context, msg interface{}) error {}
// RequestFuncType: func(ctx context.Context, req interface{}) (resp interface{}, err error) {}
func (agent *MessageAgentImpl) OnMessage(topic string, msg interface{}) error {
	m, ok := msg.(message)
	if !ok {
		return errors.Errorf("unknown message type of %v", msg)
	}

	// matches the registered handler firstly
	if val, ok := agent.handlers.Load(m.Command); ok {
		return val.(HandlerFunc)(m.Payload)
	}

	switch m.Type {
	case responseTp:
		return agent.handleResponse(m.ID, m.Payload)
	case requestTp:
		sendID, _ := extractTopic(topic)
		return agent.handleRequest(sendID, m.ID, m.Command, m.Payload)
	default:
		return agent.handleMessage(m.Command, m.Payload)
	}
}

// handleResponse receive response.
func (agent *MessageAgentImpl) handleResponse(id messageID, resp interface{}) error {
	return agent.messagePair.onResponse(id, resp)
}

// handleRequest receive request, call request handler and send response.
func (agent *MessageAgentImpl) handleRequest(senderID string, msgID messageID, command string, req interface{}) error {
	// TODO: check input/output num/type if needed, panic now
	handler := reflect.ValueOf(agent.defaultCommandHandler).MethodByName(command)
	if !handler.IsValid() {
		return errors.Errorf("request handler for command %s not found", command)
	}

	// call request handler
	ctx, cancel := context.WithTimeout(agent.ctx, defaultRequestTimeOut)
	defer cancel()
	params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
	rets := handler.Call(params)
	if err := rets[1].Interface(); err != nil {
		return err.(error)
	}
	// send response
	ctx2, cancel2 := context.WithTimeout(agent.ctx, defaultRequestTimeOut)
	defer cancel2()
	return agent.SendResponse(ctx2, senderID, msgID, command, rets[0].Interface())
}

// handle message receive message and call message handler.
func (agent *MessageAgentImpl) handleMessage(command string, msg interface{}) error {
	// TODO: check input/output num/type if needed, panic now
	handler := reflect.ValueOf(agent.defaultCommandHandler).MethodByName(command)
	if !handler.IsValid() {
		return errors.Errorf("message handler for command %s not found", command)
	}

	// call message handler
	ctx, cancel := context.WithTimeout(agent.ctx, defaultMessageTimeOut)
	defer cancel()
	params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg)}
	err := handler.Call(params)[0].Interface()
	if err == nil {
		return nil
	}
	return err.(error)
}

// RegisterTopic register p2p topic.
func (agent *MessageAgentImpl) RegisterTopic(ctx context.Context, senderID string) error {
	topic := generateTopic(senderID, agent.id)
	_, err := agent.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		message{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			return agent.OnMessage(topic, value)
		},
	)
	return err
}

// UnregisterTopic unregister p2p topic.
func (agent *MessageAgentImpl) UnregisterTopic(ctx context.Context, senderID string) error {
	_, err := agent.messageHandlerManager.UnregisterHandler(ctx, generateTopic(senderID, agent.id))
	return err
}
