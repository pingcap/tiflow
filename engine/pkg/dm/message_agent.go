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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/zap"
)

var (
	defaultMessageTimeOut  = time.Second * 2
	defaultRequestTimeOut  = time.Second * 30
	defaultResponseTimeOut = time.Second * 2
	defaultHandlerTimeOut  = time.Second * 30
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

	if err := sender.SendMessage(ctx, topic, msg, true /* block */); err != nil {
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
	return sender.SendMessage(ctx, topic, msg, false /* nonblock */)
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

// MessageAgent defines interface for message communication.
type MessageAgent interface {
	Init(ctx context.Context) error
	Tick(ctx context.Context) error
	Close(ctx context.Context) error
	UpdateSender(senderID string, sender Sender) error
	SendMessage(ctx context.Context, senderID string, command string, msg interface{}) error
	SendRequest(ctx context.Context, senderID string, command string, req interface{}) (interface{}, error)
}

// MessageAgentImpl implements the message processing mechanism.
type MessageAgentImpl struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	messagePair           *messagePair
	messageHandlerManager p2p.MessageHandlerManager
	pool                  workerpool.AsyncPool
	messageRouter         *lib.MessageRouter
	wg                    sync.WaitGroup
	// sender-id -> Sender
	senders        sync.Map
	commandHandler interface{}
	id             string
}

// NewMessageAgentImpl creates a new MessageAgent instance.
func NewMessageAgentImpl(id string, commandHandler interface{}, messageHandlerManager p2p.MessageHandlerManager) *MessageAgentImpl {
	agent := &MessageAgentImpl{
		messagePair:           newMessagePair(),
		commandHandler:        commandHandler,
		messageHandlerManager: messageHandlerManager,
		pool:                  workerpool.NewDefaultAsyncPool(10),
		id:                    id,
	}
	agent.messageRouter = lib.NewMessageRouter(agent.id, agent.pool, 100,
		func(topic p2p.Topic, msg p2p.MessageValue) error {
			err := agent.onMessage(topic, msg)
			if err != nil {
				// Todo: handle error
				log.L().Error("failed to handle message", log.ShortError(err))
			}
			return err
		},
	)
	return agent
}

// Init inits message agent.
func (agent *MessageAgentImpl) Init(ctx context.Context) error {
	agent.ctx, agent.cancel = context.WithCancel(context.Background())
	agent.wg.Add(1)
	go func() {
		defer agent.wg.Done()
		err := agent.pool.Run(agent.ctx)
		log.L().Info("workerpool exited", zap.Error(err))
	}()
	return nil
}

// Tick implements MessageAgent.Tick
func (agent *MessageAgentImpl) Tick(ctx context.Context) error {
	return agent.messageRouter.Tick(ctx)
}

// Close closes message agent.
func (agent *MessageAgentImpl) Close(ctx context.Context) error {
	if agent.cancel != nil {
		agent.cancel()
	}
	agent.wg.Wait()
	return nil
}

// UpdateSender adds or deletes the sender by sender-id and register/unreigster topic.
func (agent *MessageAgentImpl) UpdateSender(senderID string, sender Sender) error {
	if sender == nil {
		if _, loaded := agent.senders.LoadAndDelete(senderID); loaded {
			return agent.unregisterTopic(agent.ctx, senderID)
		}
	} else if _, loaded := agent.senders.LoadOrStore(senderID, sender); !loaded {
		return agent.registerTopic(agent.ctx, senderID)
	}
	return nil
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
	log.L().Debug("send message", zap.String("sender-id", senderID), zap.String("command", command), zap.Any("msg", msg))
	return sender.SendMessage(ctx2, generateTopic(agent.id, senderID), message{ID: 0, Type: messageTp, Command: command, Payload: msg}, false /* nonblock */)
}

// SendRequest send request synchronously.
func (agent *MessageAgentImpl) SendRequest(ctx context.Context, senderID string, command string, req interface{}) (interface{}, error) {
	sender, err := agent.getSender(senderID)
	if err != nil {
		return nil, err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultRequestTimeOut)
	defer cancel()
	log.L().Debug("send request", zap.String("sender-id", senderID), zap.String("command", command), zap.Any("req", req))
	return agent.messagePair.sendRequest(ctx2, generateTopic(agent.id, senderID), command, req, sender)
}

// sendResponse send response asynchronously.
func (agent *MessageAgentImpl) sendResponse(ctx context.Context, senderID string, msgID messageID, command string, resp interface{}) error {
	sender, err := agent.getSender(senderID)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultResponseTimeOut)
	defer cancel()
	log.L().Debug("send response", zap.String("sender-id", senderID), zap.String("command", command), zap.Any("resp", resp))
	return agent.messagePair.sendResponse(ctx2, generateTopic(agent.id, senderID), msgID, command, resp, sender)
}

// onMessage receive message/request/response.
// Forward the response to the corresponding request request.
// According to the command, the corresponding message processing function of defaultHandler will be called.
// According to the command, the corresponding request processing function of defaultHandler will be called, and send the response to caller.
// NOTE: comand name should same as handler name.
// MessageFuncType: func(ctx context.Context, msg *interface{}) error {}
// RequestFuncType(1): func(ctx context.Context, req *interface{}) (resp *interface{}, err error) {}
// RequestFuncType(2): func(ctx context.Context, req *interface{}) (resp *interface{}) {}
func (agent *MessageAgentImpl) onMessage(topic string, msg interface{}) error {
	log.L().Debug("on message", zap.String("topic", topic), zap.Any("msg", msg))
	m, ok := msg.(*message)
	if !ok {
		return errors.Errorf("unknown message type of topic %s", topic)
	}

	switch m.Type {
	case responseTp:
		return agent.handleResponse(m.ID, m.Command, m.Payload)
	case requestTp:
		sendID, _ := extractTopic(topic)
		return agent.handleRequest(sendID, m.ID, m.Command, m.Payload)
	default:
		return agent.handleMessage(m.Command, m.Payload)
	}
}

// handleResponse receive response.
func (agent *MessageAgentImpl) handleResponse(id messageID, command string, resp interface{}) error {
	handler := reflect.ValueOf(agent.commandHandler).MethodByName(command)
	if !handler.IsValid() {
		return errors.Errorf("response handler for command %s not found", command)
	}
	handlerType := handler.Type()
	if handlerType.NumOut() != 1 && handlerType.NumOut() != 2 {
		return errors.Errorf("wrong response handler type for command %s", command)
	}
	ret := reflect.New(handlerType.Out(0).Elem())
	if bytes, err := json.Marshal(resp); err != nil {
		return err
	} else if err := json.Unmarshal(bytes, ret.Interface()); err != nil {
		return err
	}
	return agent.messagePair.onResponse(id, ret.Interface())
}

// handleRequest receive request, call request handler and send response.
func (agent *MessageAgentImpl) handleRequest(senderID string, msgID messageID, command string, req interface{}) error {
	handler := reflect.ValueOf(agent.commandHandler).MethodByName(command)
	if !handler.IsValid() {
		return errors.Errorf("request handler for command %s not found", command)
	}
	handlerType := handler.Type()
	if handlerType.NumIn() != 2 || (handlerType.NumOut() != 1 && handlerType.NumOut() != 2) {
		return errors.Errorf("wrong request handler type for command %s", command)
	}
	arg := reflect.New(handlerType.In(1).Elem())
	if bytes, err := json.Marshal(req); err != nil {
		return err
	} else if err := json.Unmarshal(bytes, arg.Interface()); err != nil {
		return err
	}

	// call request handler
	ctx, cancel := context.WithTimeout(agent.ctx, defaultHandlerTimeOut)
	defer cancel()
	params := []reflect.Value{reflect.ValueOf(ctx), arg}
	rets := handler.Call(params)
	if err := rets[1].Interface(); err != nil {
		return err.(error)
	}
	// send response
	ctx2, cancel2 := context.WithTimeout(agent.ctx, defaultResponseTimeOut)
	defer cancel2()
	return agent.sendResponse(ctx2, senderID, msgID, command, rets[0].Interface())
}

// handle message receive message and call message handler.
func (agent *MessageAgentImpl) handleMessage(command string, msg interface{}) error {
	handler := reflect.ValueOf(agent.commandHandler).MethodByName(command)
	if !handler.IsValid() {
		return errors.Errorf("message handler for command %s not found", command)
	}
	handlerType := handler.Type()
	if handlerType.NumIn() != 2 || handlerType.NumOut() != 1 {
		return errors.Errorf("wrong message handler type for command %s", command)
	}
	arg := reflect.New(handlerType.In(1).Elem())
	if bytes, err := json.Marshal(msg); err != nil {
		return err
	} else if err := json.Unmarshal(bytes, arg.Interface()); err != nil {
		return err
	}

	// call message handler
	ctx, cancel := context.WithTimeout(agent.ctx, defaultHandlerTimeOut)
	defer cancel()
	params := []reflect.Value{reflect.ValueOf(ctx), arg}
	err := handler.Call(params)[0].Interface()
	if err == nil {
		return nil
	}
	return err.(error)
}

// registerTopic register p2p topic.
func (agent *MessageAgentImpl) registerTopic(ctx context.Context, senderID string) error {
	topic := generateTopic(senderID, agent.id)
	log.L().Debug("register topic", zap.String("topic", topic))
	_, err := agent.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&message{},
		func(sender p2p.NodeID, msg p2p.MessageValue) error {
			agent.messageRouter.AppendMessage(topic, msg)
			return nil
		},
	)
	return err
}

// unregisterTopic unregister p2p topic.
func (agent *MessageAgentImpl) unregisterTopic(ctx context.Context, senderID string) error {
	log.L().Debug("unregister topic", zap.String("topic", generateTopic(senderID, agent.id)))
	_, err := agent.messageHandlerManager.UnregisterHandler(ctx, generateTopic(senderID, agent.id))
	return err
}
