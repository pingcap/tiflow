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
	"encoding/hex"
	"encoding/json"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	defaultMessageTimeOut  = time.Second * 10
	defaultRequestTimeOut  = time.Second * 30
	defaultResponseTimeOut = time.Second * 10
	defaultHandlerTimeOut  = time.Second * 30
)

// generateTopic generate dm message topic with hex encoding.
func generateTopic(senderID string, receiverID string) string {
	hexKeys := []string{"DM"}
	hexKeys = append(hexKeys, hex.EncodeToString([]byte(senderID)))
	hexKeys = append(hexKeys, hex.EncodeToString([]byte(receiverID)))
	ret := path.Join(hexKeys...)
	return ret
}

// extractTopic extract dm message topic with hex decoding.
// TODO: handle error.
func extractTopic(topic string) (string, string) {
	v := strings.Split(strings.TrimPrefix(topic, "DM"), "/")
	// nolint:errcheck
	senderID, _ := hex.DecodeString(v[1])
	// nolint:errcheck
	receiverID, _ := hex.DecodeString(v[2])
	return string(senderID), string(receiverID)
}

type messageID uint64

type messageType int

const (
	messageTp messageType = iota + 1
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

// Client defines an interface that supports send message
type Client interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error
}

// messageMatcher implement a simple synchronous request/response message matcher since the lib currently only support asynchronous message.
type messageMatcher struct {
	// messageID -> response channel
	// TODO: limit the MaxPendingMessageCount if needed.
	pendings sync.Map
	id       atomic.Uint64
}

// newMessageMatcher creates a new messageMatcher instance
func newMessageMatcher() *messageMatcher {
	return &messageMatcher{}
}

func (m *messageMatcher) allocID() messageID {
	return messageID(m.id.Add(1))
}

// sendRequest sends a request message and wait for response.
func (m *messageMatcher) sendRequest(ctx context.Context, topic p2p.Topic, command string, req interface{}, client Client) (interface{}, error) {
	msg := message{ID: m.allocID(), Type: requestTp, Command: command, Payload: req}
	respCh := make(chan interface{}, 1)
	m.pendings.Store(msg.ID, respCh)
	defer m.pendings.Delete(msg.ID)

	if err := client.SendMessage(ctx, topic, msg, false /* nonblock */); err != nil {
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
func (m *messageMatcher) sendResponse(ctx context.Context, topic p2p.Topic, id messageID, command string, resp interface{}, client Client) error {
	msg := message{ID: id, Type: responseTp, Command: command, Payload: resp}
	return client.SendMessage(ctx, topic, msg, false /* nonblock */)
}

// onResponse receives and pairs a response message.
func (m *messageMatcher) onResponse(id messageID, resp interface{}) error {
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
	// update the client to register the request/response topic when client online.
	// update the client to nil to ungister the request/response when the client offline.
	UpdateClient(clientID string, client Client) error
	SendMessage(ctx context.Context, clientID string, command string, msg interface{}) error
	SendRequest(ctx context.Context, clientID string, command string, req interface{}) (interface{}, error)
}

// MessageAgentImpl implements the message processing mechanism.
type MessageAgentImpl struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	messageMatcher        *messageMatcher
	messageHandlerManager p2p.MessageHandlerManager
	pool                  workerpool.AsyncPool
	messageRouter         *lib.MessageRouter
	wg                    sync.WaitGroup
	mu                    sync.RWMutex
	// client-id -> Client
	clients map[string]Client
	// when receive message/request/response,
	// the corresponding processing method of commandHandler will be called according to the command name.
	commandHandler interface{}
	id             string
}

// NewMessageAgentImpl creates a new MessageAgent instance.
// message agent will call the method of commandHandler by command name automatically.
// The type of method of commandHandler should follow one of below:
// MessageFuncType: func(ctx context.Context, msg *interface{}) error {}
// RequestFuncType(1): func(ctx context.Context, req *interface{}) (resp *interface{}, err error) {}
// RequestFuncType(2): func(ctx context.Context, req *interface{}) (resp *interface{}) {}
func NewMessageAgentImpl(id string, commandHandler interface{}, messageHandlerManager p2p.MessageHandlerManager) *MessageAgentImpl {
	agent := &MessageAgentImpl{
		messageMatcher:        newMessageMatcher(),
		clients:               make(map[string]Client),
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

// UpdateClient adds or deletes the client by client-id and register/unregister topic.
func (agent *MessageAgentImpl) UpdateClient(clientID string, client Client) error {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	_, ok := agent.clients[clientID]
	if client == nil && ok {
		// delete client
		if err := agent.unregisterTopic(agent.ctx, clientID); err != nil {
			return err
		}
		delete(agent.clients, clientID)
	} else if client != nil && !ok {
		// add client
		if err := agent.registerTopic(agent.ctx, clientID); err != nil {
			return err
		}
		agent.clients[clientID] = client
	}
	return nil
}

// getClient gets client by client.
func (agent *MessageAgentImpl) getClient(clientID string) (Client, error) {
	agent.mu.RLock()
	defer agent.mu.RUnlock()
	client, ok := agent.clients[clientID]
	if !ok {
		return nil, errors.Errorf("client %s not found", clientID)
	}
	return client, nil
}

// SendMessage send message asynchronously.
func (agent *MessageAgentImpl) SendMessage(ctx context.Context, clientID string, command string, msg interface{}) error {
	client, err := agent.getClient(clientID)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultMessageTimeOut)
	defer cancel()
	log.L().Debug("send message", zap.String("client-id", clientID), zap.String("command", command), zap.Any("msg", msg))
	return client.SendMessage(ctx2, generateTopic(agent.id, clientID), message{ID: 0, Type: messageTp, Command: command, Payload: msg}, false /* nonblock */)
}

// SendRequest send request synchronously.
// caller should add its own retry mechanism if needed.
// caller should persist the request itself if needed.
func (agent *MessageAgentImpl) SendRequest(ctx context.Context, clientID string, command string, req interface{}) (interface{}, error) {
	client, err := agent.getClient(clientID)
	if err != nil {
		return nil, err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultRequestTimeOut)
	defer cancel()
	log.L().Debug("send request", zap.String("client-id", clientID), zap.String("command", command), zap.Any("req", req))
	return agent.messageMatcher.sendRequest(ctx2, generateTopic(agent.id, clientID), command, req, client)
}

// sendResponse send response asynchronously.
func (agent *MessageAgentImpl) sendResponse(ctx context.Context, clientID string, msgID messageID, command string, resp interface{}) error {
	client, err := agent.getClient(clientID)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithTimeout(ctx, defaultResponseTimeOut)
	defer cancel()
	log.L().Debug("send response", zap.String("client-id", clientID), zap.String("command", command), zap.Any("resp", resp))
	return agent.messageMatcher.sendResponse(ctx2, generateTopic(agent.id, clientID), msgID, command, resp, client)
}

// onMessage receive message/request/response.
// Forward the response to the corresponding request.
// According to the command, the corresponding message processing function of commandHandler will be called.
// According to the command, the corresponding request processing function of commandHandler will be called, and send the response to caller.
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
		clientID, _ := extractTopic(topic)
		return agent.handleRequest(clientID, m.ID, m.Command, m.Payload)
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
	return agent.messageMatcher.onResponse(id, ret.Interface())
}

// handleRequest receive request, call request handler and send response.
func (agent *MessageAgentImpl) handleRequest(clientID string, msgID messageID, command string, req interface{}) error {
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
	if len(rets) == 2 && rets[1].Interface() != nil {
		return rets[1].Interface().(error)
	}
	// send response
	ctx2, cancel2 := context.WithTimeout(agent.ctx, defaultResponseTimeOut)
	defer cancel2()
	return agent.sendResponse(ctx2, clientID, msgID, command, rets[0].Interface())
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
func (agent *MessageAgentImpl) registerTopic(ctx context.Context, clientID string) error {
	topic := generateTopic(clientID, agent.id)
	log.L().Debug("register topic", zap.String("topic", topic))
	_, err := agent.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&message{},
		func(client p2p.NodeID, msg p2p.MessageValue) error {
			agent.messageRouter.AppendMessage(topic, msg)
			return nil
		},
	)
	return err
}

// unregisterTopic unregister p2p topic.
func (agent *MessageAgentImpl) unregisterTopic(ctx context.Context, clientID string) error {
	log.L().Debug("unregister topic", zap.String("topic", generateTopic(clientID, agent.id)))
	_, err := agent.messageHandlerManager.UnregisterHandler(ctx, generateTopic(clientID, agent.id))
	return err
}
