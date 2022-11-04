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

package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultHandlerOperationTimeout = 3 * time.Second
)

// MessageHandlerManager is for managing message topic handlers.
// NOTE: for each topic, only one handler is allowed.
type MessageHandlerManager interface {
	RegisterHandler(ctx context.Context, topic Topic, tpi TypeInformation, fn HandlerFunc) (bool, error)
	UnregisterHandler(ctx context.Context, topic Topic) (bool, error)
	CheckError(ctx context.Context) error

	// Clean unregisters all existing handlers.
	Clean(ctx context.Context) error

	// SetTimeout sets the timeout for handler operations.
	// A timeout is needed because the underlying handler operations are
	// asynchronous in the MessageServer.
	SetTimeout(timeout time.Duration)
}

func newMessageHandlerManager(registrar handlerRegistrar) MessageHandlerManager {
	return &messageHandlerManagerImpl{
		messageServer: registrar,
		timeout:       atomic.NewDuration(defaultHandlerOperationTimeout),
		topics:        make(map[Topic]<-chan error),
	}
}

// handlerRegistrar is an interface for the handler management-related
// functionalities of MessageServer.
// This interface is for easier unit-testing.
type handlerRegistrar interface {
	SyncAddHandler(context.Context, Topic, TypeInformation, HandlerFunc) (<-chan error, error)
	SyncRemoveHandler(context.Context, Topic) error
}

type messageHandlerManagerImpl struct {
	messageServer handlerRegistrar
	// timeout is atomic to avoid unnecessary blocking
	timeout *atomic.Duration

	// mu protects topics
	mu     sync.Mutex
	topics map[Topic]<-chan error
}

func (m *messageHandlerManagerImpl) RegisterHandler(
	ctx context.Context,
	topic Topic,
	tpi TypeInformation,
	fn HandlerFunc,
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topics[topic]; ok {
		// A handler for this topic already exists.
		return false, nil
	}

	ctx, cancel := m.makeContext(ctx)
	defer cancel()

	errCh, err := m.messageServer.SyncAddHandler(ctx, topic, tpi, fn)
	if err != nil {
		return false, errors.Trace(err)
	}
	m.topics[topic] = errCh

	return true, nil
}

func (m *messageHandlerManagerImpl) UnregisterHandler(ctx context.Context, topic Topic) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topics[topic]; !ok {
		// The handler for this topic does not exist
		return false, nil
	}

	ctx, cancel := m.makeContext(ctx)
	defer cancel()

	if err := m.messageServer.SyncRemoveHandler(ctx, topic); err != nil {
		return false, errors.Trace(err)
	}
	delete(m.topics, topic)

	return true, nil
}

func (m *messageHandlerManagerImpl) CheckError(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, errCh := range m.topics {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			if err == nil {
				continue
			}
			log.Warn("handler error received",
				zap.String("topic", topic))
			return errors.Trace(err)
		default:
		}
	}
	return nil
}

func (m *messageHandlerManagerImpl) Clean(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic := range m.topics {
		if err := m.messageServer.SyncRemoveHandler(ctx, topic); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (m *messageHandlerManagerImpl) SetTimeout(timeout time.Duration) {
	m.timeout.Store(timeout)
}

func (m *messageHandlerManagerImpl) makeContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := m.timeout.Load()
	return context.WithTimeout(parent, timeout)
}
