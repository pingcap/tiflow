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

package test

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/metadata"
)

// ExecutorChangeType defines executor change type, used in test only
type ExecutorChangeType int

// Defines all ExecutorChangeType
const (
	Delete ExecutorChangeType = iota
)

// ExecutorChangeEvent contains executor change type and time
type ExecutorChangeEvent struct {
	Tp   ExecutorChangeType
	Time time.Time
}

// Context is used in test only, containing some essential data structure
type Context struct {
	executorChangeCh chan *ExecutorChangeEvent
	dataCh           chan interface{}
	metaKV           metadata.MetaKV
}

// NewContext creates a new Context instance
func NewContext() *Context {
	return &Context{
		executorChangeCh: make(chan *ExecutorChangeEvent, 1024),
		dataCh:           make(chan interface{}, 128),
	}
}

// SetMetaKV sets meta kv
func (c *Context) SetMetaKV(kv metadata.MetaKV) {
	c.metaKV = kv
}

// GetMetaKV returns meta kv
func (c *Context) GetMetaKV() metadata.MetaKV {
	if c.metaKV == nil {
		c.metaKV = metadata.NewMetaMock()
	}
	return c.metaKV
}

// ExecutorChange returns the notify channel of executor change
func (c *Context) ExecutorChange() <-chan *ExecutorChangeEvent {
	return c.executorChangeCh
}

// NotifyExecutorChange adds notification to executor change channel
func (c *Context) NotifyExecutorChange(event *ExecutorChangeEvent) {
	c.executorChangeCh <- event
}

// SendRecord adds data to data channel
func (c *Context) SendRecord(data interface{}) {
	c.dataCh <- data
}

// RecvRecord receives data from data channel in a blocking way
func (c *Context) RecvRecord(ctx context.Context) interface{} {
	select {
	case data := <-c.dataCh:
		return data
	case <-ctx.Done():
		return nil
	}
}

// TryRecvRecord tries to receive one record from data channel
func (c *Context) TryRecvRecord() interface{} {
	select {
	case data := <-c.dataCh:
		return data
	default:
		return nil
	}
}
