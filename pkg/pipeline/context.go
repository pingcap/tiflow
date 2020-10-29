// Copyright 2020 PingCAP, Inc.
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

package pipeline

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
)

// Context contains some read-only parameters and provide control for the pipeline life cycle
type Context interface {
	// ReplicaConfig returns the replica config
	ReplicaConfig() *config.ReplicaConfig
	// Message returns the message sent by the previous node
	Message() *Message
	// SendToNextNode sends the message to the next node
	SendToNextNode(msg *Message)

	// StdContext returns a std context
	StdContext() context.Context
	// Cancel cancels the context
	Cancel()
	// Done returns a channel like context.Context.Done
	Done() <-chan struct{}
}

type baseContext struct {
	parent Context
}

func (ctx *baseContext) ReplicaConfig() *config.ReplicaConfig {
	return ctx.parent.ReplicaConfig()
}

func (ctx *baseContext) Message() *Message {
	return ctx.parent.Message()
}

func (ctx *baseContext) SendToNextNode(msg *Message) {
	ctx.parent.SendToNextNode(msg)
}

func (ctx *baseContext) Done() <-chan struct{} {
	return ctx.parent.Done()
}

func (ctx *baseContext) StdContext() context.Context {
	return ctx.parent.StdContext()
}

func (ctx *baseContext) Cancel() {
	ctx.parent.Cancel()
}

type rootContext struct {
	baseContext
	config  *config.ReplicaConfig
	closeCh chan struct{}
}

// NewRootContext returns a new root context
func NewRootContext(config *config.ReplicaConfig) Context {
	return &rootContext{
		baseContext: baseContext{},
		config:      config,
		closeCh:     make(chan struct{}),
	}
}

func (ctx *rootContext) Message() *Message {
	panic("unreachable")
}

func (ctx *rootContext) SendToNextNode(msg *Message) {
	panic("unreachable")
}

func (ctx *rootContext) ReplicaConfig() *config.ReplicaConfig {
	return ctx.config
}
func (ctx *rootContext) Done() <-chan struct{} {
	return ctx.closeCh
}

func (ctx *rootContext) StdContext() context.Context {
	return cancelStdContext{
		closeCh: ctx.closeCh,
	}
}

func (ctx *rootContext) Cancel() {
	defer func() {
		// Avoid panic because repeated close channel
		recover() //nolint:errcheck
	}()
	close(ctx.closeCh)
}

type messageContext struct {
	baseContext
	message *Message
}

func withMessage(ctx Context, msg *Message) Context {
	return &messageContext{
		baseContext: baseContext{
			parent: ctx,
		},
		message: msg,
	}
}

func (ctx *messageContext) Message() *Message {
	return ctx.message
}

type outputChContext struct {
	baseContext
	outputCh chan *Message
}

func (ctx *outputChContext) SendToNextNode(msg *Message) {
	// The header channel should never be blocked
	ctx.outputCh <- msg
}

func withOutputCh(ctx Context, outputCh chan *Message) Context {
	return &outputChContext{
		baseContext: baseContext{
			parent: ctx,
		},
		outputCh: outputCh,
	}
}

type cancelStdContext struct {
	closeCh chan struct{}
}

func (ctx cancelStdContext) Deadline() (deadline time.Time, ok bool) {
	return time.Now(), false
}

func (ctx cancelStdContext) Done() <-chan struct{} {
	return ctx.closeCh
}

func (ctx cancelStdContext) Err() error {
	return nil
}

func (ctx cancelStdContext) Value(key interface{}) interface{} {
	return nil
}
