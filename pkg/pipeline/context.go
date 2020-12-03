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

	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/pkg/config"
	pd "github.com/tikv/pd/client"
)

// Context contains some read-only parameters and provide control for the pipeline life cycle
type Context interface {
	context.Context
	Vars() *ContextVars

	// Message returns the message sent by the previous node
	Message() *Message
	// SendToNextNode sends the message to the next node
	SendToNextNode(msg *Message)

	Throw(error)
}

// ContextVars contains some vars which can be used to anywhere in a pipeline
type ContextVars struct {
	PDClient      pd.Client
	SchemaStorage *entry.SchemaStorage
	Config        *config.ReplicaConfig
}

type baseContext struct {
	context.Context
	parent Context
}

func (ctx baseContext) Vars() *ContextVars {
	return ctx.parent.Vars()
}

func (ctx baseContext) Message() *Message {
	return ctx.parent.Message()
}

func (ctx baseContext) SendToNextNode(msg *Message) {
	ctx.parent.SendToNextNode(msg)
}

func (ctx baseContext) Throw(err error) {
	ctx.parent.Throw(err)
}

func (ctx baseContext) Deadline() (deadline time.Time, ok bool) {
	if ctx.Context == nil {
		return ctx.parent.Deadline()
	}
	return ctx.Context.Deadline()
}

func (ctx baseContext) Done() <-chan struct{} {
	if ctx.Context == nil {
		return ctx.parent.Done()
	}
	return ctx.Context.Done()
}

func (ctx baseContext) Err() error {
	if ctx.Context == nil {
		return ctx.parent.Err()
	}
	return ctx.Context.Err()
}

func (ctx baseContext) Value(key interface{}) interface{} {
	if ctx.Context == nil {
		return ctx.parent.Value(key)
	}
	return ctx.Context.Value(key)
}

type rootContext struct {
	baseContext
	vars         *ContextVars
	errorHandler func(error)
}

// NewRootContext returns a new root context
func NewRootContext(ctx context.Context, vars *ContextVars, errorHandler func(error)) (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	pCtx := &rootContext{
		baseContext: baseContext{
			Context: ctx,
		},
		vars: vars,
		errorHandler: func(err error) {
			cancel()
			errorHandler(err)
		},
	}
	return pCtx, cancel
}

func (ctx *rootContext) Message() *Message {
	panic("unreachable")
}

func (ctx *rootContext) SendToNextNode(msg *Message) {
	panic("unreachable")
}

func (ctx *rootContext) Throw(err error) {
	ctx.errorHandler(err)
}

type messageContext struct {
	baseContext
	message *Message
}

func withMessage(ctx Context, msg *Message) Context {
	return messageContext{
		baseContext: baseContext{
			parent: ctx,
		},
		message: msg,
	}
}

func (ctx messageContext) Message() *Message {
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
