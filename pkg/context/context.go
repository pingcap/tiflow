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

package context

import (
	"context"

	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/pkg/config"
	pd "github.com/tikv/pd/client"
)

// Vars contains some vars which can be used anywhere in a pipeline
// All field in Vars should be READ-ONLY and THREAD-SAFE
type Vars struct {
	// TODO add more vars
	CaptureAddr   string
	PDClient      pd.Client
	SchemaStorage entry.SchemaStorage
	Config        *config.ReplicaConfig
}

// Context contains Vars(), Done(), Throw(error) and StdContext() context.Context
// Context is used to instead of standard context
type Context interface {

	// Vars return the `Vars` store by the root context created by `NewContext`
	// Note that the `Vars` should be READ-ONLY and THREAD-SAFE
	// The root node and all its children node share one pointer of `Vars`
	// So any modification of `Vars` will cause all other family nodes to change.
	Vars() *Vars

	// Done return a channel which will be closed in the following cases:
	// - the `cancel()` returned from `WithCancel` is called.
	// - the `stdCtx` specified in `NewContext` is done.
	Done() <-chan struct{}

	// Throw an error to parents nodes
	// we can using `WatchThrow` to listen the errors thrown by children nodes
	Throw(error)

	// StdContext return a simple struct implement the stdcontext.Context interface
	// The Context in this package and the StdContext returned by this function have the same life cycle
	// It means the `StdContext.Done()` will done when the `Context` is done.
	StdContext() context.Context
}

type rootContext struct {
	Context
	vars *Vars
}

// NewContext returns a new pipeline context
func NewContext(stdCtx context.Context, vars *Vars) Context {
	ctx := &rootContext{
		vars: vars,
	}
	return withStdCancel(ctx, stdCtx)
}

func (ctx *rootContext) Vars() *Vars {
	return ctx.vars
}

func (ctx *rootContext) Throw(error) { /* do nothing */ }

type stdContext struct {
	stdCtx context.Context
	Context
}

func (ctx *stdContext) Done() <-chan struct{} {
	return ctx.stdCtx.Done()
}

func (ctx *stdContext) StdContext() context.Context {
	return ctx.stdCtx
}

//revive:disable:context-as-argument
func withStdCancel(ctx Context, stdCtx context.Context) Context {
	return &stdContext{
		stdCtx:  stdCtx,
		Context: ctx,
	}
}

// WithCancel return a Context with the cancel function
func WithCancel(ctx Context) (Context, context.CancelFunc) {
	stdCtx, cancel := context.WithCancel(ctx.StdContext())
	return withStdCancel(ctx, stdCtx), cancel
}

type throwContext struct {
	Context
	f func(error)
}

// WithErrorHandler creates a new context that can watch the Throw function
func WithErrorHandler(ctx Context, f func(error)) Context {
	return &throwContext{
		Context: ctx,
		f:       f,
	}
}

func (ctx *throwContext) Throw(err error) {
	if err == nil {
		return
	}
	ctx.f(err)
	ctx.Context.Throw(err)
}
