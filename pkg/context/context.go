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

// Vars contains some vars which can be used to anywhere in a pipeline
type Vars struct {
	// TODO add more vars
	PDClient      pd.Client
	SchemaStorage *entry.SchemaStorage
	Config        *config.ReplicaConfig
}

// Context contains Vars(), Done(), Throw(error) and StdContext() context.Context
// Context is used to instead of standard context
type Context interface {
	Vars() *Vars

	Done() <-chan struct{}
	Throw(error)
	StdContext() context.Context
}

type rootContext struct {
	stdCtx context.Context
	vars   *Vars
}

// NewContext returns a new pipeline context
func NewContext(stdCtx context.Context, vars *Vars) (Context, context.CancelFunc) {
	stdCtx, cancel := context.WithCancel(stdCtx)
	ctx := &rootContext{
		stdCtx: stdCtx,
		vars:   vars,
	}
	return ctx, cancel
}

func (ctx *rootContext) Vars() *Vars {
	return ctx.vars
}

func (ctx *rootContext) Done() <-chan struct{} {
	return ctx.stdCtx.Done()
}

func (ctx *rootContext) Throw(error) { /* do nothing */ }

func (ctx *rootContext) StdContext() context.Context {
	return ctx.stdCtx
}

type throwContext struct {
	Context
	f func(error)
}

// WatchThrow creates a new context that can watch the Throw function
func WatchThrow(ctx Context, f func(error)) Context {
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
