// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/deps"
	extKV "github.com/hanfei1991/microcosm/pkg/meta/extension"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

// Context is used to in dm to record some context field like
// * go context
// * logger.
type Context struct {
	context.Context
	Logger       log.Logger
	Dependencies RuntimeDependencies // Deprecated
	Environ      Environment

	deps *deps.Deps
}

// Background return a nop context.
func Background() *Context {
	return &Context{
		Context: context.Background(),
		Logger:  log.L(),
	}
}

// NewContext return a new Context.
func NewContext(ctx context.Context, logger log.Logger) *Context {
	return &Context{
		Context: ctx,
		Logger:  logger,
	}
}

// WithContext set go context.
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Context: ctx,
		Logger:  c.Logger,
	}
}

// WithTimeout sets a timeout associated context.
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c, timeout)
	return &Context{
		Context: ctx,
		Logger:  c.Logger,
	}, cancel
}

// WithLogger set logger.
func (c *Context) WithLogger(logger log.Logger) *Context {
	return &Context{
		Context: c.Context,
		Logger:  logger,
	}
}

// WithDeps puts a built dependency container into the context.
func (c *Context) WithDeps(deps *deps.Deps) *Context {
	return &Context{
		Context:      c.Context,
		Logger:       c.Logger,
		Dependencies: c.Dependencies,
		Environ:      c.Environ,
		deps:         deps,
	}
}

// Deps returns a handle used for dependency injection.
func (c *Context) Deps() *deps.Deps {
	return c.deps
}

// L returns real logger.
func (c *Context) L() log.Logger {
	return c.Logger
}

type RuntimeDependencies struct {
	MessageHandlerManager p2p.MessageHandlerManager
	MessageRouter         p2p.MessageSender
	MetaKVClient          metaclient.KVClient
	UserRawKVClient       extKV.KVClientEx
	ExecutorClientManager *client.Manager
	ServerMasterClient    client.MasterClient
}

type Environment struct {
	NodeID          p2p.NodeID
	Addr            string
	MasterMetaBytes []byte
}
