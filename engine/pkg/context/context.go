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

	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

// Context is used to in dm to record some context field like
// * go context
type Context struct {
	context.Context
	Environ     Environment
	ProjectInfo tenant.ProjectInfo

	deps *deps.Deps
}

// Background return a nop context.
func Background() *Context {
	return &Context{
		Context: context.Background(),
	}
}

// NewContext return a new Context.
func NewContext(ctx context.Context) *Context {
	return &Context{
		Context: ctx,
	}
}

// WithContext set go context.
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Context: ctx,
	}
}

// WithTimeout sets a timeout associated context.
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c, timeout)
	return &Context{
		Context: ctx,
	}, cancel
}

// WithDeps puts a built dependency container into the context.
func (c *Context) WithDeps(deps *deps.Deps) *Context {
	return &Context{
		Context:     c.Context,
		Environ:     c.Environ,
		ProjectInfo: c.ProjectInfo,

		deps: deps,
	}
}

// Deps returns a handle used for dependency injection.
func (c *Context) Deps() *deps.Deps {
	return c.deps
}

// Environment contains some configuration related environ values
type Environment struct {
	NodeID          p2p.NodeID
	Addr            string
	MasterMetaBytes []byte
}
