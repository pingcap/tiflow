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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/security"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// GlobalVars contains some vars which can be used anywhere in a pipeline
// the lifecycle of vars in the GlobalVars shoule be aligned with the ticdc server process.
// All field in Vars should be READ-ONLY and THREAD-SAFE
type GlobalVars struct {
	PDClient    pd.Client
	Credential  *security.Credential
	KVStorage   tidbkv.Storage
	CaptureInfo *model.CaptureInfo
}

// ChangefeedVars contains some vars which can be used anywhere in a pipeline
// the lifecycle of vars in the ChangefeedVars shoule be aligned with the changefeed.
// All field in Vars should be READ-ONLY and THREAD-SAFE
type ChangefeedVars struct {
	ID   model.ChangeFeedID
	Info *model.ChangeFeedInfo
}

// Context contains Vars(), Done(), Throw(error) and StdContext() context.Context
// Context is used to instead of standard context
type Context interface {

	// GlobalVars return the `GlobalVars` store by the root context created by `NewContext`
	// Note that the `GlobalVars` should be READ-ONLY and THREAD-SAFE
	// The root node and all its children node share one pointer of `GlobalVars`
	// So any modification of `GlobalVars` will cause all other family nodes to change.
	GlobalVars() *GlobalVars

	// ChangefeedVars return the `ChangefeedVars` store by the context created by `WithChangefeedVars`
	// Note that the `ChangefeedVars` should be READ-ONLY and THREAD-SAFE
	// The root node and all its children node share one pointer of `ChangefeedVars`
	// So any modification of `ChangefeedVars` will cause all other family nodes to change.
	// ChangefeedVars could be return nil when the `ChangefeedVars` is not set by `WithChangefeedVars`
	ChangefeedVars() *ChangefeedVars

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
	globalVars *GlobalVars
}

// NewContext returns a new pipeline context
func NewContext(stdCtx context.Context, globalVars *GlobalVars) Context {
	ctx := &rootContext{
		globalVars: globalVars,
	}
	return withStdCancel(ctx, stdCtx)
}

func (ctx *rootContext) GlobalVars() *GlobalVars {
	return ctx.globalVars
}

func (ctx *rootContext) ChangefeedVars() *ChangefeedVars {
	return nil
}

func (ctx *rootContext) Throw(error) { /* do nothing */ }

// WithChangefeedVars return a Context with the `ChangefeedVars`
func WithChangefeedVars(ctx Context, changefeedVars *ChangefeedVars) Context {
	return &changefeedVarsContext{
		Context:        ctx,
		changefeedVars: changefeedVars,
	}
}

type changefeedVarsContext struct {
	Context
	changefeedVars *ChangefeedVars
}

func (ctx *changefeedVarsContext) ChangefeedVars() *ChangefeedVars {
	return ctx.changefeedVars
}

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

// NewBackendContext4Test returns a new pipeline context for test
func NewBackendContext4Test(withChangefeedVars bool) Context {
	ctx := NewContext(context.Background(), &GlobalVars{
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-4-test",
			AdvertiseAddr: "127.0.0.1:0000",
		},
	})
	if withChangefeedVars {
		ctx = WithChangefeedVars(ctx, &ChangefeedVars{
			ID: "changefeed-id-4-test",
		})
	}
	return ctx
}

// ZapFieldCapture returns a zap field containing capture address
// TODO: log redact for capture address
func ZapFieldCapture(ctx Context) zap.Field {
	return zap.String("capture", ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
}

// ZapFieldChangefeed returns a zap field containing changefeed id
func ZapFieldChangefeed(ctx Context) zap.Field {
	return zap.String("changefeed", ctx.ChangefeedVars().ID)
}

type gauge interface {
	WithLabelValues(lvs ...string) prometheus.Gauge
}

type counter interface {
	WithLabelValues(lvs ...string) prometheus.Counter
}

// WithLabelValuesGauge return a Gauge with captureAddr and changefeedID labels
func WithLabelValuesGauge(ctx Context, gauge gauge) prometheus.Gauge {
	if ctx.ChangefeedVars() != nil {
		return gauge.WithLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	}
	return gauge.WithLabelValues(ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
}

// WithLabelValuesCounter return a Counter with captureAddr and changefeedID labels
func WithLabelValuesCounter(ctx Context, counter counter) prometheus.Counter {
	if ctx.ChangefeedVars() != nil {
		return counter.WithLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	}
	return counter.WithLabelValues(ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
}
