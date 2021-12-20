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
	"time"

	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// GlobalVars contains some vars which can be used anywhere in a pipeline
// the lifecycle of vars in the GlobalVars shoule be aligned with the ticdc server process.
// All field in Vars should be READ-ONLY and THREAD-SAFE
type GlobalVars struct {
	PDClient     pd.Client
	KVStorage    tidbkv.Storage
	CaptureInfo  *model.CaptureInfo
	EtcdClient   *kv.CDCEtcdClient
	GrpcPool     kv.GrpcPool
	TimeAcquirer pdtime.TimeAcquirer
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
	context.Context

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

	// Throw an error to parents nodes
	// we can using `WatchThrow` to listen the errors thrown by children nodes
	Throw(error)
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
	return WithStd(ctx, stdCtx)
}

func (ctx *rootContext) GlobalVars() *GlobalVars {
	return ctx.globalVars
}

func (ctx *rootContext) ChangefeedVars() *ChangefeedVars {
	return nil
}

func (ctx *rootContext) Throw(err error) {
	if err == nil {
		return
	}
	// make sure all error has been catched
	log.Panic("an error has escaped, please report a bug", zap.Error(err))
}

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

func (ctx *stdContext) Deadline() (deadline time.Time, ok bool) {
	return ctx.stdCtx.Deadline()
}

func (ctx *stdContext) Err() error {
	return ctx.stdCtx.Err()
}

func (ctx *stdContext) Value(key interface{}) interface{} {
	return ctx.stdCtx.Value(key)
}

func (ctx *stdContext) Done() <-chan struct{} {
	return ctx.stdCtx.Done()
}

// WithStd returns a Context with the standard Context
func WithStd(ctx Context, stdCtx context.Context) Context { //revive:disable:context-as-argument
	return &stdContext{
		stdCtx:  stdCtx,
		Context: ctx,
	}
}

// WithCancel returns a Context with the cancel function
func WithCancel(ctx Context) (Context, context.CancelFunc) {
	stdCtx, cancel := context.WithCancel(ctx)
	return WithStd(ctx, stdCtx), cancel
}

type throwContext struct {
	Context
	f func(error) error
}

// WithErrorHandler creates a new context that can watch the Throw function
// if the function `f` specified in WithErrorHandler returns an error,
// the error will be thrown to the parent context.
func WithErrorHandler(ctx Context, f func(error) error) Context {
	return &throwContext{
		Context: ctx,
		f:       f,
	}
}

func (ctx *throwContext) Throw(err error) {
	if err == nil {
		return
	}
	if err := ctx.f(err); err != nil {
		ctx.Context.Throw(err)
	}
}

// NewBackendContext4Test returns a new pipeline context for test
func NewBackendContext4Test(withChangefeedVars bool) Context {
	ctx := NewContext(context.Background(), &GlobalVars{
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-test",
			AdvertiseAddr: "127.0.0.1:0000",
			Version:       version.ReleaseVersion,
		},
		TimeAcquirer: pdtime.NewTimeAcquirer4Test(),
	})
	if withChangefeedVars {
		ctx = WithChangefeedVars(ctx, &ChangefeedVars{
			ID: "changefeed-id-test",
			Info: &model.ChangeFeedInfo{
				StartTs: oracle.GoTimeToTS(time.Now()),
				Config:  config.GetDefaultReplicaConfig(),
			},
		})
	}
	return ctx
}

// ZapFieldCapture returns a zap field containing capture address
func ZapFieldCapture(ctx Context) zap.Field {
	return zap.String("capture", ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
}

// ZapFieldChangefeed returns a zap field containing changefeed id
func ZapFieldChangefeed(ctx Context) zap.Field {
	return zap.String("changefeed", ctx.ChangefeedVars().ID)
}
