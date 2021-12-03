// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/filter"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

//type asyncSinkInitializer func(ctx cdcContext.Context) error

// AsyncSink is an async sink design for owner
// The EmitCheckpointTs and EmitDDLEvent is asynchronous function for now
// Other functions are still synchronization
type AsyncSink interface {
	Run(ctx cdcContext.Context) error
	// EmitCheckpointTs emits the checkpoint Ts to downstream data source
	// this function will return after recording the checkpointTs specified in memory immediately
	// and the recorded checkpointTs will be sent and updated to downstream data source every second
	EmitCheckpointTs(ctx cdcContext.Context, ts uint64)
	// EmitDDLEvent emits DDL event asynchronously and return true if the DDL is executed
	// the DDL event will be sent to another goroutine and execute to downstream
	// the caller of this function can call again and again until a true returned
	EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error)
	SinkSyncPoint(ctx cdcContext.Context, checkpointTs uint64) error
	Close(ctx context.Context) error
}

type asyncSinkImpl struct {
	sink           sink.Sink
	syncPointStore sink.SyncpointStore

	checkpointTs  model.Ts
	lastSyncPoint model.Ts

	ddlFinishedTs model.Ts
	ddlSentTs     model.Ts

	ddlCh           chan *model.DDLEvent
	errCh           chan error
	sinkInitHandler asyncSinkInitHandler
}

func newAsyncSinkImpl(_ cdcContext.Context) AsyncSink {
	return &asyncSinkImpl{
		ddlCh:           make(chan *model.DDLEvent, 1),
		errCh:           make(chan error, defaultErrChSize),
		sinkInitHandler: asyncSinkInitializer,
	}
}

type asyncSinkInitHandler func(ctx cdcContext.Context, a *asyncSinkImpl) error

func asyncSinkInitializer(ctx cdcContext.Context, a *asyncSinkImpl) error {
	var (
		id   = ctx.ChangefeedVars().ID
		info = ctx.ChangefeedVars().Info
	)

	eg, ctx1 := errgroup.WithContext(ctx)
	eg.Go(func() error {
		filter, err := filter.NewFilter(info.Config)
		if err != nil {
			return errors.Trace(err)
		}

		sink, err := sink.New(ctx1, id, info.SinkURI, filter, info.Config, info.Opts, a.errCh)
		if err != nil {
			return errors.Trace(err)
		}
		a.sink = sink
		return nil
	})

	eg.Go(func() error {
		if info.SyncPointEnabled {
			syncPointStore, err := sink.NewSyncpointStore(ctx1, id, info.SinkURI)
			if err != nil {
				return errors.Trace(err)
			}
			a.syncPointStore = syncPointStore

			if err := a.syncPointStore.CreateSynctable(ctx1); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})

	return eg.Wait()
}

func (s *asyncSinkImpl) Run(ctx cdcContext.Context) error {
	if err := s.sinkInitHandler(ctx, s); err != nil {
		return errors.Trace(err)
	}

	// TODO make the tick duration configurable
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastCheckpointTs model.Ts
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.errCh:
			return err
		case <-ticker.C:
			checkpointTs := atomic.LoadUint64(&s.checkpointTs)
			if checkpointTs == 0 || checkpointTs <= lastCheckpointTs {
				continue
			}
			lastCheckpointTs = checkpointTs
			if err := s.sink.EmitCheckpointTs(ctx, checkpointTs); err != nil {
				return errors.Trace(err)
			}
		case ddl := <-s.ddlCh:
			err := s.sink.EmitDDLEvent(ctx, ddl)
			failpoint.Inject("InjectChangefeedDDLError", func() {
				err = cerror.ErrExecDDLFailed.GenWithStackByArgs()
			})
			if err == nil || cerror.ErrDDLEventIgnored.Equal(errors.Cause(err)) {
				log.Info("Execute DDL succeeded", zap.String("changefeed", ctx.ChangefeedVars().ID), zap.Bool("ignored", err != nil), zap.Reflect("ddl", ddl))
				atomic.StoreUint64(&s.ddlFinishedTs, ddl.CommitTs)
				continue
			}
			// If DDL executing failed, and the error can not be ignored, throw an error and pause the changefeed
			log.Error("Execute DDL failed",
				zap.String("ChangeFeedID", ctx.ChangefeedVars().ID),
				zap.Error(err),
				zap.Reflect("ddl", ddl))
			return errors.Trace(err)
		}
	}
}

func (s *asyncSinkImpl) EmitCheckpointTs(ctx cdcContext.Context, ts uint64) {
	atomic.StoreUint64(&s.checkpointTs, ts)
}

func (s *asyncSinkImpl) EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	ddlFinishedTs := atomic.LoadUint64(&s.ddlFinishedTs)
	if ddl.CommitTs <= ddlFinishedTs {
		// the DDL event is executed successfully, and done is true
		return true, nil
	}
	if ddl.CommitTs <= s.ddlSentTs {
		// the DDL event is executing and not finished yes, return false
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case s.ddlCh <- ddl:
	}
	s.ddlSentTs = ddl.CommitTs
	return false, nil
}

func (s *asyncSinkImpl) SinkSyncPoint(ctx cdcContext.Context, checkpointTs uint64) error {
	if checkpointTs == s.lastSyncPoint {
		return nil
	}
	s.lastSyncPoint = checkpointTs
	// TODO implement async sink syncPoint
	return s.syncPointStore.SinkSyncpoint(ctx, ctx.ChangefeedVars().ID, checkpointTs)
}

func (s *asyncSinkImpl) Close(ctx context.Context) (err error) {
	err = s.sink.Close(ctx)
	if s.syncPointStore != nil {
		err = s.syncPointStore.Close()
	}
	return err
}
