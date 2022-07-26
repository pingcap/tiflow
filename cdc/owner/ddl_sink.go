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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

// DDLSink is a wrapper of the `Sink` interface for the owner
// DDLSink should send `DDLEvent` and `CheckpointTs` to downstream sink,
// If `SyncPointEnabled`, also send `syncPoint` to downstream.
type DDLSink interface {
	// run the DDLSink
	run(ctx cdcContext.Context, id model.ChangeFeedID, info *model.ChangeFeedInfo)
	// emitCheckpointTs emits the checkpoint Ts to downstream data source
	// this function will return after recording the checkpointTs specified in memory immediately
	// and the recorded checkpointTs will be sent and updated to downstream data source every second
	emitCheckpointTs(ts uint64, tableNames []model.TableName)
	// emitDDLEvent emits DDL event and return true if the DDL is executed
	// the DDL event will be sent to another goroutine and execute to downstream
	// the caller of this function can call again and again until a true returned
	emitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error)
	emitSyncPoint(ctx cdcContext.Context, checkpointTs uint64) error
	// close the sink, cancel running goroutine.
	close(ctx context.Context) error
	isInitialized() bool
}

type ddlSinkImpl struct {
	lastSyncPoint  model.Ts
	syncPointStore mysql.SyncpointStore

	// It is used to record the checkpointTs and the names of the table at that time.
	mu struct {
		sync.Mutex
		checkpointTs      model.Ts
		currentTableNames []model.TableName
	}
	// ddlSentTsMap is used to check whether a ddl event in a ddl job has been
	// sent to `ddlCh` successfully.
	ddlSentTsMap map[*model.DDLEvent]model.Ts

	ddlCh chan *model.DDLEvent
	errCh chan error

	sink sink.Sink
	// `sinkInitHandler` can be helpful in unit testing.
	sinkInitHandler ddlSinkInitHandler

	// cancel would be used to cancel the goroutine start by `run`
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// we use `initialized` to indicate whether the sink has been initialized.
	// the caller before calling any method of ddl sink
	// should check `initialized` first
	initialized atomic.Value
}

func newDDLSink() DDLSink {
	res := &ddlSinkImpl{
		ddlSentTsMap:    make(map[*model.DDLEvent]uint64),
		ddlCh:           make(chan *model.DDLEvent, 1),
		errCh:           make(chan error, defaultErrChSize),
		sinkInitHandler: ddlSinkInitializer,
		cancel:          func() {},
	}
	res.initialized.Store(false)
	return res
}

type ddlSinkInitHandler func(ctx cdcContext.Context, a *ddlSinkImpl, id model.ChangeFeedID, info *model.ChangeFeedInfo) error

func ddlSinkInitializer(ctx cdcContext.Context, a *ddlSinkImpl, id model.ChangeFeedID, info *model.ChangeFeedInfo) error {
	filter, err := filter.NewFilter(info.Config)
	if err != nil {
		return errors.Trace(err)
	}

	stdCtx := contextutil.PutChangefeedIDInCtx(ctx, id)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleOwner)
	s, err := sink.New(stdCtx, id, info.SinkURI, filter, info.Config, info.Opts, a.errCh)
	if err != nil {
		return errors.Trace(err)
	}
	a.sink = s

	if !info.SyncPointEnabled {
		return nil
	}
	syncPointStore, err := mysql.NewSyncpointStore(stdCtx, id, info.SinkURI)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("DDLSinkInitializeSlowly", func() {
		time.Sleep(time.Second * 5)
	})
	a.syncPointStore = syncPointStore

	if err := a.syncPointStore.CreateSynctable(stdCtx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *ddlSinkImpl) run(ctx cdcContext.Context, id model.ChangeFeedID, info *model.ChangeFeedInfo) {
	ctx, cancel := cdcContext.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		start := time.Now()
		if err := s.sinkInitHandler(ctx, s, id, info); err != nil {
			log.Warn("ddl sink initialize failed",
				zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
				zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
				zap.Duration("duration", time.Since(start)))
			ctx.Throw(err)
			return
		}
		s.initialized.Store(true)
		log.Info("ddl sink initialized, start processing...",
			zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
			zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
			zap.Duration("duration", time.Since(start)))

		// TODO make the tick duration configurable
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastCheckpointTs model.Ts
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-s.errCh:
				ctx.Throw(err)
				return
			default:
			}
			// `ticker.C` and `ddlCh` may can be triggered at the same time, it
			// does not matter which one emit first, since TiCDC allow DDL with
			// CommitTs equal to the last CheckpointTs be emitted later.
			select {
			case <-ctx.Done():
				return
			case err := <-s.errCh:
				ctx.Throw(err)
				return
			case <-ticker.C:
				s.mu.Lock()
				checkpointTs := s.mu.checkpointTs
				if checkpointTs == 0 || checkpointTs <= lastCheckpointTs {
					s.mu.Unlock()
					continue
				}
				tables := s.mu.currentTableNames
				s.mu.Unlock()
				lastCheckpointTs = checkpointTs
				if err := s.sink.EmitCheckpointTs(ctx, checkpointTs, tables); err != nil {
					ctx.Throw(errors.Trace(err))
					return
				}
			case ddl := <-s.ddlCh:
				log.Info("begin emit ddl event",
					zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
					zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
					zap.Any("DDL", ddl))
				err := s.sink.EmitDDLEvent(ctx, ddl)
				failpoint.Inject("InjectChangefeedDDLError", func() {
					err = cerror.ErrExecDDLFailed.GenWithStackByArgs()
				})
				if err == nil || cerror.ErrDDLEventIgnored.Equal(errors.Cause(err)) {
					log.Info("Execute DDL succeeded",
						zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
						zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
						zap.Bool("ignored", err != nil),
						zap.Any("ddl", ddl))
					// Force emitting checkpoint ts when a ddl event is finished.
					// Otherwise, a kafka consumer may not execute that ddl event.
					s.mu.Lock()
					ddl.Done = true
					checkpointTs := s.mu.checkpointTs
					if checkpointTs == 0 || checkpointTs <= lastCheckpointTs {
						s.mu.Unlock()
						continue
					}
					tables := s.mu.currentTableNames
					s.mu.Unlock()
					lastCheckpointTs = checkpointTs
					if err := s.sink.EmitCheckpointTs(ctx, checkpointTs, tables); err != nil {
						ctx.Throw(errors.Trace(err))
						return
					}
					continue
				}
				// If DDL executing failed, and the error can not be ignored,
				// throw an error and pause the changefeed
				log.Error("Execute DDL failed",
					zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
					zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
					zap.Error(err),
					zap.Any("ddl", ddl))
				ctx.Throw(errors.Trace(err))
				return
			}
		}
	}()
}

func (s *ddlSinkImpl) emitCheckpointTs(ts uint64, tableNames []model.TableName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.checkpointTs = ts
	s.mu.currentTableNames = tableNames
}

// emitDDLEvent returns true if the ddl event is already executed.
// For a rename tables job, the events in that job have identical StartTs
// and CommitTs. So in emitDDLEvent, we get the DDL finished ts of an event
// from a map in order to check whether that event is finshed or not.
func (s *ddlSinkImpl) emitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	s.mu.Lock()
	if ddl.Done {
		// the DDL event is executed successfully, and done is true
		log.Info("ddl already executed",
			zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
			zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
			zap.Any("DDL", ddl))
		delete(s.ddlSentTsMap, ddl)
		s.mu.Unlock()
		return true, nil
	}
	s.mu.Unlock()

	ddlSentTs := s.ddlSentTsMap[ddl]
	if ddl.CommitTs <= ddlSentTs {
		log.Debug("ddl is not finished yet",
			zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
			zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
			zap.Uint64("ddlSentTs", ddlSentTs), zap.Any("DDL", ddl))
		// the DDL event is executing and not finished yet, return false
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case s.ddlCh <- ddl:
		s.ddlSentTsMap[ddl] = ddl.CommitTs
		log.Info("ddl is sent",
			zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
			zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
			zap.Uint64("ddlSentTs", ddlSentTs))
	default:
		log.Warn("ddl chan full, send it the next round",
			zap.String("namespace", ctx.ChangefeedVars().ID.Namespace),
			zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
			zap.Uint64("ddlSentTs", ddlSentTs),
			zap.Any("DDL", ddl))
		// if this hit, we think that ddlCh is full,
		// just return false and send the ddl in the next round.
	}
	return false, nil
}

func (s *ddlSinkImpl) emitSyncPoint(ctx cdcContext.Context, checkpointTs uint64) error {
	if checkpointTs == s.lastSyncPoint {
		return nil
	}
	s.lastSyncPoint = checkpointTs
	// TODO implement async sink syncPoint
	return s.syncPointStore.SinkSyncpoint(ctx, ctx.ChangefeedVars().ID, checkpointTs)
}

func (s *ddlSinkImpl) close(ctx context.Context) (err error) {
	s.cancel()
	if s.sink != nil {
		err = s.sink.Close(ctx)
	}
	if s.syncPointStore != nil {
		err = s.syncPointStore.Close()
	}
	s.wg.Wait()
	if err != nil && errors.Cause(err) != context.Canceled {
		return err
	}
	return nil
}

func (s *ddlSinkImpl) isInitialized() bool {
	return s.initialized.Load().(bool)
}
