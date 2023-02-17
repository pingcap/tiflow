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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/factory"
	"github.com/pingcap/tiflow/cdc/syncpointstore"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
	run(ctx context.Context)
	// emitCheckpointTs emits the checkpoint Ts to downstream data source
	// this function will return after recording the checkpointTs specified in memory immediately
	// and the recorded checkpointTs will be sent and updated to downstream data source every second
	emitCheckpointTs(ts uint64, tables []*model.TableInfo)
	// emitDDLEvent emits DDL event and return true if the DDL is executed
	// the DDL event will be sent to another goroutine and execute to downstream
	// the caller of this function can call again and again until a true returned
	emitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error)
	emitSyncPoint(ctx context.Context, checkpointTs uint64) error
	// close the sink, cancel running goroutine.
	close(ctx context.Context) error
	isInitialized() bool
}

type ddlSinkImpl struct {
	lastSyncPoint  model.Ts
	syncPointStore syncpointstore.SyncPointStore

	// It is used to record the checkpointTs and the names of the table at that time.
	mu struct {
		sync.Mutex
		checkpointTs  model.Ts
		currentTables []*model.TableInfo
	}
	// ddlSentTsMap is used to check whether a ddl event in a ddl job has been
	// sent to `ddlCh` successfully.
	ddlSentTsMap map[*model.DDLEvent]model.Ts

	ddlCh chan *model.DDLEvent
	errCh chan error

	sinkV2 sinkv2.DDLEventSink
	// `sinkInitHandler` can be helpful in unit testing.
	sinkInitHandler ddlSinkInitHandler

	// cancel would be used to cancel the goroutine start by `run`
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// we use `initialized` to indicate whether the sink has been initialized.
	// the caller before calling any method of ddl sink
	// should check `initialized` first
	initialized atomic.Value

	changefeedID model.ChangeFeedID
	info         *model.ChangeFeedInfo

	reportErr func(err error)
}

func newDDLSink(changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo, reportErr func(err error)) DDLSink {
	res := &ddlSinkImpl{
		ddlSentTsMap:    make(map[*model.DDLEvent]uint64),
		ddlCh:           make(chan *model.DDLEvent, 1),
		sinkInitHandler: ddlSinkInitializer,
		cancel:          func() {},

		changefeedID: changefeedID,
		info:         info,

		errCh:     make(chan error, defaultErrChSize),
		reportErr: reportErr,
	}
	res.initialized.Store(false)
	return res
}

type ddlSinkInitHandler func(ctx context.Context, a *ddlSinkImpl) error

func ddlSinkInitializer(ctx context.Context, a *ddlSinkImpl) error {
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleOwner)
	log.Info("Try to create ddlSink based on sinkV2",
		zap.String("namespace", a.changefeedID.Namespace),
		zap.String("changefeed", a.changefeedID.ID))
	s, err := factory.New(ctx, a.info.SinkURI, a.info.Config)
	if err != nil {
		return errors.Trace(err)
	}
	a.sinkV2 = s

	if !a.info.Config.EnableSyncPoint {
		return nil
	}
	syncPointStore, err := syncpointstore.NewSyncPointStore(
		ctx, a.changefeedID, a.info.SinkURI, a.info.Config.SyncPointRetention)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("DDLSinkInitializeSlowly", func() {
		time.Sleep(time.Second * 5)
	})
	a.syncPointStore = syncPointStore

	if err := a.syncPointStore.CreateSyncTable(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *ddlSinkImpl) run(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		start := time.Now()
		if err := s.sinkInitHandler(ctx, s); err != nil {
			log.Warn("ddl sink initialize failed",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
			s.reportErr(err)
			return
		}
		s.initialized.Store(true)
		log.Info("ddl sink initialized, start processing...",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
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
				s.reportErr(err)
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
				s.reportErr(err)
				return
			case <-ticker.C:
				s.mu.Lock()
				checkpointTs := s.mu.checkpointTs
				if checkpointTs == 0 || checkpointTs <= lastCheckpointTs {
					s.mu.Unlock()
					continue
				}
				tables := s.mu.currentTables
				s.mu.Unlock()
				lastCheckpointTs = checkpointTs

				if err := s.sinkV2.WriteCheckpointTs(ctx,
					checkpointTs, tables); err != nil {
					s.reportErr(err)
					return
				}

			case ddl := <-s.ddlCh:
				log.Info("begin emit ddl event",
					zap.String("namespace", s.changefeedID.Namespace),
					zap.String("changefeed", s.changefeedID.ID),
					zap.Any("DDL", ddl))

				err := s.sinkV2.WriteDDLEvent(ctx, ddl)
				failpoint.Inject("InjectChangefeedDDLError", func() {
					err = cerror.ErrExecDDLFailed.GenWithStackByArgs()
				})
				if err == nil {
					log.Info("Execute DDL succeeded",
						zap.String("namespace", s.changefeedID.Namespace),
						zap.String("changefeed", s.changefeedID.ID),
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
					tables := s.mu.currentTables
					s.mu.Unlock()
					lastCheckpointTs = checkpointTs
					if err := s.sinkV2.WriteCheckpointTs(ctx,
						checkpointTs, tables); err != nil {
						s.reportErr(err)
						return
					}
					continue
				}
				// If DDL executing failed, and the error can not be ignored,
				// throw an error and pause the changefeed
				log.Error("Execute DDL failed",
					zap.String("namespace", s.changefeedID.Namespace),
					zap.String("changefeed", s.changefeedID.ID),
					zap.Error(err),
					zap.Any("ddl", ddl))
				s.reportErr(err)
				return
			}
		}
	}()
}

func (s *ddlSinkImpl) emitCheckpointTs(ts uint64, tables []*model.TableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.checkpointTs = ts
	s.mu.currentTables = tables
}

// emitDDLEvent returns true if the ddl event is already executed.
// For the `rename tables` job, the events in that job have identical StartTs
// and CommitTs. So in emitDDLEvent, we get the DDL finished ts of an event
// from a map in order to check whether that event is finished or not.
func (s *ddlSinkImpl) emitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error) {
	s.mu.Lock()
	if ddl.Done {
		// the DDL event is executed successfully, and done is true
		log.Info("ddl already executed, skip it",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Any("DDL", ddl))
		delete(s.ddlSentTsMap, ddl)
		s.mu.Unlock()
		return true, nil
	}

	ddlSentTs := s.ddlSentTsMap[ddl]
	if ddl.CommitTs <= ddlSentTs {
		log.Debug("ddl is not finished yet",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Uint64("ddlSentTs", ddlSentTs), zap.Any("DDL", ddl))
		// the DDL event is executing and not finished yet, return false
		s.mu.Unlock()
		return false, nil
	}

	query, err := addSpecialComment(ddl.Query)
	if err != nil {
		log.Error("Add special comment failed",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Error(err),
			zap.Any("ddl", ddl))
		s.mu.Unlock()
		return false, errors.Trace(err)
	}
	ddl.Query = query
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case s.ddlCh <- ddl:
		s.ddlSentTsMap[ddl] = ddl.CommitTs
		log.Info("ddl is sent",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Uint64("ddlSentTs", ddl.CommitTs))
	default:
		log.Warn("ddl chan full, send it the next round",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Uint64("ddlSentTs", ddlSentTs),
			zap.Any("DDL", ddl))
		// if this hit, we think that ddlCh is full,
		// just return false and send the ddl in the next round.
	}
	return false, nil
}

func (s *ddlSinkImpl) emitSyncPoint(ctx context.Context, checkpointTs uint64) error {
	if checkpointTs == s.lastSyncPoint {
		return nil
	}
	s.lastSyncPoint = checkpointTs
	// TODO implement async sink syncPoint
	return s.syncPointStore.SinkSyncPoint(ctx, s.changefeedID, checkpointTs)
}

func (s *ddlSinkImpl) close(ctx context.Context) (err error) {
	s.cancel()
	// they will both be nil if changefeed return an error in initializing
	if s.sinkV2 != nil {
		s.sinkV2.Close()
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

// addSpecialComment translate tidb feature to comment
func addSpecialComment(ddlQuery string) (string, error) {
	stms, _, err := parser.New().ParseSQL(ddlQuery)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(stms) != 1 {
		log.Panic("invalid ddlQuery statement size", zap.String("ddlQuery", ddlQuery))
	}
	var sb strings.Builder
	// translate TiDB feature to special comment
	restoreFlags := format.RestoreTiDBSpecialComment
	// escape the keyword
	restoreFlags |= format.RestoreNameBackQuotes
	// upper case keyword
	restoreFlags |= format.RestoreKeyWordUppercase
	// wrap string with single quote
	restoreFlags |= format.RestoreStringSingleQuotes
	// remove placement rule
	restoreFlags |= format.SkipPlacementRuleForRestore
	if err = stms[0].Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}
