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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	"github.com/pingcap/tiflow/cdc/syncpointstore"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

// DDLSink is a wrapper of the `Sink` interface for the owner
// DDLSink should send `DDLEvent` and `CheckpointTs` to downstream,
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
	// close the ddlsink, cancel running goroutine.
	close(ctx context.Context) error
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

	sink ddlsink.Sink
	// `sinkInitHandler` can be helpful in unit testing.
	sinkInitHandler ddlSinkInitHandler

	// cancel would be used to cancel the goroutine start by `run`
	cancel context.CancelFunc
	wg     sync.WaitGroup

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

		reportErr: reportErr,
	}
	return res
}

type ddlSinkInitHandler func(ctx context.Context, a *ddlSinkImpl) error

func ddlSinkInitializer(ctx context.Context, a *ddlSinkImpl) error {
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleOwner)
	log.Info("Try to create ddlSink based on sink",
		zap.String("namespace", a.changefeedID.Namespace),
		zap.String("changefeed", a.changefeedID.ID))
	s, err := factory.New(ctx, a.info.SinkURI, a.info.Config)
	if err != nil {
		return errors.Trace(err)
	}
	a.sink = s

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

func (s *ddlSinkImpl) makeSinkReady(ctx context.Context) error {
	if s.sink == nil {
		if err := s.sinkInitHandler(ctx, s); err != nil {
			log.Warn("ddl sink initialize failed",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Error(err))
			return errors.New("ddlSink not ready")
		}
	}
	return nil
}

// retry the given action with 5s interval. Before every retry, s.sink will be re-initialized.
func (s *ddlSinkImpl) retrySinkActionWithErrorReport(ctx context.Context, action func() error) (err error) {
	for {
		if err = action(); err == nil {
			return nil
		}
		s.sink = nil
		s.reportErr(model.NewWarning(err, model.ComponentOwnerSink))

		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *ddlSinkImpl) writeCheckpointTs(ctx context.Context, lastCheckpointTs *model.Ts) error {
	doWrite := func() (err error) {
		s.mu.Lock()
		checkpointTs := s.mu.checkpointTs
		if checkpointTs == 0 || checkpointTs <= *lastCheckpointTs {
			s.mu.Unlock()
			return
		}
		tables := make([]*model.TableInfo, 0, len(s.mu.currentTables))
		tables = append(tables, s.mu.currentTables...)
		s.mu.Unlock()

		if err = s.makeSinkReady(ctx); err == nil {
			err = s.sink.WriteCheckpointTs(ctx, checkpointTs, tables)
		}
		if err == nil {
			*lastCheckpointTs = checkpointTs
		}
		return
	}

	return s.retrySinkActionWithErrorReport(ctx, doWrite)
}

func (s *ddlSinkImpl) writeDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("begin emit ddl event",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID),
		zap.Any("DDL", ddl))

	doWrite := func() (err error) {
		if err = s.makeSinkReady(ctx); err == nil {
			err = s.sink.WriteDDLEvent(ctx, ddl)
			failpoint.Inject("InjectChangefeedDDLError", func() {
				err = cerror.ErrExecDDLFailed.GenWithStackByArgs()
			})
		}
		if err != nil {
			log.Error("Execute DDL failed",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Any("DDL", ddl),
				zap.Error(err))
		} else {
			ddl.Done = true
			log.Info("Execute DDL succeeded",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Any("DDL", ddl))
		}
		return
	}
	return s.retrySinkActionWithErrorReport(ctx, doWrite)
}

func (s *ddlSinkImpl) run(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// TODO make the tick duration configurable
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastCheckpointTs model.Ts
		var err error
		for {
			// `ticker.C` and `ddlCh` may can be triggered at the same time, it
			// does not matter which one emit first, since TiCDC allow DDL with
			// CommitTs equal to the last CheckpointTs be emitted later.
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err = s.writeCheckpointTs(ctx, &lastCheckpointTs); err != nil {
					return
				}
			case ddl := <-s.ddlCh:
				if err = s.writeDDLEvent(ctx, ddl); err != nil {
					return
				}
				// Force emitting checkpoint ts when a ddl event is finished.
				// Otherwise, a kafka consumer may not execute that ddl event.
				if err = s.writeCheckpointTs(ctx, &lastCheckpointTs); err != nil {
					return
				}
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

	query, err := s.addSpecialComment(ddl)
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
	s.wg.Wait()

	// they will both be nil if changefeed return an error in initializing
	if s.sink != nil {
		s.sink.Close()
	}
	if s.syncPointStore != nil {
		err = s.syncPointStore.Close()
	}
	if err != nil && errors.Cause(err) != context.Canceled {
		return err
	}
	return nil
}

// addSpecialComment translate tidb feature to comment
func (s *ddlSinkImpl) addSpecialComment(ddl *model.DDLEvent) (string, error) {
	stms, _, err := parser.New().Parse(ddl.Query, ddl.Charset, ddl.Collate)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(stms) != 1 {
		log.Panic("invalid ddlQuery statement size",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.String("ddlQuery", ddl.Query))
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
	// force disable ttl
	restoreFlags |= format.RestoreWithTTLEnableOff
	if err = stms[0].Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}

	result := sb.String()
	log.Info("add special comment to DDL",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID),
		zap.String("DDL", ddl.Query),
		zap.String("charset", ddl.Charset),
		zap.String("collate", ddl.Collate),
		zap.String("result", result))

	return result, nil
}
