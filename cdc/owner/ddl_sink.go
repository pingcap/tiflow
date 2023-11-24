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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	timysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	sinkv1 "github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/factory"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
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
	syncPointStore mysql.SyncPointStore

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

	sinkV1 sinkv1.Sink
	errCh  chan error

	sinkV2 sinkv2.DDLEventSink

	// `sinkInitHandler` can be helpful in unit testing.
	sinkInitHandler ddlSinkInitHandler

	// cancel would be used to cancel the goroutine start by `run`
	cancel context.CancelFunc
	wg     sync.WaitGroup

	changefeedID model.ChangeFeedID
	info         *model.ChangeFeedInfo

	sinkRetry     *retry.ErrorRetry
	reportError   func(err error)
	reportWarning func(err error)
}

func newDDLSink(
	changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo,
	reportError func(err error), reportWarning func(err error),
) DDLSink {
	res := &ddlSinkImpl{
		ddlSentTsMap:    make(map[*model.DDLEvent]uint64),
		ddlCh:           make(chan *model.DDLEvent, 1),
		sinkInitHandler: ddlSinkInitializer,
		cancel:          func() {},

		changefeedID: changefeedID,
		info:         info,

		sinkRetry:     retry.NewInfiniteErrorRetry(),
		reportError:   reportError,
		reportWarning: reportWarning,
	}
	return res
}

type ddlSinkInitHandler func(ctx context.Context, a *ddlSinkImpl) error

func ddlSinkInitializer(ctx context.Context, a *ddlSinkImpl) error {
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleOwner)
	conf := config.GetGlobalServerConfig()
	if !conf.Debug.EnableNewSink {
		log.Info("Try to create ddlSink based on sinkV1",
			zap.String("namespace", a.changefeedID.Namespace),
			zap.String("changefeed", a.changefeedID.ID))
		s, err := sinkv1.New(ctx, a.changefeedID, a.info.SinkURI, a.info.Config, a.errCh)
		if err != nil {
			return errors.Trace(err)
		}
		a.sinkV1 = s
	} else {
		log.Info("Try to create ddlSink based on sinkV2",
			zap.String("namespace", a.changefeedID.Namespace),
			zap.String("changefeed", a.changefeedID.ID))
		s, err := factory.New(ctx, a.info.SinkURI, a.info.Config)
		if err != nil {
			return errors.Trace(err)
		}
		a.sinkV2 = s
	}

	return nil
}

func (s *ddlSinkImpl) makeSyncPointStoreReady(ctx context.Context) error {
	if s.info.Config.EnableSyncPoint && s.syncPointStore == nil {
		syncPointStore, err := mysql.NewSyncPointStore(
			ctx, s.changefeedID, s.info.SinkURI, s.info.Config.SyncPointRetention)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("DDLSinkInitializeSlowly", func() {
			time.Sleep(time.Second * 5)
		})
		s.syncPointStore = syncPointStore

		if err := s.syncPointStore.CreateSyncTable(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *ddlSinkImpl) makeSinkReady(ctx context.Context) error {
	if s.sinkV1 == nil && s.sinkV2 == nil {
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
func (s *ddlSinkImpl) retrySinkAction(ctx context.Context, name string, action func() error) (err error) {
	for {
		if err = action(); err == nil {
			return nil
		}
		s.sinkV1 = nil
		s.sinkV2 = nil
		isRetryable := !cerror.ShouldFailChangefeed(err) && errors.Cause(err) != context.Canceled
		log.Warn("owner ddl sink fails on action",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.String("action", name),
			zap.Bool("retryable", isRetryable),
			zap.Error(err))

		if isRetryable {
			s.reportWarning(err)
		} else {
			s.reportError(err)
			return err
		}

		backoff, err := s.sinkRetry.GetRetryBackoff(err)
		if err != nil {
			return errors.New(fmt.Sprintf("GetRetryBackoff: %s", err.Error()))
		}

		if err = util.Hang(ctx, backoff); err != nil {
			return errors.Trace(err)
		}
	}
}

func (s *ddlSinkImpl) observedRetrySinkAction(ctx context.Context, name string, action func() error) (err error) {
	errCh := make(chan error, 1)
	go func() { errCh <- s.retrySinkAction(ctx, name, action) }()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-errCh:
			return err
		case <-ticker.C:
			log.Info("owner ddl sink performs an action too long",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.String("action", name))
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
			if s.sinkV1 != nil {
				err = s.sinkV1.EmitCheckpointTs(ctx, checkpointTs, tables)
			} else {
				err = s.sinkV2.WriteCheckpointTs(ctx, checkpointTs, tables)
			}
		}
		if err == nil {
			*lastCheckpointTs = checkpointTs
		}
		return
	}

	return s.observedRetrySinkAction(ctx, "writeCheckpointTs", doWrite)
}

func (s *ddlSinkImpl) writeDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Info("begin emit ddl event",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID),
		zap.Any("DDL", ddl))

	doWrite := func() (err error) {
		if err = s.makeSinkReady(ctx); err == nil {
			if s.sinkV1 != nil {
				err = s.sinkV1.EmitDDLEvent(ctx, ddl)
			} else {
				err = s.sinkV2.WriteDDLEvent(ctx, ddl)
			}
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
			ddl.Done.Store(true)
			log.Info("Execute DDL succeeded",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Any("DDL", ddl))
		}
		return
	}

	return s.observedRetrySinkAction(ctx, "writeDDLEvent", doWrite)
}

func (s *ddlSinkImpl) run(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go func() {
		var err error
		log.Info("owner ddl sink background loop is started",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID))
		defer func() {
			s.wg.Done()
			log.Info("owner ddl sink background loop exits",
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID),
				zap.Error(err))
		}()

		// TODO make the tick duration configurable
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastCheckpointTs model.Ts
		for {
			// `ticker.C` and `ddlCh` may can be triggered at the same time, it
			// does not matter which one emit first, since TiCDC allow DDL with
			// CommitTs equal to the last CheckpointTs be emitted later.
			select {
			case <-ctx.Done():
				err = ctx.Err()
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
	if ddl.Done.Load() {
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

func (s *ddlSinkImpl) emitSyncPoint(ctx context.Context, checkpointTs uint64) (err error) {
	if checkpointTs == s.lastSyncPoint {
		return nil
	}
	s.lastSyncPoint = checkpointTs

	for {
		if err = s.makeSyncPointStoreReady(ctx); err == nil {
			// TODO implement async sink syncPoint
			err = s.syncPointStore.SinkSyncPoint(ctx, s.changefeedID, checkpointTs)
		}
		if err == nil {
			return nil
		}
		if !cerror.ShouldFailChangefeed(err) && errors.Cause(err) != context.Canceled {
			// TODO(qupeng): retry it internally after async sink syncPoint is ready.
			s.reportError(err)
			return err
		}
		s.reportError(err)
		return err
	}
}

func (s *ddlSinkImpl) close(ctx context.Context) (err error) {
	s.cancel()
	s.wg.Wait()

	// they will both be nil if changefeed return an error in initializing
	if s.sinkV1 != nil {
		_ = s.sinkV1.Close(ctx)
	} else if s.sinkV2 != nil {
		_ = s.sinkV2.Close()
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
	p := parser.New()
	// We need to use the correct SQL mode to parse the DDL query.
	// Otherwise, the parser may fail to parse the DDL query.
	// For example, it is needed to parse the following DDL query:
	//  `alter table "t" add column "c" int default 1;`
	// by adding `ANSI_QUOTES` to the SQL mode.
	mode, err := timysql.GetSQLMode(s.info.Config.SQLMode)
	if err != nil {
		return "", errors.Trace(err)
	}
	p.SetSQLMode(mode)
	stms, _, err := p.Parse(ddl.Query, ddl.Charset, ddl.Collate)
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
