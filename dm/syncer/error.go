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

package syncer

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"go.uber.org/zap"
)

// ignoreTrackerDDLError is also same with ignoreDDLError, but in order to keep tracker's table structure same as
// upstream's, we can't ignore "already exists" errors because already exists doesn't mean same.
func ignoreTrackerDDLError(err error) bool {
	switch {
	case infoschema.ErrDatabaseExists.Equal(err), infoschema.ErrDatabaseDropExists.Equal(err),
		infoschema.ErrTableDropExists.Equal(err),
		dbterror.ErrCantDropFieldOrKey.Equal(err):
		return true
	default:
		return false
	}
}

func isDropColumnWithIndexError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}
	// different version of TiDB has different error message, try to cover most versions
	return (mysqlErr.Number == errno.ErrUnsupportedDDLOperation || mysqlErr.Number == tmysql.ErrUnknown) &&
		strings.Contains(mysqlErr.Message, "drop column") &&
		(strings.Contains(mysqlErr.Message, "with index") ||
			strings.Contains(mysqlErr.Message, "with composite index") ||
			strings.Contains(mysqlErr.Message, "with tidb_enable_change_multi_schema is disable"))
}

func cloneStrings(values []string) []string {
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func (s *Syncer) setAsyncDDLReconcileInfo(
	ddls []string,
	index int,
	createTime int64,
	startLocation binlog.Location,
	currentLocation binlog.Location,
) *asyncDDLReconcileInfo {
	info := &asyncDDLReconcileInfo{
		ddls:            cloneStrings(ddls),
		index:           index,
		createTime:      createTime,
		startLocation:   startLocation.Clone(),
		currentLocation: currentLocation.Clone(),
	}
	s.asyncDDLReconcileMu.Lock()
	s.asyncDDLReconcile = info
	s.asyncDDLReconcileMu.Unlock()
	return info
}

func (s *Syncer) getAsyncDDLReconcileInfo() *asyncDDLReconcileInfo {
	s.asyncDDLReconcileMu.Lock()
	defer s.asyncDDLReconcileMu.Unlock()
	return s.asyncDDLReconcile
}

func (s *Syncer) clearAsyncDDLReconcileInfo(info *asyncDDLReconcileInfo) {
	s.asyncDDLReconcileMu.Lock()
	if s.asyncDDLReconcile == info {
		s.asyncDDLReconcile = nil
	}
	s.asyncDDLReconcileMu.Unlock()
}

func (s *Syncer) asyncDDLResolved(info *asyncDDLReconcileInfo) bool {
	s.asyncDDLReconcileMu.Lock()
	defer s.asyncDDLReconcileMu.Unlock()
	return s.asyncDDLReconcile == info && info.resolved
}

func (s *Syncer) markAsyncDDLResolved(info *asyncDDLReconcileInfo) {
	s.asyncDDLReconcileMu.Lock()
	if s.asyncDDLReconcile == info {
		info.resolved = true
	}
	s.asyncDDLReconcileMu.Unlock()
}

// clearResolvedAsyncDDL is called only after all post-DDL bookkeeping and the
// DDL checkpoint flush succeed. Keeping a resolved descriptor until this point
// prevents Resume from submitting the DDL again if bookkeeping fails after the
// TiDB job itself has completed.
func (s *Syncer) clearResolvedAsyncDDL(
	ddls []string,
	startLocation binlog.Location,
	currentLocation binlog.Location,
) {
	info := s.getAsyncDDLReconcileInfo()
	if info == nil || !s.asyncDDLResolved(info) ||
		!info.matches(ddls, startLocation, currentLocation, s.cfg.EnableGTID) {
		return
	}
	s.clearAsyncDDLReconcileInfo(info)
}

func (info *asyncDDLReconcileInfo) matches(
	ddls []string,
	startLocation binlog.Location,
	currentLocation binlog.Location,
	enableGTID bool,
) bool {
	if info == nil || len(info.ddls) != len(ddls) || info.index < 0 || info.index >= len(info.ddls) {
		return false
	}
	for i := range ddls {
		if info.ddls[i] != ddls[i] {
			return false
		}
	}
	return binlog.CompareLocation(info.startLocation, startLocation, enableGTID) == 0 &&
		binlog.CompareLocation(info.currentLocation, currentLocation, enableGTID) == 0
}

// asyncDDLPollContext returns the graceful-run context when Syncer.Run has
// initialized it. DDL execution continues to use syncCtx during the grace
// period; only polling an already-submitted asynchronous DDL is interrupted by
// the graceful deadline. The fallback keeps direct unit tests usable.
func (s *Syncer) asyncDDLPollContext(fallback *tcontext.Context) *tcontext.Context {
	if s.runCtx != nil {
		return s.runCtx
	}
	return fallback
}

// pollAsyncDDL waits for the TiDB DDL identified by its SQL and creation time.
// Canceling tctx only stops DM's polling query; it never sends a cancellation
// request for the TiDB DDL job itself.
func (s *Syncer) pollAsyncDDL(
	tctx *tcontext.Context,
	originErr error,
	ddl string,
	conn *dbconn.DBConn,
	createTime int64,
) error {
	duration := 30
	failpoint.Inject("ChangeDuration", func() {
		duration = 1
	})
	ticker := time.NewTicker(time.Duration(duration) * time.Second)
	defer ticker.Stop()

	for {
		if ctxErr := tctx.Ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		status, err := getDDLStatusFromTiDB(tctx, conn, ddl, createTime)
		if err != nil {
			if ctxErr := tctx.Ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			s.tctx.L().Warn("error when getting DDL status from TiDB", zap.Error(err))
		}
		failpoint.Inject("TestStatus", func(val failpoint.Value) {
			status = val.(string)
			s.tctx.L().Info("injected test status:", zap.String("TestStatus", status))
		})
		// Cancellation wins even when the status query itself completed. Without
		// this check, an empty or unexpected status racing with the graceful
		// deadline could be reported as the original invalid-connection error.
		if ctxErr := tctx.Ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		switch status {
		case model.JobStateDone.String(), model.JobStateSynced.String():
			return nil
		case model.JobStateCancelled.String(), model.JobStateRollingback.String(), model.JobStateRollbackDone.String(), model.JobStateCancelling.String():
			return terror.ErrSyncerCancelledDDL.Generate(ddl)
		case model.JobStateRunning.String(), model.JobStateQueueing.String(), model.JobStateNone.String():
		default:
			tctx.L().Warn("Unexpected DDL status", zap.String("DDL status", status))
			return originErr
		}
		select {
		case <-tctx.Ctx.Done():
			// Return the context error instead of the original invalid-connection
			// error so intentional Pause/Stop is classified as cancellation.
			return tctx.Ctx.Err()
		case <-ticker.C:
		}
	}
}

// reconcileAsyncDDL resolves an asynchronous DDL left by a previous paused
// run. The bool result reports whether a pending DDL matched the current job,
// in which case the caller must not submit the DDL again.
func (s *Syncer) reconcileAsyncDDL(
	tctx *tcontext.Context,
	ddls []string,
	startLocation binlog.Location,
	currentLocation binlog.Location,
	conn *dbconn.DBConn,
) (bool, error) {
	info := s.getAsyncDDLReconcileInfo()
	if info == nil {
		return false, nil
	}
	if !info.matches(ddls, startLocation, currentLocation, s.cfg.EnableGTID) {
		s.tctx.L().Error("pending asynchronous DDL does not match the current DDL",
			zap.Strings("pending DDLs", info.ddls),
			zap.Strings("current DDLs", ddls),
			zap.Stringer("pending start location", info.startLocation),
			zap.Stringer("current start location", startLocation),
			zap.Stringer("pending end location", info.currentLocation),
			zap.Stringer("current end location", currentLocation))
		// Do not execute a later DDL past an unresolved DDL barrier.
		return true, terror.ErrDBUnExpect.Generate("pending asynchronous DDL does not match the current DDL")
	}
	if s.asyncDDLResolved(info) {
		return true, nil
	}

	err := s.pollAsyncDDL(s.asyncDDLPollContext(tctx), mysql.ErrInvalidConn, info.ddls[info.index], conn, info.createTime)
	if err == nil {
		s.markAsyncDDLResolved(info)
	} else if terror.ErrSyncerCancelledDDL.Equal(err) {
		// TiDB has reached a terminal non-success state, so no unknown
		// downstream outcome remains to reconcile. Clear the descriptor after
		// surfacing the error and preserve the existing explicit-Resume retry
		// behavior.
		s.clearAsyncDDLReconcileInfo(info)
	}
	return true, err
}

// handleSpecialDDLError handles special errors for DDL execution
// if createTime equals to -1, skip the handle procedure for waitAsyncDDL.
func (s *Syncer) handleSpecialDDLError(
	tctx *tcontext.Context,
	err error,
	ddls []string,
	index int,
	conn *dbconn.DBConn,
	createTime int64,
	startLocation binlog.Location,
	currentLocation binlog.Location,
) error {
	// We use default parser because ddls are came from *Syncer.genDDLInfo, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
	parser2 := parser.New()

	// it only ignore `invalid connection` error (timeout or other causes) for `ADD INDEX`.
	// `invalid connection` means some data already sent to the server,
	// and we assume that the whole SQL statement has already sent to the server for this error.
	// if we have other methods to judge the DDL dispatched but timeout for executing, we can update this method.
	// NOTE: we must ensure other PK/UK exists for correctness.
	// NOTE: when we are refactoring the shard DDL algorithm, we also need to consider supporting non-blocking `ADD INDEX`.
	ignoreAddIndexTimeout := func(tctx *tcontext.Context, err error, ddls []string, index int, conn *dbconn.DBConn, createTime int64) error {
		// must ensure only the last statement executed failed with the `invalid connection` error
		if len(ddls) == 0 || index != len(ddls)-1 || errors.Cause(err) != mysql.ErrInvalidConn {
			return err // return the original error
		}

		ddl2 := ddls[index]
		stmt, err2 := parser2.ParseOneStmt(ddl2, "", "")
		if err2 != nil {
			return err // return the original error
		}

		handle := func() {
			tctx.L().Warn("ignore special error for DDL", zap.String("DDL", ddl2), log.ShortError(err))
			err2 := conn.ResetConn(tctx) // also reset the `invalid connection` for later use.
			if err2 != nil {
				tctx.L().Warn("reset connection failed", log.ShortError(err2))
			}
		}

		switch v := stmt.(type) {
		case *ast.AlterTableStmt:
			// ddls should be split with only one spec
			if len(v.Specs) > 1 {
				return err
			} else if v.Specs[0].Tp == ast.AlterTableAddConstraint {
				// only take effect on `ADD INDEX`, no UNIQUE KEY and FOREIGN KEY
				// UNIQUE KEY may affect correctness, FOREIGN KEY should be filtered.
				// ref https://github.com/pingcap/tidb/blob/3cdea0dfdf28197ee65545debce8c99e6d2945e3/ddl/ddl_api.go#L1929-L1948.
				switch v.Specs[0].Constraint.Tp {
				case ast.ConstraintKey, ast.ConstraintIndex:
					handle()
					return nil // ignore the error
				}
			}
		case *ast.CreateIndexStmt:
			handle()
			return nil // ignore the error
		}
		return err
	}

	// for DROP COLUMN with its single-column index, try drop index first then drop column
	// TiDB will support DROP COLUMN with index soon. After its support, executing that SQL will not have an error,
	// so this function will not trigger and cause some trouble
	dropColumnF := func(tctx *tcontext.Context, originErr error, ddls []string, index int, conn *dbconn.DBConn, createTime int64) error {
		if !isDropColumnWithIndexError(originErr) {
			return originErr
		}
		ddl2 := ddls[index]
		stmt, err2 := parser2.ParseOneStmt(ddl2, "", "")
		if err2 != nil {
			return originErr // return the original error
		}

		var (
			schema string
			table  string
			col    string
		)
		n, ok := stmt.(*ast.AlterTableStmt)
		switch {
		case !ok:
			return originErr
		case len(n.Specs) != 1:
			return originErr
		case n.Specs[0].Tp != ast.AlterTableDropColumn:
			return originErr
		default:
			schema = n.Table.Schema.O
			table = n.Table.Name.O
			col = n.Specs[0].OldColumnName.Name.O
		}
		tctx.L().Warn("try to fix drop column error", zap.String("DDL", ddl2), log.ShortError(originErr))

		// check if dependent index is single-column index on this column
		sql2 := "SELECT INDEX_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and COLUMN_NAME = ?"
		var rows *sql.Rows
		rows, err2 = conn.QuerySQL(tctx, s.metricsProxies, sql2, schema, table, col)
		if err2 != nil {
			return originErr
		}
		var (
			idx       string
			idx2Check []string
			idx2Drop  []string
			count     int
		)
		for rows.Next() {
			if err2 = rows.Scan(&idx); err2 != nil {
				// nolint:sqlclosecheck
				rows.Close()
				return originErr
			}
			idx2Check = append(idx2Check, idx)
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		// Close is idempotent, we could close in advance to reuse conn
		rows.Close()

		sql2 = "SELECT count(*) FROM information_schema.statistics WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and INDEX_NAME = ?"
		for _, idx := range idx2Check {
			rows, err2 = conn.QuerySQL(tctx, s.metricsProxies, sql2, schema, table, idx)
			if err2 != nil || !rows.Next() || rows.Scan(&count) != nil || count != 1 {
				tctx.L().Warn("can't auto drop index", zap.String("index", idx))
				if rows != nil {
					// nolint: sqlclosecheck
					rows.Close()
				}
				return originErr
			}
			if rows.Err() != nil {
				return rows.Err()
			}
			idx2Drop = append(idx2Drop, idx)
			rows.Close()
		}

		sqls := make([]string, len(idx2Drop))
		for i, idx := range idx2Drop {
			sqls[i] = fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", dbutil.TableName(schema, table), dbutil.ColumnName(idx))
		}
		if _, err2 = conn.ExecuteSQL(tctx, s.metricsProxies, sqls); err2 != nil {
			tctx.L().Warn("auto drop index failed", log.ShortError(err2))
			return originErr
		}

		tctx.L().Info("drop index success, now try to drop column", zap.Strings("index", idx2Drop))
		if _, err2 = conn.ExecuteSQLWithIgnore(tctx, s.metricsProxies, errorutil.IsIgnorableMySQLDDLError, ddls[index:]); err2 != nil {
			return err2
		}

		tctx.L().Info("execute drop column SQL success", zap.String("DDL", ddl2))
		return nil
	}
	// TODO: add support for downstream alter pk without schema

	// it handles the operations for DDL when encountering `invalid connection` by waiting the asynchronous ddl to synchronize
	waitAsyncDDL := func(tctx *tcontext.Context, err error, ddls []string, index int, conn *dbconn.DBConn, createTime int64) error {
		if len(ddls) == 0 || index < 0 || index > len(ddls)-1 || errors.Cause(err) != mysql.ErrInvalidConn || createTime == -1 {
			return err // return the original error
		}

		info := s.setAsyncDDLReconcileInfo(ddls, index, createTime, startLocation, currentLocation)
		retErr := s.pollAsyncDDL(s.asyncDDLPollContext(tctx), err, info.ddls[info.index], conn, info.createTime)
		if retErr == nil {
			s.markAsyncDDLResolved(info)
		} else if terror.ErrSyncerCancelledDDL.Equal(retErr) {
			// A terminal cancelled/rollback state is a known failure rather than
			// an unknown async result. Surface it now and allow an explicit Resume
			// to use the syncer's normal retry/recovery path.
			s.clearAsyncDDLReconcileInfo(info)
		}
		return retErr
	}

	retErr := err
	toHandle := []func(*tcontext.Context, error, []string, int, *dbconn.DBConn, int64) error{
		dropColumnF,
		ignoreAddIndexTimeout,
		waitAsyncDDL,
	}
	for _, f := range toHandle {
		retErr = f(tctx, retErr, ddls, index, conn, createTime)
		if retErr == nil {
			break
		}
	}
	return retErr
}

func isDuplicateServerIDError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "A slave with the same server_uuid/server_id as this slave has connected to the master")
}

func isConnectionRefusedError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "connect: connection refused")
}
