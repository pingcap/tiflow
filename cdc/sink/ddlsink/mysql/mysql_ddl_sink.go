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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	defaultSupportVectorVersion = "8.4.0"
)

// GetDBConnImpl is the implementation of pmysql.IDBConnectionFactory.
// Exported for testing.
var GetDBConnImpl pmysql.IDBConnectionFactory = &pmysql.DBConnectionFactory{}

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that writes DDL events to MySQL.
type DDLSink struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// db is the database connection.
	db  *sql.DB
	cfg *pmysql.Config
	// statistics is the statistics of this sink.
	// We use it to record the DDL count.
	statistics *metrics.Statistics

	needFormat bool
}

// NewDDLSink creates a new DDLSink.
func NewDDLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (*DDLSink, error) {
	cfg := pmysql.NewConfig()
	err := cfg.Apply(config.GetGlobalServerConfig().TZ, changefeedID, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, GetDBConnImpl.CreateTemporaryConnection)
	if err != nil {
		return nil, err
	}

	db, err := GetDBConnImpl.CreateStandardConnection(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	cfg.IsTiDB = pmysql.CheckIsTiDB(ctx, db)

	cfg.IsWriteSourceExisted, err = pmysql.CheckIfBDRModeIsSupported(ctx, db)
	if err != nil {
		return nil, err
	}

	m := &DDLSink{
		id:         changefeedID,
		db:         db,
		cfg:        cfg,
		statistics: metrics.NewStatistics(changefeedID, sink.TxnSink),
		needFormat: needFormatDDL(db, cfg),
	}

	log.Info("MySQL DDL sink is created",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID))
	return m, nil
}

// WriteDDLEvent writes a DDL event to the mysql database.
func (m *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.waitAsynExecDone(ctx, ddl)

	if err := m.execDDLWithMaxRetries(ctx, ddl); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// execDDLWithMaxRetries will retry executing DDL statements.
// When a DDL execution takes a long time and an invalid connection error occurs.
// If the downstream is TiDB, it will query the DDL and wait until it finishes.
// For 'add index' ddl, it will return immediately without waiting and will query it during the next DDL execution.
func (m *DDLSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
	ddlCreateTime := getDDLCreateTime(ctx, m.db)
	return retry.Do(ctx, func() error {
		err := m.statistics.RecordDDLExecution(func() error { return m.execDDL(ctx, ddl) })
		if err != nil {
			if errorutil.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			if m.cfg.IsTiDB && ddlCreateTime != "" && errors.Cause(err) == mysql.ErrInvalidConn {
				log.Warn("Wait the asynchronous ddl to synchronize", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime),
					zap.String("readTimeout", m.cfg.ReadTimeout), zap.Error(err))
				return m.waitDDLDone(ctx, ddl, ddlCreateTime)
			}
			log.Warn("Execute DDL with error, retry later",
				zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

func (m *DDLSink) execDDL(pctx context.Context, ddl *model.DDLEvent) error {
	ctx := pctx
	shouldSwitchDB := needSwitchDB(ddl)

	// Convert vector type to string type for unsupport database
	if m.needFormat {
		if newQuery := formatQuery(ddl.Query); newQuery != ddl.Query {
			log.Warn("format ddl query", zap.String("newQuery", newQuery), zap.String("query", ddl.Query), zap.String("collate", ddl.Collate), zap.String("charset", ddl.Charset))
			ddl.Query = newQuery
		}
	}

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})

	start := time.Now()
	log.Info("Start exec DDL", zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.TableName.Schema)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = pmysql.SetWriteSource(pctx, m.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback",
					zap.String("namespace", m.id.Namespace),
					zap.String("changefeed", m.id.ID), zap.Error(err))
			}
		}
		return err
	}

	if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.String("sql", ddl.Query),
				zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", ddl.Query)))
	}

	log.Info("Exec DDL succeeded",
		zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID),
		zap.Duration("duration", time.Since(start)), zap.String("sql", ddl.Query))
	return nil
}

func (m *DDLSink) waitDDLDone(ctx context.Context, ddl *model.DDLEvent, ddlCreateTime string) error {
	ticker := time.NewTicker(5 * time.Second)
	ticker1 := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer ticker1.Stop()
	for {
		state, err := getDDLStateFromTiDB(ctx, m.db, ddl.Query, ddlCreateTime)
		if err != nil {
			log.Error("Error when getting DDL state from TiDB", zap.Error(err))
		}
		switch state {
		case timodel.JobStateDone, timodel.JobStateSynced:
			log.Info("DDL replicate success", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime))
			return nil
		case timodel.JobStateCancelled, timodel.JobStateRollingback, timodel.JobStateRollbackDone, timodel.JobStateCancelling:
			return errors.ErrExecDDLFailed.GenWithStackByArgs(ddl.Query)
		case timodel.JobStateRunning, timodel.JobStateQueueing:
			switch ddl.Type {
			// returned immediately if not block dml
			case timodel.ActionAddIndex:
				log.Info("DDL is running downstream", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime), zap.Any("ddlState", state))
				return nil
			}
		default:
			log.Warn("Unexpected DDL state, may not be found downstream", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime), zap.Any("ddlState", state))
			return errors.ErrDDLStateNotFound.GenWithStackByArgs(state)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-ticker1.C:
			log.Info("DDL is still running downstream, it blocks other DDL or DML events", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime))
		}
	}
}

// WriteCheckpointTs does nothing.
func (m *DDLSink) WriteCheckpointTs(_ context.Context, _ uint64, _ []*model.TableInfo) error {
	// Only for RowSink for now.
	return nil
}

// Close closes the database connection.
func (m *DDLSink) Close() {
	if m.statistics != nil {
		m.statistics.Close()
	}
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			log.Warn("MySQL ddl sink close db wit error",
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID),
				zap.Error(err))
		}
	}
}

func needSwitchDB(ddl *model.DDLEvent) bool {
	if len(ddl.TableInfo.TableName.Schema) == 0 {
		return false
	}
	if ddl.Type == timodel.ActionCreateSchema || ddl.Type == timodel.ActionDropSchema {
		return false
	}
	return true
}

// needFormatDDL checks vector type support
func needFormatDDL(db *sql.DB, cfg *pmysql.Config) bool {
	if !cfg.HasVectorType {
		log.Warn("please set `has-vector-type` to be true if a column is vector type when the downstream is not TiDB or TiDB version less than specify version",
			zap.Any("hasVectorType", cfg.HasVectorType), zap.Any("supportVectorVersion", defaultSupportVectorVersion))
		return false
	}
	versionInfo, err := export.SelectVersion(db)
	if err != nil {
		log.Warn("fail to get version", zap.Error(err), zap.Bool("isTiDB", cfg.IsTiDB))
		return false
	}
	serverInfo := version.ParseServerInfo(versionInfo)
	version := semver.New(defaultSupportVectorVersion)
	if !cfg.IsTiDB || serverInfo.ServerVersion.LessThan(*version) {
		log.Error("downstream unsupport vector type. it will be converted to longtext", zap.String("version", serverInfo.ServerVersion.String()), zap.String("supportVectorVersion", defaultSupportVectorVersion), zap.Bool("isTiDB", cfg.IsTiDB))
		return true
	}
	return false
}

func getDDLCreateTime(ctx context.Context, db *sql.DB) string {
	ddlCreateTime := "" // default when scan failed
	row, err := db.QueryContext(ctx, "BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;")
	if err != nil {
		log.Warn("selecting tidb current timestamp failed", zap.Error(err))
	} else {
		for row.Next() {
			err = row.Scan(&ddlCreateTime)
			if err != nil {
				log.Warn("getting ddlCreateTime failed", zap.Error(err))
			}
		}
		//nolint:sqlclosecheck
		_ = row.Close()
		_ = row.Err()
	}
	return ddlCreateTime
}

// getDDLStateFromTiDB retrieves the ddl job status of the ddl query from downstream tidb based on the ddl query and the approximate ddl create time.
func getDDLStateFromTiDB(ctx context.Context, db *sql.DB, ddl string, createTime string) (timodel.JobState, error) {
	// ddlCreateTime and createTime are both based on UTC timezone of downstream
	showJobs := fmt.Sprintf(`SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY FROM information_schema.ddl_jobs 
	WHERE CREATE_TIME >= "%s" AND QUERY = "%s";`, createTime, ddl)
	//nolint:rowserrcheck
	jobsRows, err := db.QueryContext(ctx, showJobs)
	if err != nil {
		return timodel.JobStateNone, err
	}

	var jobsResults [][]string
	jobsResults, err = export.GetSpecifiedColumnValuesAndClose(jobsRows, "QUERY", "STATE", "JOB_ID", "JOB_TYPE", "SCHEMA_STATE")
	if err != nil {
		return timodel.JobStateNone, err
	}
	if len(jobsResults) > 0 {
		result := jobsResults[0]
		state, jobID, jobType, schemaState := result[1], result[2], result[3], result[4]
		log.Debug("Find ddl state in downstream",
			zap.String("jobID", jobID),
			zap.String("jobType", jobType),
			zap.String("schemaState", schemaState),
			zap.String("ddl", ddl),
			zap.String("state", state),
			zap.Any("jobsResults", jobsResults),
		)
		return timodel.StrToJobState(result[1]), nil
	}
	return timodel.JobStateNone, nil
}
