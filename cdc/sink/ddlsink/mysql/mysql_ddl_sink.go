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
	"net/url"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second
)

// GetDBConnImpl is the implementation of pmysql.Factory.
// Exported for testing.
var GetDBConnImpl pmysql.Factory = pmysql.CreateMySQLDBConn

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

	dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, GetDBConnImpl)
	if err != nil {
		return nil, err
	}

	db, err := GetDBConnImpl(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	cfg.IsTiDB, err = pmysql.CheckIsTiDB(ctx, db)
	if err != nil {
		return nil, err
	}

	cfg.IsWriteSourceExisted, err = pmysql.CheckIfBDRModeIsSupported(ctx, db)
	if err != nil {
		return nil, err
	}

	m := &DDLSink{
		id:         changefeedID,
		db:         db,
		cfg:        cfg,
		statistics: metrics.NewStatistics(ctx, changefeedID, sink.TxnSink),
	}

	log.Info("MySQL DDL sink is created",
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID))
	return m, nil
}

// WriteDDLEvent writes a DDL event to the mysql database.
func (m *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if ddl.Type == timodel.ActionAddIndex && m.cfg.IsTiDB {
		return m.asyncExecAddIndexDDLIfTimeout(ctx, ddl)
	}
	return m.execDDLWithMaxRetries(ctx, ddl)
}

func (m *DDLSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
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

// isReorgOrPartitionDDL returns true if given ddl type is reorg ddl or
// partition ddl.
func isReorgOrPartitionDDL(t timodel.ActionType) bool {
	// partition related ddl
	return t == timodel.ActionAddTablePartition ||
		t == timodel.ActionExchangeTablePartition ||
		t == timodel.ActionReorganizePartition ||
		// reorg ddls
		t == timodel.ActionAddPrimaryKey ||
		t == timodel.ActionAddIndex ||
		t == timodel.ActionModifyColumn ||
		// following ddls can be fast when the downstream is TiDB, we must
		// still take them into consideration to ensure compatibility with all
		// MySQL-compatible databases.
		t == timodel.ActionAddColumn ||
		t == timodel.ActionAddColumns ||
		t == timodel.ActionDropColumn ||
		t == timodel.ActionDropColumns
}

func (m *DDLSink) execDDL(pctx context.Context, ddl *model.DDLEvent) error {
	ctx := pctx
	// When executing Reorg and Partition DDLs in TiDB, there is no timeout
	// mechanism by default. Instead, the system will wait for the DDL operation
	// to be executed or completed before proceeding.
	if !isReorgOrPartitionDDL(ddl.Type) {
		writeTimeout, _ := time.ParseDuration(m.cfg.WriteTimeout)
		writeTimeout += networkDriftDuration
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(pctx, writeTimeout)
		defer cancelFunc()
	}

	shouldSwitchDB := needSwitchDB(ddl)

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})

	start := time.Now()
	log.Info("Start exec DDL", zap.String("DDL", ddl.Query), zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("namespace", m.id.Namespace), zap.String("changefeed", m.id.ID))
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
			log.Error("Failed to rollback", zap.String("sql", ddl.Query),
				zap.String("namespace", m.id.Namespace),
				zap.String("changefeed", m.id.ID), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error("Failed to exec DDL", zap.String("sql", ddl.Query),
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", m.id.Namespace),
			zap.String("changefeed", m.id.ID), zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query),
		zap.Duration("duration", time.Since(start)),
		zap.String("namespace", m.id.Namespace),
		zap.String("changefeed", m.id.ID))
	return nil
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

// asyncExecAddIndexDDLIfTimeout executes ddl in async mode.
// this function only works in TiDB, because TiDB will save ddl jobs
// and execute them asynchronously even if ticdc crashed.
func (m *DDLSink) asyncExecAddIndexDDLIfTimeout(ctx context.Context, ddl *model.DDLEvent) error {
	done := make(chan error, 1)
	// wait for 2 seconds at most
	tick := time.NewTimer(2 * time.Second)
	defer tick.Stop()
	log.Info("async exec add index ddl start",
		zap.String("changefeedID", m.id.String()),
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("ddl", ddl.Query))
	go func() {
		if err := m.execDDLWithMaxRetries(ctx, ddl); err != nil {
			log.Error("async exec add index ddl failed",
				zap.String("changefeedID", m.id.String()),
				zap.Uint64("commitTs", ddl.CommitTs),
				zap.String("ddl", ddl.Query))
			done <- err
			return
		}
		log.Info("async exec add index ddl done",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		done <- nil
	}()

	select {
	case <-ctx.Done():
		// if the ddl is canceled, we just return nil, if the ddl is not received by tidb,
		// the downstream ddl is lost, because the checkpoint ts is forwarded.
		log.Info("async add index ddl exits as canceled",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		return nil
	case err := <-done:
		// if the ddl is executed within 2 seconds, we just return the result to the caller.
		return err
	case <-tick.C:
		// if the ddl is still running, we just return nil,
		// then if the ddl is failed, the downstream ddl is lost.
		// because the checkpoint ts is forwarded.
		log.Info("async add index ddl is still running",
			zap.String("changefeedID", m.id.String()),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("ddl", ddl.Query))
		return nil
	}
}
