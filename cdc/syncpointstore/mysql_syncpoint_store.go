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

package syncpointstore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

type mysqlSyncPointStore struct {
	db                     *sql.DB
	clusterID              string
	syncPointRetention     time.Duration
	lastCleanSyncPointTime time.Time
}

// newSyncPointStore create a sink to record the syncPoint map in downstream DB for every changefeed
func newMySQLSyncPointStore(
	ctx context.Context,
	id model.ChangeFeedID,
	sinkURI *url.URL,
	syncPointRetention time.Duration,
) (SyncPointStore, error) {
	cfg := mysql.NewConfig()
	err := cfg.Apply(config.GetGlobalServerConfig().TZ, id, sinkURI, config.GetDefaultReplicaConfig())
	if err != nil {
		return nil, err
	}
	getTestDb := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		testDB, err := sql.Open("mysql", dsnStr)
		if err != nil {
			return nil, err
		}

		return testDB, nil
	}
	dsnStr, err := mysql.GenerateDSN(ctx, sinkURI, cfg, getTestDb)
	if err != nil {
		return nil, errors.Trace(err)
	}
	syncDB, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	err = syncDB.PingContext(ctx)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	log.Info("Start mysql syncpoint sink", zap.String("changefeed", id.String()))

	return &mysqlSyncPointStore{
		db:                     syncDB,
		clusterID:              config.GetGlobalServerConfig().ClusterID,
		syncPointRetention:     syncPointRetention,
		lastCleanSyncPointTime: time.Now(),
	}, nil
}

func (s *mysqlSyncPointStore) CreateSyncTable(ctx context.Context) error {
	database := filter.TiCDCSystemSchema
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("create sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "create sync table: begin Tx fail;"))
	}
	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	_, err = tx.Exec("USE " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		primary_ts varchar(18),
		secondary_ts varchar(18),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (created_at),
		PRIMARY KEY (changefeed, primary_ts)
	);`
	query = fmt.Sprintf(query, filter.SyncPointTable)
	_, err = tx.Exec(query)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to create syncpoint table;"))
}

func (s *mysqlSyncPointStore) SinkSyncPoint(ctx context.Context,
	id model.ChangeFeedID,
	checkpointTs uint64,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "sync table: begin Tx fail;"))
	}
	row := tx.QueryRow("select @@tidb_current_ts")
	var secondaryTs string
	err = row.Scan(&secondaryTs)
	if err != nil {
		log.Info("sync table: get tidb_current_ts err", zap.String("changefeed", id.String()))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
	}
	// insert ts map
	query := "insert ignore into " + filter.TiCDCSystemSchema + "." + filter.SyncPointTable +
		"(ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES (?,?,?,?)"
	_, err = tx.Exec(query, s.clusterID, id.ID, checkpointTs, secondaryTs)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
	}

	// set global tidb_external_ts to secondary ts
	// TiDB supports tidb_external_ts system variable since v6.4.0.
	query = fmt.Sprintf("set global tidb_external_ts = %s", secondaryTs)
	_, err = tx.Exec(query)
	if err != nil {
		if errorutil.IsSyncPointIgnoreError(err) {
			// TODO(dongmen): to confirm if we need to log this error.
			log.Warn("set global external ts failed, ignore this error", zap.Error(err))
		} else {
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write syncpoint table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
		}
	}

	// clean stale ts map in downstream
	if time.Since(s.lastCleanSyncPointTime) >= s.syncPointRetention {
		query = fmt.Sprintf(
			"DELETE IGNORE FROM "+
				filter.TiCDCSystemSchema+"."+
				filter.SyncPointTable+
				" WHERE ticdc_cluster_id = '%s' and changefeed = '%s' and created_at < (NOW() - INTERVAL %.2f SECOND)",
			s.clusterID,
			id.ID,
			s.syncPointRetention.Seconds())
		_, err = tx.Exec(query)
		if err != nil {
			// It is ok to ignore the error, since it will not affect the correctness of the system,
			// and no any business logic depends on this behavior, so we just log the error.
			log.Error("failed to clean syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err)))
		} else {
			s.lastCleanSyncPointTime = time.Now()
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table;"))
}

func (s *mysqlSyncPointStore) Close() error {
	err := s.db.Close()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}
