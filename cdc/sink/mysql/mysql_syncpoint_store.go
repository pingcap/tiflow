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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/security"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
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
	var syncDB *sql.DB

	// todo If is neither mysql nor tidb, such as kafka, just ignore this feature.
	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme != "mysql" && scheme != "tidb" && scheme != "mysql+ssl" && scheme != "tidb+ssl" {
		return nil, errors.New("can not create mysql sink with unsupported scheme")
	}
	params := defaultParams.Clone()
	s := sinkURI.Query().Get("tidb-txn-mode")
	if s != "" {
		if s == "pessimistic" || s == "optimistic" {
			params.tidbTxnMode = s
		} else {
			log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic, use optimistic as default")
		}
	}
	var tlsParam string
	if sinkURI.Query().Get("ssl-ca") != "" {
		credential := security.Credential{
			CAPath:   sinkURI.Query().Get("ssl-ca"),
			CertPath: sinkURI.Query().Get("ssl-cert"),
			KeyPath:  sinkURI.Query().Get("ssl-key"),
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		name := "cdc_mysql_tls" + "syncpoint" + id.ID
		err = gmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		tlsParam = "?tls=" + name
	}
	if _, ok := sinkURI.Query()["time-zone"]; ok {
		s = sinkURI.Query().Get("time-zone")
		if s == "" {
			params.timezone = ""
		} else {
			params.timezone = fmt.Sprintf(`"%s"`, s)
		}
	} else {
		tz := contextutil.TimezoneFromCtx(ctx)
		params.timezone = fmt.Sprintf(`"%s"`, tz.String())
	}

	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := sinkURI.User.Password()
	port := sinkURI.Port()
	if port == "" {
		port = "4000"
	}

	// This will handle the IPv6 address format.
	host := net.JoinHostPort(sinkURI.Hostname(), port)

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, tlsParam)
	dsn, err := gmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// create test db used for parameter detection
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}

	testDB, err := pmysql.GetTestDB(ctx, dsn, GetDBConnImpl)
	if err != nil {
		return nil, err
	}
	defer testDB.Close()

	dsnStr, err = generateDSNByParams(ctx, dsn, params, testDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	syncDB, err = sql.Open("mysql", dsnStr)
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	_, err = tx.Exec("USE " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

func (s *mysqlSyncPointStore) SinkSyncPoint(ctx context.Context,
	id model.ChangeFeedID,
	checkpointTs uint64,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
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
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
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
			return cerror.WrapError(cerror.ErrMySQLTxnError, err)
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
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

func (s *mysqlSyncPointStore) Close() error {
	err := s.db.Close()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}
