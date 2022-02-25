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

package sink

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/verification"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// syncpointTableName is the name of table where all syncpoint maps sit
const (
	syncpointDatabaseName string = "tidb_cdc"
	syncpointTableName    string = "syncpoint_v1"
)

type mysqlSyncpointStore struct {
	db *sql.DB
}

// newSyncpointStore create a sink to record the syncpoint map in downstream DB for every changefeed
func newMySQLSyncpointStore(ctx context.Context, id string, sinkURI *url.URL, sourceURI *url.URL, interval time.Duration, filter *filter.Filter) (SyncpointStore, error) {
	var syncDB *sql.DB

	// todo If is neither mysql nor tidb, such as kafka, just ignore this feature.
	sinkDSNStr, err := generateDSNStr(ctx, id, sinkURI, "sink")
	if err != nil {
		return nil, errors.Trace(err)
	}
	syncDB, err = sql.Open("mysql", sinkDSNStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	err = syncDB.PingContext(ctx)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	syncpointStore := &mysqlSyncpointStore{
		db: syncDB,
	}
	sourceDSNStr, err := generateDSNStr(ctx, id, sourceURI, "source")
	if err != nil {
		return nil, errors.Trace(err)
	}

	//TODO; support mysql later
	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme == "tidb" || scheme == "tidb+ssl" {
		cfg := verification.Config{
			// delay the verification in case syncpoint not started immediately
			CheckInterval: interval + time.Second,
			UpstreamDSN:   sourceDSNStr,
			DownStreamDSN: sinkDSNStr,
			Filter:        filter,
			DataBaseName:  syncpointDatabaseName,
			TableName:     syncpointTableName,
			ChangefeedID:  id,
		}
		err = verification.NewVerification(ctx, &cfg)
		if err != nil {
			log.Warn("Start verification fail", zap.Error(err))
			return nil, err
		}
	}
	log.Info("Start mysql syncpoint sink")

	return syncpointStore, nil
}

func generateDSNStr(ctx context.Context, id string, uri *url.URL, dsnType string) (string, error) {
	scheme := strings.ToLower(uri.Scheme)
	if scheme != "mysql" && scheme != "tidb" && scheme != "mysql+ssl" && scheme != "tidb+ssl" {
		return "", errors.New("can create mysql sink with unsupported scheme")
	}

	params := defaultParams.Clone()
	s := uri.Query().Get("tidb-txn-mode")
	if s != "" {
		if s == "pessimistic" || s == "optimistic" {
			params.tidbTxnMode = s
		} else {
			log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic, use optimistic as default")
		}
	}
	var tlsParam string
	if uri.Query().Get("ssl-ca") != "" {
		credential := security.Credential{
			CAPath:   uri.Query().Get("ssl-ca"),
			CertPath: uri.Query().Get("ssl-cert"),
			KeyPath:  uri.Query().Get("ssl-key"),
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		name := fmt.Sprintf("cdc_mysql_tls_syncpoint_%s_%s", id, dsnType)
		err = dmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		tlsParam = "?tls=" + name
	}
	if _, ok := uri.Query()["time-zone"]; ok {
		s = uri.Query().Get("time-zone")
		if s == "" {
			params.timezone = ""
		} else {
			params.timezone = fmt.Sprintf(`"%s"`, s)
		}
	} else {
		tz := util.TimezoneFromCtx(ctx)
		params.timezone = fmt.Sprintf(`"%s"`, tz.String())
	}

	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := uri.User.Username()
	password, _ := uri.User.Password()
	port := uri.Port()
	if username == "" {
		username = "root"
	}
	if port == "" {
		port = "4000"
	}

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, uri.Hostname(), port, tlsParam)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return "", errors.Trace(err)
	}

	// create test db used for parameter detection
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	testDB, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack(fmt.Sprintf("fail to open MySQL connection when configuring %s", dsnType))
	}
	defer testDB.Close()
	dsnStr, err = generateDSNByParams(ctx, dsn, params, testDB)
	if err != nil {
		return "", errors.Trace(err)
	}

	return dsnStr, nil
}

func (s *mysqlSyncpointStore) CreateSynctable(ctx context.Context) error {
	database := syncpointDatabaseName
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("create sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err2)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	_, err = tx.Exec("USE " + database)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err2)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	// TODO: gc, ttl for tidb, partition table for mysql
	_, err = tx.Exec("CREATE TABLE  IF NOT EXISTS " + syncpointTableName + " (cf varchar(255),primary_ts varchar(18),secondary_ts varchar(18), result int, PRIMARY KEY ( `cf`, `primary_ts` ) )")
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to create syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err2)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

func (s *mysqlSyncpointStore) SinkSyncpoint(ctx context.Context, id string, checkpointTs uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("sync table: begin Tx fail", zap.Error(err))
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	row := tx.QueryRow("select @@tidb_current_ts")
	var secondaryTs string
	err = row.Scan(&secondaryTs)
	if err != nil {
		log.Info("sync table: get tidb_current_ts err")
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err2)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	_, err = tx.Exec("insert ignore into "+syncpointDatabaseName+"."+syncpointTableName+"(cf, primary_ts, secondary_ts, result) VALUES (?,?,?,?)", id, checkpointTs, secondaryTs, 0)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err2)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

func (s *mysqlSyncpointStore) Close() error {
	err := s.db.Close()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}
