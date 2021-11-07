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
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
)

const (
	// expose these two variables for redo log applier
	DefaultWorkerCount = 16
	DefaultMaxTxnRow   = 256

	defaultDMLMaxRetryTime     = 8
	defaultDDLMaxRetryTime     = 20
	defaultTiDBTxnMode         = "optimistic"
	defaultFlushInterval       = time.Millisecond * 50
	defaultBatchReplaceEnabled = true
	defaultBatchReplaceSize    = 20
	defaultReadTimeout         = "2m"
	defaultWriteTimeout        = "2m"
	defaultDialTimeout         = "2m"
	defaultSafeMode            = true
)

var defaultParams = &sinkParams{
	workerCount:         DefaultWorkerCount,
	maxTxnRow:           DefaultMaxTxnRow,
	tidbTxnMode:         defaultTiDBTxnMode,
	batchReplaceEnabled: defaultBatchReplaceEnabled,
	batchReplaceSize:    defaultBatchReplaceSize,
	readTimeout:         defaultReadTimeout,
	writeTimeout:        defaultWriteTimeout,
	dialTimeout:         defaultDialTimeout,
	safeMode:            defaultSafeMode,
}

var validSchemes = map[string]bool{
	"mysql":     true,
	"mysql+ssl": true,
	"tidb":      true,
	"tidb+ssl":  true,
}

type sinkParams struct {
	workerCount         int
	maxTxnRow           int
	tidbTxnMode         string
	changefeedID        string
	captureAddr         string
	batchReplaceEnabled bool
	batchReplaceSize    int
	readTimeout         string
	writeTimeout        string
	dialTimeout         string
	enableOldValue      bool
	safeMode            bool
	timezone            string
	tls                 string
}

func (s *sinkParams) Clone() *sinkParams {
	clone := *s
	return &clone
}

func parseSinkURIToParams(ctx context.Context, sinkURI *url.URL, opts map[string]string) (*sinkParams, error) {
	params := defaultParams.Clone()

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if caddr, ok := opts[OptCaptureAddr]; ok {
		params.captureAddr = caddr
	}

	if sinkURI == nil {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("fail to open MySQL sink, empty URL")
	}
	scheme := strings.ToLower(sinkURI.Scheme)
	if _, ok := validSchemes[scheme]; !ok {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("can't create mysql sink with unsupported scheme: %s", scheme)
	}
	s := sinkURI.Query().Get("worker-count")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		if c > 0 {
			params.workerCount = c
		}
	}
	s = sinkURI.Query().Get("max-txn-row")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.maxTxnRow = c
	}
	s = sinkURI.Query().Get("tidb-txn-mode")
	if s != "" {
		if s == "pessimistic" || s == "optimistic" {
			params.tidbTxnMode = s
		} else {
			log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic, use optimistic as default")
		}
	}
	if sinkURI.Query().Get("ssl-ca") != "" {
		credential := security.Credential{
			CAPath:   sinkURI.Query().Get("ssl-ca"),
			CertPath: sinkURI.Query().Get("ssl-cert"),
			KeyPath:  sinkURI.Query().Get("ssl-key"),
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		name := "cdc_mysql_tls" + params.changefeedID
		err = dmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		params.tls = "?tls=" + name
	}

	s = sinkURI.Query().Get("batch-replace-enable")
	if s != "" {
		enable, err := strconv.ParseBool(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.batchReplaceEnabled = enable
	}
	if params.batchReplaceEnabled && sinkURI.Query().Get("batch-replace-size") != "" {
		size, err := strconv.Atoi(sinkURI.Query().Get("batch-replace-size"))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.batchReplaceSize = size
	}

	// TODO: force safe mode in startup phase
	s = sinkURI.Query().Get("safe-mode")
	if s != "" {
		safeModeEnabled, err := strconv.ParseBool(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.safeMode = safeModeEnabled
	}

	if _, ok := sinkURI.Query()["time-zone"]; ok {
		s = sinkURI.Query().Get("time-zone")
		if s == "" {
			params.timezone = ""
		} else {
			params.timezone = fmt.Sprintf(`"%s"`, s)
		}
	} else {
		tz := util.TimezoneFromCtx(ctx)
		params.timezone = fmt.Sprintf(`"%s"`, tz.String())
	}

	// read, write, and dial timeout for each individual connection, equals to
	// readTimeout, writeTimeout, timeout in go mysql driver respectively.
	// ref: https://github.com/go-sql-driver/mysql#connection-pool-and-timeouts
	// To keep the same style with other sink parameters, we use dash as word separator.
	s = sinkURI.Query().Get("read-timeout")
	if s != "" {
		params.readTimeout = s
	}
	s = sinkURI.Query().Get("write-timeout")
	if s != "" {
		params.writeTimeout = s
	}
	s = sinkURI.Query().Get("timeout")
	if s != "" {
		params.dialTimeout = s
	}

	return params, nil
}

func generateDSNByParams(
	ctx context.Context,
	dsnCfg *dmysql.Config,
	params *sinkParams,
	testDB *sql.DB,
) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	// if timezone is empty string, we don't pass this variable in dsn
	if params.timezone != "" {
		dsnCfg.Params["time_zone"] = params.timezone
	}
	dsnCfg.Params["readTimeout"] = params.readTimeout
	dsnCfg.Params["writeTimeout"] = params.writeTimeout
	dsnCfg.Params["timeout"] = params.dialTimeout

	autoRandom, err := checkTiDBVariable(ctx, testDB, "allow_auto_random_explicit_insert", "1")
	if err != nil {
		return "", err
	}
	if autoRandom != "" {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = autoRandom
	}

	txnMode, err := checkTiDBVariable(ctx, testDB, "tidb_txn_mode", params.tidbTxnMode)
	if err != nil {
		return "", err
	}
	if txnMode != "" {
		dsnCfg.Params["tidb_txn_mode"] = txnMode
	}

	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("format dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
}

func checkTiDBVariable(ctx context.Context, db *sql.DB, variableName, defaultValue string) (string, error) {
	var name string
	var value string
	querySQL := fmt.Sprintf("show session variables like '%s';", variableName)
	err := db.QueryRowContext(ctx, querySQL).Scan(&name, &value)
	if err != nil && err != sql.ErrNoRows {
		errMsg := "fail to query session variable " + variableName
		return "", cerror.ErrMySQLQueryError.Wrap(err).GenWithStack(errMsg)
	}
	// session variable works, use given default value
	if err == nil {
		return defaultValue, nil
	}
	// session variable not exists, return "" to ignore it
	return "", nil
}
