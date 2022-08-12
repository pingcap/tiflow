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
	"strconv"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

const (
	txnModeOptimistic  = "optimistic"
	txnModePessimistic = "pessimistic"

	// defaultWorkerCount is the default number of workers.
	defaultWorkerCount = 16
	// defaultMaxTxnRow is the default max number of rows in a transaction.
	defaultMaxTxnRow = 256
	// The upper limit of max worker counts.
	maxWorkerCount = 1024
	// The upper limit of max txn rows.
	maxMaxTxnRow = 2048

	defaultTiDBTxnMode         = txnModeOptimistic
	defaultBatchReplaceEnabled = true
	defaultBatchReplaceSize    = 20
	defaultReadTimeout         = "2m"
	defaultWriteTimeout        = "2m"
	defaultDialTimeout         = "2m"
	defaultSafeMode            = true
	defaultTxnIsolationRC      = "READ-COMMITTED"
	defaultCharacterSet        = "utf8mb4"
)

func defaultParams() *sinkParams {
	return &sinkParams{
		workerCount:         defaultWorkerCount,
		maxTxnRow:           defaultMaxTxnRow,
		tidbTxnMode:         defaultTiDBTxnMode,
		batchReplaceEnabled: defaultBatchReplaceEnabled,
		batchReplaceSize:    defaultBatchReplaceSize,
		readTimeout:         defaultReadTimeout,
		writeTimeout:        defaultWriteTimeout,
		dialTimeout:         defaultDialTimeout,
		safeMode:            defaultSafeMode,
	}
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
	batchReplaceEnabled bool
	batchReplaceSize    int
	readTimeout         string
	writeTimeout        string
	dialTimeout         string
	safeMode            bool
	timezone            string
	tls                 string
}

func (s *sinkParams) Clone() *sinkParams {
	clone := *s
	return &clone
}

func parseSinkURIToParams(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL) (*sinkParams, error) {
	if sinkURI == nil {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("fail to open MySQL sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !sink.IsMySQLCompatibleScheme(scheme) {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("can't create MySQL sink with unsupported scheme: %s", scheme)
	}

	var err error
	params := defaultParams()
	query := sinkURI.Query()

	if err = getWorkerCount(query, &params.workerCount); err != nil {
		return nil, err
	}
	if err = getMaxTxnRow(query, &params.maxTxnRow); err != nil {
		return nil, err
	}
	if err = getTiDBTxnMode(query, &params.tidbTxnMode); err != nil {
		return nil, err
	}
	if err = getSSLCA(query, changefeedID, &params.tls); err != nil {
		return nil, err
	}
	err = getBatchReplaceEnable(query, &params.batchReplaceEnabled, &params.batchReplaceSize)
	if err != nil {
		return nil, err
	}
	if err = getSafeMode(query, &params.safeMode); err != nil {
		return nil, err
	}
	if err = getTimezone(ctx, query, &params.timezone); err != nil {
		return nil, err
	}
	if err = getDuration(query, "read-timeout", &params.readTimeout); err != nil {
		return nil, err
	}
	if err = getDuration(query, "write-timeout", &params.writeTimeout); err != nil {
		return nil, err
	}
	if err = getDuration(query, "timeout", &params.dialTimeout); err != nil {
		return nil, err
	}
	return params, nil
}

func getWorkerCount(values url.Values, workerCount *int) error {
	s := values.Get("worker-count")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if c <= 0 {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid worker-count %d, which must be greater than 0", c))
	}
	if c > maxWorkerCount {
		log.Warn("worker-count too large",
			zap.Int("original", c), zap.Int("override", maxWorkerCount))
		c = maxWorkerCount
	}

	*workerCount = c
	return nil
}

func getMaxTxnRow(values url.Values, maxTxnRow *int) error {
	s := values.Get("max-txn-row")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if c <= 0 {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-txn-row %d, which must be greater than 0", c))
	}
	if c > maxMaxTxnRow {
		log.Warn("max-txn-row too large",
			zap.Int("original", c), zap.Int("override", maxMaxTxnRow))
		c = maxMaxTxnRow
	}
	*maxTxnRow = c
	return nil
}

func getTiDBTxnMode(values url.Values, mode *string) error {
	s := values.Get("tidb-txn-mode")
	if len(s) == 0 {
		return nil
	}
	if s == txnModeOptimistic || s == txnModePessimistic {
		*mode = s
	} else {
		log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic",
			zap.String("default", defaultTiDBTxnMode))
	}
	return nil
}

func getSSLCA(values url.Values, changefeedID model.ChangeFeedID, tls *string) error {
	s := values.Get("ssl-ca")
	if len(s) == 0 {
		return nil
	}

	credential := security.Credential{
		CAPath:   values.Get("ssl-ca"),
		CertPath: values.Get("ssl-cert"),
		KeyPath:  values.Get("ssl-key"),
	}
	tlsCfg, err := credential.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}

	name := "cdc_mysql_tls" + changefeedID.Namespace + "_" + changefeedID.ID
	err = dmysql.RegisterTLSConfig(name, tlsCfg)
	if err != nil {
		return cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	*tls = "?tls=" + name
	return nil
}

func getBatchReplaceEnable(values url.Values, batchReplaceEnabled *bool, batchReplaceSize *int) error {
	s := values.Get("batch-replace-enable")
	if len(s) > 0 {
		enable, err := strconv.ParseBool(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		*batchReplaceEnabled = enable
	}

	if !*batchReplaceEnabled {
		return nil
	}

	s = values.Get("batch-replace-size")
	if len(s) == 0 {
		return nil
	}
	size, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*batchReplaceSize = size
	return nil
}

func getSafeMode(values url.Values, safeMode *bool) error {
	s := values.Get("safe-mode")
	if len(s) == 0 {
		return nil
	}
	enabled, err := strconv.ParseBool(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*safeMode = enabled
	return nil
}

func getTimezone(ctx context.Context, values url.Values, timezone *string) error {
	if _, ok := values["time-zone"]; !ok {
		tz := contextutil.TimezoneFromCtx(ctx)
		*timezone = fmt.Sprintf(`"%s"`, tz.String())
		return nil
	}

	s := values.Get("time-zone")
	if len(s) == 0 {
		*timezone = ""
		return nil
	}

	value, err := url.QueryUnescape(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	_, err = time.LoadLocation(value)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*timezone = fmt.Sprintf(`"%s"`, s)
	return nil
}

func getDuration(values url.Values, key string, target *string) error {
	s := values.Get(key)
	if len(s) == 0 {
		return nil
	}
	_, err := time.ParseDuration(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*target = s
	return nil
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

	// Since we don't need select, just set default isolation level to read-committed
	// transaction_isolation is mysql newly introduced variable and will vary from MySQL5.7/MySQL8.0/Mariadb
	isolation, err := checkTiDBVariable(ctx, testDB, "transaction_isolation", defaultTxnIsolationRC)
	if err != nil {
		return "", err
	}
	if isolation != "" {
		dsnCfg.Params["transaction_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	} else {
		dsnCfg.Params["tx_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	}

	// equals to executing "SET NAMES utf8mb4"
	dsnCfg.Params["charset"] = defaultCharacterSet

	tidbPlacementMode, err := checkTiDBVariable(ctx, testDB, "tidb_placement_mode", "ignore")
	if err != nil {
		return "", err
	}
	if tidbPlacementMode != "" {
		dsnCfg.Params["tidb_placement_mode"] = fmt.Sprintf(`"%s"`, tidbPlacementMode)
	}
	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("dsn", dsnClone.FormatDSN()))

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

// SinkOptions includes some options for transaction backends.
type SinkOptions struct {
	forceReplicate bool
	enableOldValue bool
}

// SinkOptionsFromReplicaConfig creates a SinkOptions from a ReplicaConfig.
func SinkOptionsFromReplicaConfig(config *config.ReplicaConfig) SinkOptions {
	return SinkOptions{
		forceReplicate: config.ForceReplicate,
		enableOldValue: config.EnableOldValue,
	}
}

// SinkOptionsDefault creates a default SinkOptions.
func SinkOptionsDefault() SinkOptions {
	return SinkOptionsFromReplicaConfig(config.GetDefaultReplicaConfig())
}
