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
	// defaultMaxMultiUpdateRowCount is the default max number of rows in a
	// single multi update SQL.
	defaultMaxMultiUpdateRowCount = 40
	// defaultMaxMultiUpdateRowSize(1KB) defines the default value of MaxMultiUpdateRowSize
	// When row average size is larger MaxMultiUpdateRowSize,
	// disable multi update, otherwise enable multi update.
	defaultMaxMultiUpdateRowSize = 1024
	// The upper limit of max worker counts.
	maxWorkerCount = 1024
	// The upper limit of max txn rows.
	maxMaxTxnRow = 2048
	// The upper limit of max multi update rows in a single SQL.
	maxMaxMultiUpdateRowCount = 256
	// The upper limit of max multi update row size(8KB).
	maxMaxMultiUpdateRowSize = 8192

	defaultTiDBTxnMode         = txnModeOptimistic
	defaultBatchReplaceEnabled = true
	defaultBatchReplaceSize    = 20
	defaultReadTimeout         = "2m"
	defaultWriteTimeout        = "2m"
	defaultDialTimeout         = "2m"
	// Note(dongmen): defaultSafeMode is set to false since v6.4.0.
	defaultSafeMode       = false
	defaultTxnIsolationRC = "READ-COMMITTED"
	defaultCharacterSet   = "utf8mb4"

	// BackoffBaseDelay indicates the base delay time for retrying.
	BackoffBaseDelay = 500 * time.Millisecond
	// BackoffMaxDelay indicates the max delay time for retrying.
	BackoffMaxDelay = 60 * time.Second

	defaultBatchDMLEnable = true
)

// Config is the configs for MySQL backend.
type Config struct {
	WorkerCount            int
	MaxTxnRow              int
	MaxMultiUpdateRowCount int
	MaxMultiUpdateRowSize  int
	tidbTxnMode            string
	BatchReplaceEnabled    bool
	BatchReplaceSize       int
	ReadTimeout            string
	WriteTimeout           string
	DialTimeout            string
	SafeMode               bool
	Timezone               string
	TLS                    string
	ForceReplicate         bool
	EnableOldValue         bool

	IsTiDB         bool // IsTiDB is true if the downstream is TiDB
	SourceID       uint64
	BatchDMLEnable bool
}

// NewConfig returns the default mysql backend config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:            defaultWorkerCount,
		MaxTxnRow:              defaultMaxTxnRow,
		MaxMultiUpdateRowCount: defaultMaxMultiUpdateRowCount,
		MaxMultiUpdateRowSize:  defaultMaxMultiUpdateRowSize,
		tidbTxnMode:            defaultTiDBTxnMode,
		BatchReplaceEnabled:    defaultBatchReplaceEnabled,
		BatchReplaceSize:       defaultBatchReplaceSize,
		ReadTimeout:            defaultReadTimeout,
		WriteTimeout:           defaultWriteTimeout,
		DialTimeout:            defaultDialTimeout,
		SafeMode:               defaultSafeMode,
		BatchDMLEnable:         defaultBatchDMLEnable,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrMySQLConnectionError.GenWithStack("fail to open MySQL sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !sink.IsMySQLCompatibleScheme(scheme) {
		return cerror.ErrMySQLConnectionError.GenWithStack("can't create MySQL sink with unsupported scheme: %s", scheme)
	}
	query := sinkURI.Query()
	if err = getWorkerCount(query, &c.WorkerCount); err != nil {
		return err
	}
	if err = getMaxTxnRow(query, &c.MaxTxnRow); err != nil {
		return err
	}
	if err = getMaxMultiUpdateRowCount(query, &c.MaxMultiUpdateRowCount); err != nil {
		return err
	}
	if err = getMaxMultiUpdateRowSize(query, &c.MaxMultiUpdateRowSize); err != nil {
		return err
	}
	if err = getTiDBTxnMode(query, &c.tidbTxnMode); err != nil {
		return err
	}
	if err = getSSLCA(query, changefeedID, &c.TLS); err != nil {
		return err
	}
	if err = getBatchReplaceEnable(query, &c.BatchReplaceEnabled, &c.BatchReplaceSize); err != nil {
		return err
	}
	if err = getSafeMode(query, &c.SafeMode); err != nil {
		return err
	}
	if err = getTimezone(ctx, query, &c.Timezone); err != nil {
		return err
	}
	if err = getDuration(query, "read-timeout", &c.ReadTimeout); err != nil {
		return err
	}
	if err = getDuration(query, "write-timeout", &c.WriteTimeout); err != nil {
		return err
	}
	if err = getDuration(query, "timeout", &c.DialTimeout); err != nil {
		return err
	}
	if err = getBatchDMLEnable(query, &c.BatchDMLEnable); err != nil {
		return err
	}
	c.EnableOldValue = replicaConfig.EnableOldValue
	c.ForceReplicate = replicaConfig.ForceReplicate
	c.SourceID = replicaConfig.Sink.TiDBSourceID

	return nil
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

func getMaxMultiUpdateRowCount(values url.Values, maxMultiUpdateRow *int) error {
	s := values.Get("max-multi-update-row")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if c <= 0 {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-multi-update-row %d, which must be greater than 0", c))
	}
	if c > maxMaxMultiUpdateRowCount {
		log.Warn("max-multi-update-row too large",
			zap.Int("original", c), zap.Int("override", maxMaxMultiUpdateRowCount))
		c = maxMaxMultiUpdateRowCount
	}
	*maxMultiUpdateRow = c
	return nil
}

func getMaxMultiUpdateRowSize(values url.Values, maxMultiUpdateRowSize *int) error {
	s := values.Get("max-multi-update-row-size")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if c < 0 {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-multi-update-row-size %d, "+
				"which must be greater than or equal to 0", c))
	}
	if c > maxMaxMultiUpdateRowSize {
		log.Warn("max-multi-update-row-size too large",
			zap.Int("original", c), zap.Int("override", maxMaxMultiUpdateRowSize))
		c = maxMaxMultiUpdateRowSize
	}
	*maxMultiUpdateRowSize = c
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

func getBatchDMLEnable(values url.Values, batchDMLEnable *bool) error {
	s := values.Get("batch-dml-enable")
	if len(s) > 0 {
		enable, err := strconv.ParseBool(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		*batchDMLEnable = enable
	}
	return nil
}
