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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin/binding"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/imdario/mergo"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	txnModeOptimistic  = "optimistic"
	txnModePessimistic = "pessimistic"

	// DefaultWorkerCount is the default number of workers.
	DefaultWorkerCount = 16
	// DefaultMaxTxnRow is the default max number of rows in a transaction.
	DefaultMaxTxnRow = 256
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

	defaultTiDBTxnMode  = txnModeOptimistic
	defaultReadTimeout  = "2m"
	defaultWriteTimeout = "2m"
	defaultDialTimeout  = "2m"
	// Note(dongmen): defaultSafeMode is set to false since v6.4.0.
	defaultSafeMode       = false
	defaultTxnIsolationRC = "READ-COMMITTED"
	defaultCharacterSet   = "utf8mb4"

	// BackoffBaseDelay indicates the base delay time for retrying.
	BackoffBaseDelay = 500 * time.Millisecond
	// BackoffMaxDelay indicates the max delay time for retrying.
	BackoffMaxDelay = 60 * time.Second

	defaultBatchDMLEnable  = true
	defaultMultiStmtEnable = true

	// defaultcachePrepStmts is the default value of cachePrepStmts
	defaultCachePrepStmts = true
)

type urlConfig struct {
	WorkerCount                  *int    `form:"worker-count"`
	MaxTxnRow                    *int    `form:"max-txn-row"`
	MaxMultiUpdateRowSize        *int    `form:"max-multi-update-row-size"`
	MaxMultiUpdateRowCount       *int    `form:"max-multi-update-row"`
	TiDBTxnMode                  *string `form:"tidb-txn-mode"`
	SSLCa                        *string `form:"ssl-ca"`
	SSLCert                      *string `form:"ssl-cert"`
	SSLKey                       *string `form:"ssl-key"`
	SafeMode                     *bool   `form:"safe-mode"`
	TimeZone                     *string `form:"time-zone"`
	WriteTimeout                 *string `form:"write-timeout"`
	ReadTimeout                  *string `form:"read-timeout"`
	Timeout                      *string `form:"timeout"`
	EnableBatchDML               *bool   `form:"batch-dml-enable"`
	EnableMultiStatement         *bool   `form:"multi-stmt-enable"`
	EnableCachePreparedStatement *bool   `form:"cache-prep-stmts"`
}

// Config is the configs for MySQL backend.
type Config struct {
	WorkerCount            int
	MaxTxnRow              int
	MaxMultiUpdateRowCount int
	MaxMultiUpdateRowSize  int
	tidbTxnMode            string
	ReadTimeout            string
	WriteTimeout           string
	DialTimeout            string
	SafeMode               bool
	Timezone               string
	TLS                    string
	ForceReplicate         bool

	IsTiDB bool // IsTiDB is true if the downstream is TiDB
	// IsBDRModeSupported is true if the downstream is TiDB and write source is existed.
	// write source exists when the downstream is TiDB and version is greater than or equal to v6.5.0.
	IsWriteSourceExisted bool

	SourceID        uint64
	BatchDMLEnable  bool
	MultiStmtEnable bool
	CachePrepStmts  bool
}

// NewConfig returns the default mysql backend config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:            DefaultWorkerCount,
		MaxTxnRow:              DefaultMaxTxnRow,
		MaxMultiUpdateRowCount: defaultMaxMultiUpdateRowCount,
		MaxMultiUpdateRowSize:  defaultMaxMultiUpdateRowSize,
		tidbTxnMode:            defaultTiDBTxnMode,
		ReadTimeout:            defaultReadTimeout,
		WriteTimeout:           defaultWriteTimeout,
		DialTimeout:            defaultDialTimeout,
		SafeMode:               defaultSafeMode,
		BatchDMLEnable:         defaultBatchDMLEnable,
		MultiStmtEnable:        defaultMultiStmtEnable,
		CachePrepStmts:         defaultCachePrepStmts,
		SourceID:               config.DefaultTiDBSourceID,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	serverTimezone string,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrMySQLInvalidConfig.GenWithStack("fail to open MySQL sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !sink.IsMySQLCompatibleScheme(scheme) {
		return cerror.ErrMySQLInvalidConfig.GenWithStack("can't create MySQL sink with unsupported scheme: %s", scheme)
	}
	req := &http.Request{URL: sinkURI}
	urlParameter := &urlConfig{}
	if err := binding.Query.Bind(req, urlParameter); err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if urlParameter, err = mergeConfig(replicaConfig, urlParameter); err != nil {
		return err
	}
	if err = getWorkerCount(urlParameter, &c.WorkerCount); err != nil {
		return err
	}
	if err = getMaxTxnRow(urlParameter, &c.MaxTxnRow); err != nil {
		return err
	}
	if err = getMaxMultiUpdateRowCount(urlParameter, &c.MaxMultiUpdateRowCount); err != nil {
		return err
	}
	if err = getMaxMultiUpdateRowSize(urlParameter, &c.MaxMultiUpdateRowSize); err != nil {
		return err
	}
	getTiDBTxnMode(urlParameter, &c.tidbTxnMode)
	if err = getSSLCA(urlParameter, changefeedID, &c.TLS); err != nil {
		return err
	}
	getSafeMode(urlParameter, &c.SafeMode)
	if err = getTimezone(serverTimezone, urlParameter, &c.Timezone); err != nil {
		return err
	}
	if err = getDuration(urlParameter.ReadTimeout, &c.ReadTimeout); err != nil {
		return err
	}
	if err = getDuration(urlParameter.WriteTimeout, &c.WriteTimeout); err != nil {
		return err
	}
	if err = getDuration(urlParameter.Timeout, &c.DialTimeout); err != nil {
		return err
	}
	getBatchDMLEnable(urlParameter, &c.BatchDMLEnable)
	getMultiStmtEnable(urlParameter, &c.MultiStmtEnable)
	getCachePrepStmts(urlParameter, &c.CachePrepStmts)
	c.ForceReplicate = replicaConfig.ForceReplicate

	// Note(dongmen): The TiDBSourceID should never be 0 here, but we have found that
	// in some problematic cases, the TiDBSourceID is 0 since something went wrong in the
	// configuration process. So we need to check it here again.
	// We do this is because it can cause the data to be inconsistent if the TiDBSourceID is 0
	// in BDR Mode cluster.
	if replicaConfig.Sink.TiDBSourceID == 0 {
		log.Error("The TiDB source ID should never be set to 0. Please report it as a bug. The default value will be used: 1.",
			zap.Uint64("tidbSourceID", replicaConfig.Sink.TiDBSourceID))
		c.SourceID = config.DefaultTiDBSourceID
	} else {
		c.SourceID = replicaConfig.Sink.TiDBSourceID
		log.Info("TiDB source ID is set", zap.Uint64("sourceID", c.SourceID))
	}

	return nil
}

func mergeConfig(
	replicaConfig *config.ReplicaConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	dest.SafeMode = replicaConfig.Sink.SafeMode
	if replicaConfig.Sink != nil && replicaConfig.Sink.MySQLConfig != nil {
		mConfig := replicaConfig.Sink.MySQLConfig
		dest.WorkerCount = mConfig.WorkerCount
		dest.MaxTxnRow = mConfig.MaxTxnRow
		dest.MaxMultiUpdateRowCount = mConfig.MaxMultiUpdateRowCount
		dest.MaxMultiUpdateRowSize = mConfig.MaxMultiUpdateRowSize
		dest.TiDBTxnMode = mConfig.TiDBTxnMode
		dest.SSLCa = mConfig.SSLCa
		dest.SSLCert = mConfig.SSLCert
		dest.SSLKey = mConfig.SSLKey
		dest.TimeZone = mConfig.TimeZone
		dest.WriteTimeout = mConfig.WriteTimeout
		dest.ReadTimeout = mConfig.ReadTimeout
		dest.Timeout = mConfig.Timeout
		dest.EnableBatchDML = mConfig.EnableBatchDML
		dest.EnableMultiStatement = mConfig.EnableMultiStatement
		dest.EnableCachePreparedStatement = mConfig.EnableCachePreparedStatement
	}
	if err := mergo.Merge(dest, urlParameters, mergo.WithOverride); err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	return dest, nil
}

func getWorkerCount(values *urlConfig, workerCount *int) error {
	if values.WorkerCount == nil {
		return nil
	}
	c := *values.WorkerCount
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

func getMaxTxnRow(config *urlConfig, maxTxnRow *int) error {
	if config.MaxTxnRow == nil {
		return nil
	}

	c := *config.MaxTxnRow
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

func getMaxMultiUpdateRowCount(values *urlConfig, maxMultiUpdateRow *int) error {
	if values.MaxMultiUpdateRowCount == nil {
		return nil
	}

	c := *values.MaxMultiUpdateRowCount
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

func getMaxMultiUpdateRowSize(values *urlConfig, maxMultiUpdateRowSize *int) error {
	if values.MaxMultiUpdateRowSize == nil {
		return nil
	}

	c := *values.MaxMultiUpdateRowSize
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

func getTiDBTxnMode(values *urlConfig, mode *string) {
	if values.TiDBTxnMode == nil || len(*values.TiDBTxnMode) == 0 {
		return
	}
	s := strings.ToLower(*values.TiDBTxnMode)
	if s == txnModeOptimistic || s == txnModePessimistic {
		*mode = s
	} else {
		log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic",
			zap.String("default", defaultTiDBTxnMode))
	}
}

func getSSLCA(values *urlConfig, changefeedID model.ChangeFeedID, tls *string) error {
	if values.SSLCa == nil || len(*values.SSLCa) == 0 {
		return nil
	}

	var (
		sslCert string
		sslKey  string
	)
	if values.SSLCert != nil {
		sslCert = *values.SSLCert
	}
	if values.SSLKey != nil {
		sslKey = *values.SSLKey
	}
	credential := security.Credential{
		CAPath:   *values.SSLCa,
		CertPath: sslCert,
		KeyPath:  sslKey,
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

func getSafeMode(values *urlConfig, safeMode *bool) {
	if values.SafeMode != nil {
		*safeMode = *values.SafeMode
	}
}

func getTimezone(serverTimezoneStr string,
	values *urlConfig, timezone *string,
) error {
	const pleaseSpecifyTimezone = "We recommend that you specify the time-zone explicitly. " +
		"Please make sure that the timezone of the TiCDC server, " +
		"sink-uri and the downstream database are consistent. " +
		"If the downstream database does not load the timezone information, " +
		"you can refer to https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html."
	serverTimezone, err := util.GetTimezone(serverTimezoneStr)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if values.TimeZone == nil {
		// If time-zone is not specified, use the timezone of the server.
		log.Warn("Because time-zone is not specified, "+
			"the timezone of the TiCDC server will be used. "+
			pleaseSpecifyTimezone,
			zap.String("timezone", serverTimezone.String()))
		*timezone = fmt.Sprintf(`"%s"`, serverTimezone.String())
		return nil
	}

	s := *values.TimeZone
	if len(s) == 0 {
		*timezone = ""
		log.Warn("Because time-zone is empty, " +
			"the timezone of the downstream database will be used. " +
			pleaseSpecifyTimezone)
		return nil
	}

	changefeedTimezone, err := util.GetTimezone(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*timezone = fmt.Sprintf(`"%s"`, changefeedTimezone.String())
	// We need to check whether the timezone of the TiCDC server and the sink-uri are consistent.
	// If they are inconsistent, it may cause the data to be inconsistent.
	if changefeedTimezone.String() != serverTimezone.String() {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, errors.Errorf(
			"the timezone of the TiCDC server and the sink-uri are inconsistent. "+
				"TiCDC server timezone: %s, sink-uri timezone: %s. "+
				"Please make sure that the timezone of the TiCDC server, "+
				"sink-uri and the downstream database are consistent.",
			serverTimezone.String(), changefeedTimezone.String()))
	}

	return nil
}

func getDuration(s *string, target *string) error {
	if s == nil {
		return nil
	}
	_, err := time.ParseDuration(*s)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	*target = *s
	return nil
}

func getBatchDMLEnable(values *urlConfig, batchDMLEnable *bool) {
	if values.EnableBatchDML != nil {
		*batchDMLEnable = *values.EnableBatchDML
	}
}

func getMultiStmtEnable(values *urlConfig, multiStmtEnable *bool) {
	if values.EnableMultiStatement != nil {
		*multiStmtEnable = *values.EnableMultiStatement
	}
}

func getCachePrepStmts(values *urlConfig, cachePrepStmts *bool) {
	if values.EnableCachePreparedStatement != nil {
		*cachePrepStmts = *values.EnableCachePreparedStatement
	}
}
