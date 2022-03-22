//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package verification

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Verifier interface {
	// Verify run the e2e consistency check, return false and the (startTs, endTs] time range of broken data, if check fail
	// return true means no need to run next step, endTs is returned for GC
	Verify(ctx context.Context) (bool, string, string, error)
	// Close stop the verify process
	Close() error
}

type Config struct {
	CheckInterval      time.Duration
	ResourceLimitation string
	UpstreamDSN        string
	DownStreamDSN      string
	// TODO: how about the IgnoreTxnStartTs and DDLAllowlist, skip the tables involved, send as params?
	Filter       *filter.Filter
	DataBaseName string
	TableName    string
	ChangefeedID string
}

type TiDBVerification struct {
	etcdClient        etcd.EtcdClient
	config            *Config
	upstreamChecker   *checker
	downstreamChecker *checker
	running           atomic.Bool
}

const (
	defaultCheckInterval = 60 * time.Second
)

const (
	unchecked = iota
	checkPass
	checkFail
)

// NewVerification start the verification process if no error
func NewVerification(ctx cdcContext.Context, config *Config) error {
	if config == nil {
		return cerror.WrapError(cerror.ErrVerificationConfigInvalid, errors.New("Config can not be nil"))
	}

	upstreamDB, err := openDB(ctx, config.UpstreamDSN)
	if err != nil {
		return err
	}
	downstreamDB, err := openDB(ctx, config.DownStreamDSN)
	if err != nil {
		return err
	}

	if config.CheckInterval == 0 {
		config.CheckInterval = defaultCheckInterval
	}
	v := &TiDBVerification{
		config:            config,
		upstreamChecker:   newChecker(upstreamDB),
		downstreamChecker: newChecker(downstreamDB),
		etcdClient:        ctx.GlobalVars().EtcdClient.Client,
	}
	go v.runVerify(ctx)

	return nil
}

func (v *TiDBVerification) runVerify(ctx context.Context) {
	// in case run verify at the same if have multiple changefeed created
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(v.config.CheckInterval + time.Duration(r.Int63n(int64(v.config.CheckInterval/4))))
	log.Info("runVerify interval", zap.Duration("interval", v.config.CheckInterval+time.Duration(r.Int63n(int64(v.config.CheckInterval/4)))))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("runVerify ctx cancel", zap.String("changefeed", v.config.ChangefeedID), zap.Error(ctx.Err()))
			if err := v.Close(); err != nil {
				log.Error("runVerify Close fail", zap.String("changefeed", v.config.ChangefeedID), zap.Error(err))
			}

			return
		case <-ticker.C:
			// TODO:
			// resource limitation cancel https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-checksum.html
			enoughCheck, startTs, endTs, err := v.Verify(ctx)
			if err != nil {
				log.Warn("run e2e verify error",
					zap.String("changefeed", v.config.ChangefeedID),
					zap.String("startTs", startTs),
					zap.String("endTs", endTs),
					zap.Error(err))

				continue
			}
			log.Info("e2e verify ret",
				zap.String("changefeed", v.config.ChangefeedID),
				zap.String("startTs", startTs),
				zap.String("endTs", endTs),
				zap.Bool("enoughCheck", enoughCheck))

			if !enoughCheck {
				if err = v.putVerificationTask(ctx, &taskInfo{StartTs: startTs, EndTs: endTs}); err != nil {
					log.Error("putVerificationTask fail",
						zap.String("changefeed", v.config.ChangefeedID),
						zap.String("startTs", startTs),
						zap.String("endTs", endTs),
						zap.Error(err))
					continue
				}

				log.Info("putVerificationTask",
					zap.String("changefeed", v.config.ChangefeedID),
					zap.String("startTs", startTs),
					zap.String("endTs", endTs))
			}
		}
	}
}

type taskInfo struct {
	StartTs string `json:"startTs"`
	EndTs   string `json:"endTs"`
}

// Marshal using json.Marshal.
func (c *taskInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *taskInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return cerror.WrapError(cerror.ErrUnmarshalFailed, errors.Annotatef(err, "unmarshal data: %v", data))
}

func (v *TiDBVerification) putVerificationTask(ctx context.Context, task *taskInfo) error {
	key := etcd.GetEtcdKeyVerification(v.config.ChangefeedID)
	value, err := task.Marshal()
	if err != nil {
		return err
	}

	_, err = v.etcdClient.Put(ctx, key, string(value))
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// Verify implement Verify api,
// if no error, return false and (startTs, endTs] for next step,
// return true means no need to run next step, endTs is returned for GC
func (v *TiDBVerification) Verify(ctx context.Context) (bool, string, string, error) {
	if v.running.Load() {
		log.Info("e2e Verify running")
		return true, "", "", nil
	}

	v.running.Store(true)
	defer v.running.Store(false)

	ts, err := v.getTS(ctx)
	if err != nil {
		return false, "", "", err
	}
	if ts.result != unchecked {
		return true, "", "", nil
	}

	startTs, endTs := "", ""
	result := false
	for ts.result == unchecked {
		ret, err := v.checkConsistency(ctx, ts)
		if err != nil {
			return false, "", "", err
		}

		checkRet := checkPass
		if !ret {
			checkRet = checkFail
		}
		err = v.updateCheckResult(ctx, ts, checkRet)
		if err != nil {
			return false, "", "", err
		}

		// if pass set result true, sent out endTs for GC
		// if run from previous set startTs for next step
		if checkRet == checkPass {
			if endTs != "" {
				startTs = ts.primaryTs
			} else {
				endTs = ts.primaryTs
				result = true
			}
			break
		}

		preTs, err := v.getPreviousTS(ctx, ts.cf, ts.primaryTs)
		if err != nil {
			if sql.ErrNoRows == errors.Cause(err) {
				endTs = ts.primaryTs
				break
			}
			return false, "", "", err
		}

		// if previous check pass run module check.
		// if fail means already run module check last time, skip by return endTs for GC.
		// if unchecked, run e2e check against previous.
		if preTs.result == checkPass {
			startTs = preTs.primaryTs
		}
		if preTs.result == checkFail {
			startTs = ""
			result = true
		}
		endTs = ts.primaryTs
		ts = preTs
	}

	return result, startTs, endTs, nil
}

func (v *TiDBVerification) checkConsistency(ctx context.Context, t tsPair) (bool, error) {
	err := setSnapshot(ctx, v.upstreamChecker.db, t.primaryTs)
	if err != nil {
		return false, err
	}
	err = setSnapshot(ctx, v.downstreamChecker.db, t.secondaryTs)
	if err != nil {
		return false, err
	}
	log.Info("check consistency between upstream and downstream db",
		zap.String("primaryTs", t.primaryTs),
		zap.String("secondaryTs", t.secondaryTs),
		zap.String("changefeed", t.cf))
	return compareCheckSum(ctx, v.upstreamChecker, v.downstreamChecker, v.config.Filter)
}

func (v *TiDBVerification) updateCheckResult(ctx context.Context, t tsPair, checkRet int) error {
	err := setSnapshot(ctx, v.downstreamChecker.db, "0")
	if err != nil {
		return err
	}

	tx, err := v.downstreamChecker.db.BeginTx(ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	query := fmt.Sprintf("update %s.%s set result=? where primary_ts=? and secondary_ts=? and cf=?", v.config.DataBaseName, v.config.TableName)
	_, err = tx.ExecContext(ctx, query, checkRet, t.primaryTs, t.secondaryTs, t.cf)
	if err != nil {
		errR := tx.Rollback()
		if errR != nil {
			log.Error("failed to rollback update syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, errR)))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}
	return cerror.WrapError(cerror.ErrMySQLTxnError, tx.Commit())
}

var openDB = func(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLConnectionError, err)
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLConnectionError, err)
	}
	return db, nil
}

func setSnapshot(ctx context.Context, db *sql.DB, ts string) error {
	query := fmt.Sprintf(`set @@tidb_snapshot=%s`, ts)
	_, err := db.ExecContext(ctx, query)
	return cerror.WrapError(cerror.ErrMySQLTxnError, err)
}

type tsPair struct {
	cf          string
	primaryTs   string
	secondaryTs string
	result      int
}

func (v *TiDBVerification) getPreviousTS(ctx context.Context, cf, pts string) (tsPair, error) {
	var t tsPair
	query := fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts<? order by primary_ts desc limit 1", v.config.DataBaseName, v.config.TableName)
	p, err := strconv.Atoi(pts)
	if err != nil {
		return t, err
	}
	row := v.downstreamChecker.db.QueryRowContext(ctx, query, cf, p)
	if row.Err() != nil {
		return t, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}

	err = row.Scan(&t.cf, &t.primaryTs, &t.secondaryTs, &t.result)
	return t, cerror.WrapError(cerror.ErrMySQLQueryError, err)
}

func (v *TiDBVerification) getTS(ctx context.Context) (tsPair, error) {
	var ts tsPair
	query := fmt.Sprintf("select max(primary_ts) as primary_ts from %s.%s where cf=?", v.config.DataBaseName, v.config.TableName)
	row := v.downstreamChecker.db.QueryRowContext(ctx, query, v.config.ChangefeedID)
	if row.Err() != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}
	if err := row.Scan(&ts.primaryTs); err != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}

	query = fmt.Sprintf("select cf, primary_ts, secondary_ts, result from %s.%s where cf=? and primary_ts=?", v.config.DataBaseName, v.config.TableName)
	row = v.downstreamChecker.db.QueryRowContext(ctx, query, v.config.ChangefeedID, ts.primaryTs)
	if row.Err() != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, row.Err())
	}
	if err := row.Scan(&ts.cf, &ts.primaryTs, &ts.secondaryTs, &ts.result); err != nil {
		return ts, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}

	return ts, nil
}

func (v *TiDBVerification) Close() error {
	err := multierr.Append(v.upstreamChecker.db.Close(), v.downstreamChecker.db.Close())
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}
