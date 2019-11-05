// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"database/sql"
	"sync"

	"github.com/cenkalti/backoff"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type ddlHandler struct {
	puller       *Puller
	mounter      *txn.Mounter
	checkpointTS uint64
	resolvedTS   uint64
	ddlJobs      []*txn.DDL

	l      sync.Mutex
	g      *errgroup.Group
	cancel func()
}

func NewDDLHandler(pdCli pd.Client) *ddlHandler {
	puller := NewPuller(pdCli, 0, []util.Span{util.GetDDLSpan()})
	ctx, cancel := context.WithCancel(context.Background())
	h := &ddlHandler{
		puller: puller,
		cancel: cancel,
	}
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		err := puller.CollectRawTxns(ctx, h.receiveDDL)
		if err != nil {
			return errors.Annotate(err, "ddl puller")
		}
		return nil
	})
	h.g = errg
	return h
}

func (h *ddlHandler) receiveDDL(ctx context.Context, rawTxn txn.RawTxn) error {
	h.l.Lock()
	defer h.l.Unlock()
	if len(rawTxn.Entries) == 0 {
		h.resolvedTS = rawTxn.TS
		return nil
	}
	t, err := h.mounter.Mount(rawTxn)
	if err != nil {
		return errors.Trace(err)
	}
	if !t.IsDDL() {
		return nil
	}
	h.ddlJobs = append(h.ddlJobs, t.DDL)
	return nil
}

func (h *ddlHandler) PullDDL() (uint64, []*txn.DDL, error) {
	h.l.Lock()
	defer h.l.Unlock()
	h.checkpointTS = h.resolvedTS
	result := h.ddlJobs
	h.ddlJobs = nil
	return h.resolvedTS, result, nil
}

func (h *ddlHandler) ExecDDL(ctx context.Context, sinkURI string, ddl *txn.DDL) error {
	// TODO cache the database connection
	// TODO handle other target database, kile kafka, file
	db, err := sql.Open("mysql", sinkURI)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()
	err = h.execDDLWithMaxRetries(ctx, db, ddl, 5)
	return errors.Trace(err)
}

func (h *ddlHandler) execDDLWithMaxRetries(ctx context.Context, db *sql.DB, ddl *txn.DDL, maxRetries uint64) error {
	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		maxRetries,
	)
	return backoff.Retry(func() error {
		err := h.execDDL(ctx, db, ddl)
		if isIgnorableDDLError(err) {
			return nil
		}
		if err == context.Canceled || err == context.DeadlineExceeded {
			err = backoff.Permanent(err)
		}
		return err
	}, retryCfg)
}

func (h *ddlHandler) execDDL(ctx context.Context, db *sql.DB, ddl *txn.DDL) error {
	shouldSwitchDB := len(ddl.Database) > 0 && ddl.Job.Type != model.ActionCreateSchema

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+util.QuoteName(ddl.Database)+";")
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.Job.Query); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Job.Query))
	return nil
}

func getSQLErrCode(err error) (terror.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return terror.ErrCode(mysqlErr.Number), true
}

func isIgnorableDDLError(err error) bool {
	errCode, ok := getSQLErrCode(err)
	if !ok {
		return false
	}
	// we can get error code from:
	// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
	// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
	// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), ddl.ErrCantDropFieldOrKey.Code(), mysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

func (h *ddlHandler) Close() error {
	h.cancel()
	err := h.g.Wait()
	return err
}
