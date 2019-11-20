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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/puller"
	"github.com/pingcap/tidb-cdc/cdc/sink"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

//TODO: add tests
type ddlHandler struct {
	puller     puller.Puller
	mounter    *txn.Mounter
	resolvedTS uint64
	ddlJobs    []*txn.DDL

	mu     sync.Mutex
	wg     *errgroup.Group
	cancel func()
}

func NewDDLHandler(pdCli pd.Client, checkpointTS uint64) *ddlHandler {
	puller := puller.NewPuller(pdCli, checkpointTS, []util.Span{util.GetDDLSpan()})
	ctx, cancel := context.WithCancel(context.Background())
	// TODO get time loc from config
	txnMounter := txn.NewTxnMounter(nil, time.UTC)
	h := &ddlHandler{
		puller:  puller,
		cancel:  cancel,
		mounter: txnMounter,
	}
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	// FIXME: user of ddlHandler can't know error happen.
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
	h.wg = errg
	return h
}

func (h *ddlHandler) receiveDDL(ctx context.Context, rawTxn txn.RawTxn) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resolvedTS = rawTxn.Ts
	if len(rawTxn.Entries) == 0 {
		return nil
	}
	t, err := h.mounter.Mount(rawTxn)
	if err != nil {
		return errors.Trace(err)
	}
	if !t.IsDDL() {
		log.Warn("should not be DML here", zap.Reflect("txn", t))
		return nil
	}
	h.ddlJobs = append(h.ddlJobs, t.DDL)
	return nil
}

var _ OwnerDDLHandler = &ddlHandler{}

// PullDDL implements `roles.OwnerDDLHandler` interface.
func (h *ddlHandler) PullDDL() (uint64, []*txn.DDL, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := h.ddlJobs
	h.ddlJobs = nil
	return h.resolvedTS, result, nil
}

// ExecDDL implements roles.OwnerDDLHandler interface.
func (h *ddlHandler) ExecDDL(ctx context.Context, sinkURI string, ddl *txn.DDL) error {
	// TODO cache the sink
	// TODO handle other target database, kile kafka, file
	db, err := sql.Open("mysql", sinkURI)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()
	s := sink.NewMySQLSinkDDLOnly(db)

	err = s.Emit(ctx, txn.Txn{Ts: ddl.Job.BinlogInfo.FinishedTS, DDL: ddl})
	return errors.Trace(err)
}

func (h *ddlHandler) Close() error {
	h.cancel()
	err := h.wg.Wait()
	return err
}
