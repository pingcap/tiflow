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

package replication

import (
	"context"
	"sync"

	"github.com/pingcap/log"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
)

type ddlHandler interface {
	Run(ctx context.Context) error
	PullDDL() (uint64, []*timodel.Job, error)
}

type ddlHandlerImpl struct {
	puller puller.Puller

	mu         sync.Mutex
	resolvedTS uint64
	lastDDLTs  uint64 // TODO remove
	ddlJobs    []*timodel.Job
}

// TODO test-case: resolvedTs is initialized to (startTs - 1)
func newDDLHandler(ctx context.Context, pdCli pd.Client, credential *security.Credential, kvStorage tidbkv.Storage, startTs uint64) *ddlHandlerImpl {
	plr := puller.NewPuller(
		ctx,
		pdCli,
		credential,
		kvStorage,
		startTs,
		[]regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()},
		nil,
		false)

	return &ddlHandlerImpl{
		puller:     plr,
		resolvedTS: startTs - 1,
	}
}

func (h *ddlHandlerImpl) Run(ctx context.Context) error {
	log.Debug("DDL puller started")
	ctx = util.PutTableInfoInCtx(ctx, -1, "")
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return h.puller.Run(ctx)
	})

	rawDDLCh := puller.SortOutput(ctx, h.puller.Output())

	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e := <-rawDDLCh:
				if e == nil {
					continue
				}
				err := h.receiveDDL(e)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	return errg.Wait()
}

func (h *ddlHandlerImpl) receiveDDL(rawDDL *model.RawKVEntry) error {
	if rawDDL.OpType == model.OpTypeResolved {
		h.mu.Lock()
		h.resolvedTS = rawDDL.CRTs
		h.mu.Unlock()
		return nil
	}
	job, err := entry.UnmarshalDDL(rawDDL)
	if err != nil {
		return errors.Trace(err)
	}
	if job == nil {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if job.BinlogInfo.FinishedTS == h.lastDDLTs {
		return nil
	}
	h.lastDDLTs = job.BinlogInfo.FinishedTS
	h.ddlJobs = append(h.ddlJobs, job)
	return nil
}

// TODO test-case: resolvedTs not zero
func (h *ddlHandlerImpl) PullDDL() (uint64, []*timodel.Job, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := h.ddlJobs
	h.ddlJobs = nil
	return h.resolvedTS, result, nil
}
