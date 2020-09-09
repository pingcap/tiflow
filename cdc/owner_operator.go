// Copyright 2020 PingCAP, Inc.
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
	"sync"

	tidbkv "github.com/pingcap/tidb/kv"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
)

//TODO: add tests
type ddlHandler struct {
	puller     puller.Puller
	resolvedTS uint64
	ddlJobs    []*timodel.Job

	mu     sync.Mutex
	wg     *errgroup.Group
	cancel func()
}

func newDDLHandler(pdCli pd.Client, credential *security.Credential, kvStorage tidbkv.Storage, checkpointTS uint64) *ddlHandler {
	plr := puller.NewPuller(pdCli, credential, kvStorage, checkpointTS, []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}, nil, false)
	ctx, cancel := context.WithCancel(context.Background())
	h := &ddlHandler{
		puller: plr,
		cancel: cancel,
	}
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)
	ctx = util.PutTableInfoInCtx(ctx, -1, "")

	// FIXME: user of ddlHandler can't know error happen.
	errg.Go(func() error {
		return plr.Run(ctx)
	})

	rawDDLCh := puller.SortOutput(ctx, plr.Output())

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
	h.wg = errg
	return h
}

func (h *ddlHandler) receiveDDL(rawDDL *model.RawKVEntry) error {
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
	h.ddlJobs = append(h.ddlJobs, job)
	return nil
}

var _ OwnerDDLHandler = &ddlHandler{}

// PullDDL implements `roles.OwnerDDLHandler` interface.
func (h *ddlHandler) PullDDL() (uint64, []*timodel.Job, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := h.ddlJobs
	h.ddlJobs = nil
	return h.resolvedTS, result, nil
}

func (h *ddlHandler) Close() error {
	h.cancel()
	err := h.wg.Wait()
	return errors.Trace(err)
}
