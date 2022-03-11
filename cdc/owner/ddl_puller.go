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

package owner

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// DDLPuller is a wrapper of the Puller interface for the owner
// DDLPuller starts a puller, listens to the DDL range, adds the received DDLs into an internal queue
type DDLPuller interface {
	// Run runs the DDLPuller
	Run(ctx cdcContext.Context) error
	// FrontDDL returns the first DDL job in the internal queue
	FrontDDL() (uint64, *timodel.Job)
	// PopFrontDDL returns and pops the first DDL job in the internal queue
	PopFrontDDL() (uint64, *timodel.Job)
	// Close closes the DDLPuller
	Close()
}

type ddlPullerImpl struct {
	puller puller.Puller
	filter *filter.Filter

	mu             sync.Mutex
	resolvedTS     uint64
	pendingDDLJobs []*timodel.Job
	lastDDLJobID   int64
	cancel         context.CancelFunc
}

func newDDLPuller(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
	pdCli := ctx.GlobalVars().PDClient
	f, err := filter.NewFilter(ctx.ChangefeedVars().Info.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var plr puller.Puller
	kvStorage := ctx.GlobalVars().KVStorage
	// kvStorage can be nil only in the test
	if kvStorage != nil {
		plr = puller.NewPuller(ctx, pdCli, ctx.GlobalVars().GrpcPool, kvStorage, startTs,
			[]regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}, false)
	}

	return &ddlPullerImpl{
		puller:     plr,
		resolvedTS: startTs,
		filter:     f,
		cancel:     func() {},
	}, nil
}

func (h *ddlPullerImpl) Run(ctx cdcContext.Context) error {
	ctx, cancel := cdcContext.WithCancel(ctx)
	h.cancel = cancel
	log.Debug("DDL puller started", zap.String("changefeed-id", ctx.ChangefeedVars().ID))
	stdCtx := util.PutTableInfoInCtx(ctx, -1, puller.DDLPullerTableName)
	stdCtx = util.PutChangefeedIDInCtx(stdCtx, ctx.ChangefeedVars().ID)
<<<<<<< HEAD
	errg, stdCtx := errgroup.WithContext(stdCtx)
=======
	stdCtx = util.PutRoleInCtx(stdCtx, util.RoleProcessor)
	g, stdCtx := errgroup.WithContext(stdCtx)
	lastResolvedTsAdvancedTime := h.clock.Now()
>>>>>>> 1e8f99f5e (cdc/owner: add some logs to help debug puller / kvclient / lock resolver (#4822))

	g.Go(func() error {
		return h.puller.Run(stdCtx)
	})

	rawDDLCh := puller.SortOutput(stdCtx, h.puller.Output())

	receiveDDL := func(rawDDL *model.RawKVEntry) error {
		if rawDDL == nil {
			return nil
		}
		if rawDDL.OpType == model.OpTypeResolved {
			h.mu.Lock()
			defer h.mu.Unlock()
			if rawDDL.CRTs > h.resolvedTS {
<<<<<<< HEAD
=======
				lastResolvedTsAdvancedTime = h.clock.Now()
>>>>>>> 1e8f99f5e (cdc/owner: add some logs to help debug puller / kvclient / lock resolver (#4822))
				h.resolvedTS = rawDDL.CRTs
			}
			return nil
		}
		job, err := entry.UnmarshalDDL(rawDDL)
		if err != nil {
			return errors.Trace(err)
		}
		if job == nil {
			return nil
		}
		if h.filter.ShouldDiscardDDL(job.Type) {
			log.Info("discard the ddl job", zap.Int64("jobID", job.ID), zap.String("query", job.Query))
			return nil
		}
		if job.ID == h.lastDDLJobID {
			log.Warn("ignore duplicated DDL job", zap.Any("job", job))
			return nil
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.pendingDDLJobs = append(h.pendingDDLJobs, job)
		h.lastDDLJobID = job.ID
		return nil
	}

<<<<<<< HEAD
	errg.Go(func() error {
=======
	ticker := h.clock.Ticker(ownerDDLPullerStuckWarnTimeout)
	defer ticker.Stop()

	g.Go(func() error {
>>>>>>> 1e8f99f5e (cdc/owner: add some logs to help debug puller / kvclient / lock resolver (#4822))
		for {
			select {
			case <-stdCtx.Done():
				return stdCtx.Err()
<<<<<<< HEAD
=======
			case <-ticker.C:
				duration := h.clock.Since(lastResolvedTsAdvancedTime)
				if duration > ownerDDLPullerStuckWarnTimeout {
					log.Warn("ddl puller resolved ts has not advanced",
						zap.String("changefeed", ctx.ChangefeedVars().ID),
						zap.Duration("duration", duration),
						zap.Uint64("resolvedTs", h.resolvedTS))
				}
>>>>>>> 1e8f99f5e (cdc/owner: add some logs to help debug puller / kvclient / lock resolver (#4822))
			case e := <-rawDDLCh:
				if err := receiveDDL(e); err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	return g.Wait()
}

func (h *ddlPullerImpl) FrontDDL() (uint64, *timodel.Job) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return h.resolvedTS, nil
	}
	job := h.pendingDDLJobs[0]
	return job.BinlogInfo.FinishedTS, job
}

func (h *ddlPullerImpl) PopFrontDDL() (uint64, *timodel.Job) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return h.resolvedTS, nil
	}
	job := h.pendingDDLJobs[0]
	h.pendingDDLJobs = h.pendingDDLJobs[1:]
	return job.BinlogInfo.FinishedTS, job
}

func (h *ddlPullerImpl) Close() {
	log.Info("Close the ddl puller")
	h.cancel()
}
