package puller

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	ownerDDLPullerStuckWarnTimeout = 30 * time.Second
)

type DDLPuller interface {
	// Run runs the DDLPuller
	Run(ctx context.Context) error
	// FrontDDL returns the first DDL job in the internal queue
	FrontDDL() (uint64, *timodel.Job)
	// PopFrontDDL returns and pops the first DDL job in the internal queue
	PopFrontDDL() (uint64, *timodel.Job)
	// Close closes the DDLPuller
	Close()
}

type ddlPullerImpl struct {
	puller Puller
	filter *filter.Filter

	mu             sync.Mutex
	resolvedTS     uint64
	pendingDDLJobs []*timodel.Job
	lastDDLJobID   int64
	cancel         context.CancelFunc

	changefeedID model.ChangeFeedID

	clock                      clock.Clock
	lastResolvedTsAdvancedTime time.Time
}

func NewDDLPuller(ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	up *upstream.Upstream,
	startTs uint64,
	changefeed model.ChangeFeedID,
) (DDLPuller, error) {
	f, err := filter.NewFilter(replicaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var puller Puller
	storage := up.KVStorage
	// storage can be nil only in the test
	if storage == nil {
		puller = New(
			ctx, up.PDClient,
			up.GrpcPool,
			up.RegionCache,
			storage,
			up.PDClock,
			changefeed,
			//// Add "_ddl_puller" to make it different from table pullers.
			//model.ChangeFeedID{
			//	Namespace: ctx.ChangefeedVars().ID.Namespace,
			//	// Add "_ddl_puller" to make it different from table pullers.
			//	ID: ctx.ChangefeedVars().ID.ID + "_ddl_puller",
			//},
			startTs,
			[]regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()},
			config.GetGlobalServerConfig().KVClient,
		)
	}

	return &ddlPullerImpl{
		puller:       puller,
		resolvedTS:   startTs,
		filter:       f,
		cancel:       func() {},
		clock:        clock.New(),
		changefeedID: changefeed,
	}, nil

}

//func newDDLPuller(ctx cdcContext.Context,
//	up *upstream.Upstream, startTs uint64,
//) (DDLPuller, error) {
//	f, err := filter.NewFilter(ctx.ChangefeedVars().Info.Config)
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//	kvCfg := config.GetGlobalServerConfig().KVClient
//	var plr puller.Puller
//	kvStorage := up.KVStorage
//	// kvS
//	if kvStorage != nil {

//	}
//
//	return &ddlPullerImpl{
//		puller:     plr,
//		resolvedTS: startTs,
//		filter:     f,
//		cancel:     func() {},
//		clock:      clock.New(),
//		changefeedID: model.ChangeFeedID{
//			Namespace: ctx.ChangefeedVars().ID.Namespace,
//			// Add "_ddl_puller" to make it different from table pullers.
//			ID: ctx.ChangefeedVars().ID.ID + "_ddl_puller",
//		},
//	}, nil
//}

func (h *ddlPullerImpl) handleRawDDL(rawDDL *model.RawKVEntry) error {
	if rawDDL == nil {
		return nil
	}
	if rawDDL.OpType == model.OpTypeResolved {
		h.mu.Lock()
		defer h.mu.Unlock()
		if rawDDL.CRTs > h.resolvedTS {
			h.lastResolvedTsAdvancedTime = h.clock.Now()
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
		log.Info("discard the ddl job",
			zap.String("namespace", h.changefeedID.Namespace),
			zap.String("changefeed", h.changefeedID.ID),
			zap.Int64("jobID", job.ID), zap.String("query", job.Query))
		return nil
	}
	if job.ID == h.lastDDLJobID {
		log.Warn("ignore duplicated DDL job",
			zap.String("namespace", h.changefeedID.Namespace),
			zap.String("changefeed", h.changefeedID.ID),
			zap.Any("job", job))
		return nil
	}
	log.Info("receive new ddl job",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID),
		zap.Any("job", job))

	h.mu.Lock()
	defer h.mu.Unlock()
	h.pendingDDLJobs = append(h.pendingDDLJobs, job)
	h.lastDDLJobID = job.ID
	return nil
}

func (h *ddlPullerImpl) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	ctx = contextutil.PutTableInfoInCtx(ctx, -1, DDLPullerTableName)
	ctx = contextutil.PutChangefeedIDInCtx(ctx, h.changefeedID)

	// ctx = contextutil.PutRoleInCtx(ctx, util.RoleProcessor)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return h.puller.Run(ctx)
	})

	rawDDLCh := memory.SortOutput(ctx, h.puller.Output())

	ticker := h.clock.Ticker(ownerDDLPullerStuckWarnTimeout)
	defer ticker.Stop()

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				duration := h.clock.Since(h.lastResolvedTsAdvancedTime)
				if duration > ownerDDLPullerStuckWarnTimeout {
					log.Warn("ddl puller resolved ts has not advanced",
						zap.String("namespace", h.changefeedID.Namespace),
						zap.String("changefeed", h.changefeedID.ID),
						zap.Duration("duration", duration),
						zap.Uint64("resolvedTs", h.resolvedTS))
				}
			case e := <-rawDDLCh:
				if err := h.handleRawDDL(e); err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	log.Info("DDL puller started",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID),
		zap.Uint64("resolvedTS", h.resolvedTS))

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
	log.Info("Close the ddl puller",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID))
	h.cancel()
}
