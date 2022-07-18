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

package puller

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	ddlPullerStuckWarnDuration = 30 * time.Second
	// DDLPullerTableName is the fake table name for ddl puller
	DDLPullerTableName = "DDL_PULLER"
)

// DDLJobPuller is used to pull ddl job from TiKV.
type DDLJobPuller interface {
	Puller

	UnmarshalDDL(rawKV *model.RawKVEntry) (*timodel.Job, error)
}

type ddlJobPullerImpl struct {
	Puller

	kvStorage tidbkv.Storage
	tableInfo *model.TableInfo
	columnID  int64
}

func (p *ddlJobPullerImpl) initJobTableMeta() error {
	version, err := p.kvStorage.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap, err := kv.GetSnapshotMeta(p.kvStorage, version.Ver)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfos, err := snap.ListDatabases()
	if err != nil {
		return cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}

	db, err := findDBByName(dbInfos, mysql.SystemDB)
	if err != nil {
		return errors.Trace(err)
	}

	tbls, err := snap.ListTables(db.ID)
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo, err := findTableByName(tbls, "tidb_ddl_job")
	if err != nil {
		return errors.Trace(err)
	}

	col, err := findColumnByName(tableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.tableInfo = model.WrapTableInfo(db.ID, db.Name.L, 0, tableInfo)
	p.columnID = col.ID
	return nil
}

func (p *ddlJobPullerImpl) UnmarshalDDL(rawKV *model.RawKVEntry) (*timodel.Job, error) {
	if rawKV.OpType != model.OpTypePut {
		return nil, nil
	}
	if p.tableInfo == nil && !bytes.HasPrefix(rawKV.Key, entry.MetaPrefix) {
		err := p.initJobTableMeta()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return entry.ParseDDLJob(p.tableInfo, rawKV, p.columnID)
}

func findDBByName(dbs []*timodel.DBInfo, name string) (*timodel.DBInfo, error) {
	for _, db := range dbs {
		if db.Name.L == name {
			return db, nil
		}
	}

	return nil, cerror.WrapError(cerror.ErrDDLSchemaNotFound, errors.Errorf("can't find schema %s", name))
}

func findTableByName(tbls []*timodel.TableInfo, name string) (*timodel.TableInfo, error) {
	for _, t := range tbls {
		if t.Name.L == name {
			return t, nil
		}
	}

	return nil, cerror.WrapError(cerror.ErrDDLSchemaNotFound, errors.Errorf("can't find table %s", name))
}

func findColumnByName(cols []*timodel.ColumnInfo, name string) (*timodel.ColumnInfo, error) {
	for _, c := range cols {
		if c.Name.L == name {
			return c, nil
		}
	}

	return nil, cerror.WrapError(cerror.ErrDDLSchemaNotFound, errors.Errorf("can't find column %s", name))
}

// NewDDLJobPuller create a new NewDDLJobPuller fetch event start from checkpointTs
// and put into buf.
func NewDDLJobPuller(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	checkpointTs uint64,
	cfg *config.KVClientConfig,
	changefeed model.ChangeFeedID,
) (DDLJobPuller, error) {
	return &ddlJobPullerImpl{
		Puller:    New(ctx, pdCli, grpcPool, regionCache, kvStorage, pdClock, checkpointTs, regionspan.GetAllDDLSpan(), cfg, changefeed),
		kvStorage: kvStorage,
	}, nil
}

// DDLPuller is the interface for DDL Puller, used by owner only.
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
	puller DDLJobPuller
	filter filter.Filter

	mu             sync.Mutex
	resolvedTS     uint64
	pendingDDLJobs []*timodel.Job
	lastDDLJobID   int64
	cancel         context.CancelFunc

	changefeedID model.ChangeFeedID

	clock                      clock.Clock
	lastResolvedTsAdvancedTime time.Time
	metricDiscardedDDLCounter  prometheus.Counter
}

// NewDDLPuller return a puller for DDL Event
func NewDDLPuller(ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	up *upstream.Upstream,
	startTs uint64,
	changefeed model.ChangeFeedID,
) (DDLPuller, error) {
	// It is no matter to use a empty as timezone here because DDLPuller
	// doesn't use expression filter's method.
	f, err := filter.NewFilter(replicaConfig, "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	// add "_ddl_puller" to make it different from table pullers.
	changefeed.ID += "_ddl_puller"

	var puller DDLJobPuller
	storage := up.KVStorage
	// storage can be nil only in the test
	if storage != nil {
		puller, err = NewDDLJobPuller(
			ctx,
			up.PDClient,
			up.GrpcPool,
			up.RegionCache,
			storage,
			up.PDClock,
			startTs,
			config.GetGlobalServerConfig().KVClient,
			changefeed,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return &ddlPullerImpl{
		puller:        puller,
		resolvedTS:    startTs,
		filter:        f,
		cancel:        func() {},
		clock:         clock.New(),
		changefeedID:  changefeed,
		metricDiscardedDDLCounter: discardedDDLCounter.
			WithLabelValues(changefeed.Namespace, changefeed.ID),
	}, nil
}

func (h *ddlPullerImpl) handleRawDDL(rawDDL *model.RawKVEntry) error {
	if rawDDL == nil {
		return nil
	}
	if rawDDL.OpType == model.OpTypeResolved {
		if rawDDL.CRTs > atomic.LoadUint64(&h.resolvedTS) {
			h.lastResolvedTsAdvancedTime = h.clock.Now()
			atomic.StoreUint64(&h.resolvedTS, rawDDL.CRTs)
		}
		return nil
	}
	job, err := h.puller.UnmarshalDDL(rawDDL)
	if err != nil {
		return errors.Trace(err)
	}
	if job == nil {
		return nil
	}
	if h.filter.ShouldDiscardDDL(job.Type) {
		h.metricDiscardedDDLCounter.Inc()
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

// Run the ddl puller to receive DDL events
func (h *ddlPullerImpl) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	ctx = contextutil.PutTableInfoInCtx(ctx, -1, DDLPullerTableName)
	ctx = contextutil.PutChangefeedIDInCtx(ctx, h.changefeedID)
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleOwner)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return h.puller.Run(ctx)
	})

	rawDDLCh := memory.SortOutput(ctx, h.puller.Output())

	ticker := h.clock.Ticker(ddlPullerStuckWarnDuration)
	defer ticker.Stop()

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				duration := h.clock.Since(h.lastResolvedTsAdvancedTime)
				if duration > ddlPullerStuckWarnDuration {
					log.Warn("ddl puller resolved ts has not advanced",
						zap.String("namespace", h.changefeedID.Namespace),
						zap.String("changefeed", h.changefeedID.ID),
						zap.Duration("duration", duration),
						zap.Uint64("resolvedTs", atomic.LoadUint64(&h.resolvedTS)))
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
		zap.Uint64("resolvedTS", atomic.LoadUint64(&h.resolvedTS)))

	return g.Wait()
}

// FrontDDL return the first pending DDL job
func (h *ddlPullerImpl) FrontDDL() (uint64, *timodel.Job) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return atomic.LoadUint64(&h.resolvedTS), nil
	}
	job := h.pendingDDLJobs[0]
	return job.BinlogInfo.FinishedTS, job
}

// PopFrontDDL return the first pending DDL job and remove it from the pending list
func (h *ddlPullerImpl) PopFrontDDL() (uint64, *timodel.Job) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return atomic.LoadUint64(&h.resolvedTS), nil
	}
	job := h.pendingDDLJobs[0]
	h.pendingDDLJobs = h.pendingDDLJobs[1:]
	return job.BinlogInfo.FinishedTS, job
}

// Close the ddl puller, release all resources.
func (h *ddlPullerImpl) Close() {
	log.Info("Close the ddl puller",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID))
	h.cancel()
}
