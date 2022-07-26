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
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/entry/schema"
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
	// Run starts the DDLJobPuller.
	Run(ctx context.Context) error
	// Output the DDL job entry, it contains the DDL job and the error.
	Output() <-chan *model.DDLJobEntry
}

type ddlJobPullerImpl struct {
	changefeedID   model.ChangeFeedID
	puller         Puller
	kvStorage      tidbkv.Storage
	schemaSnapshot *schema.Snapshot
	resolvedTs     uint64
	filter         filter.Filter
	// tableInfo is initialized when receive the first concurrent DDL job.
	// It holds the info of table `tidb_ddl_jobs` of upstream TiDB.
	tableInfo *model.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_jobs`.
	columnID                  int64
	outputCh                  chan *model.DDLJobEntry
	metricDiscardedDDLCounter prometheus.Counter
}

// Run starts the DDLJobPuller.
func (p *ddlJobPullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return errors.Trace(p.puller.Run(ctx))
	})

	rawDDLCh := memory.SortOutput(ctx, p.puller.Output())

	for {
		var ddlRawKV *model.RawKVEntry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ddlRawKV = <-rawDDLCh:
		}
		if ddlRawKV == nil {
			continue
		}

		if ddlRawKV.OpType == model.OpTypeResolved {
			if ddlRawKV.CRTs > p.resolvedTs {
				p.resolvedTs = ddlRawKV.CRTs
			}
		}

		job, err := p.unmarshalDDL(ddlRawKV)
		if err != nil {
			log.Error("unmarshal ddl job failed", zap.Error(err))
			return errors.Trace(err)
		}

		if job != nil {
			log.Info("fizz: get ddl job", zap.String("job", job.String()), zap.String("query", job.Query))
		}

		skip, err := p.handleJob(job)
		if err != nil {
			log.Error("fizz: ddl job handler error", zap.Error(err))
			return errors.Trace(err)
		}
		if skip {
			continue
		}

		jobEntry := &model.DDLJobEntry{
			Job:    job,
			OpType: ddlRawKV.OpType,
			CRTs:   ddlRawKV.CRTs,
			Err:    err,
		}
		if job != nil {
			log.Info("fizz: sent ddl job entry", zap.String("query", job.Query), zap.String("job", job.String()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.outputCh <- jobEntry:
		}
	}
}

// Output the DDL job entry, it contains the DDL job and the error.
func (p *ddlJobPullerImpl) Output() <-chan *model.DDLJobEntry {
	return p.outputCh
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

func (p *ddlJobPullerImpl) unmarshalDDL(rawKV *model.RawKVEntry) (*timodel.Job, error) {
	if rawKV.OpType != model.OpTypePut {
		return nil, nil
	}
	if p.tableInfo == nil && !entry.IsLegacyFormatJob(rawKV) {
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

// handleRenameTables handles the rename tables DDL job.
// Since TiDB treats rename multiple tables as a single DDL job,
// we need to handle it separately.
func (p *ddlJobPullerImpl) handleRenameTables(job *timodel.Job) (skip bool, err error) {
	// parseRenameTables gets a list of DDLEvent from a rename tables DDL job.
	var (
		oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
		newTableNames, oldSchemaNames           []*timodel.CIStr
	)

	var (
		remainOldSchemaIDs, remainNewSchemaIDs, remainOldTableIDs []int64
		remainNewTableNames, remainOldSchemaNames                 []*timodel.CIStr
	)

	err = job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs,
		&newTableNames, &oldTableIDs, &oldSchemaNames)
	if err != nil {
		return true, errors.Trace(err)
	}

	multiTableInfos := job.BinlogInfo.MultipleTableInfos
	if len(multiTableInfos) != len(oldSchemaIDs) ||
		len(multiTableInfos) != len(newSchemaIDs) ||
		len(multiTableInfos) != len(newTableNames) ||
		len(multiTableInfos) != len(oldTableIDs) ||
		len(multiTableInfos) != len(oldSchemaNames) {
		return true, cerror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
	}

	newMultiTableInfos := make([]*timodel.TableInfo, 0, len(multiTableInfos))
	for i, tableInfo := range multiTableInfos {
		preTableInfo, ok := p.schemaSnapshot.PhysicalTableByID(tableInfo.ID)
		if !ok {
			// If we can not find a table by its id in schemaSnapshot,
			// it means we didn't replicate the table before,
			// so we can just ignore it here.
			log.Info("table not found", zap.Int64("tableID", tableInfo.ID),
				zap.String("tableName", tableInfo.Name.O))
			continue
		}

		oldSchemaName := oldSchemaNames[i].L // fizz: 因为 filter 可能是大小写敏感的，这里需要测一下
		oldTableName := preTableInfo.Name.O

		// We only filter out the rename tables subJob by its
		// old schema name and old table name.
		if p.filter.ShouldDiscardDDL(job.Type, oldSchemaName, oldTableName) {
			continue
		}

		newMultiTableInfos = append(newMultiTableInfos, tableInfo)

		remainOldSchemaIDs = append(remainOldSchemaIDs, oldSchemaIDs[i])
		remainNewSchemaIDs = append(remainNewSchemaIDs, newSchemaIDs[i])
		remainOldTableIDs = append(remainOldTableIDs, oldTableIDs[i])
		remainNewTableNames = append(remainNewTableNames, newTableNames[i])
		remainOldSchemaNames = append(remainOldSchemaNames, oldSchemaNames[i])
	}

	if len(newMultiTableInfos) == 0 {
		return true, nil
	}

	newArgs := make([]json.RawMessage, 5)
	v, err := json.Marshal(remainOldSchemaIDs)
	if err != nil {
		return true, errors.Trace(err)
	}
	newArgs[0] = v
	v, err = json.Marshal(remainNewSchemaIDs)
	if err != nil {
		return true, errors.Trace(err)
	}
	newArgs[1] = v
	v, err = json.Marshal(remainNewTableNames)
	if err != nil {
		return true, errors.Trace(err)
	}
	newArgs[2] = v
	v, err = json.Marshal(remainOldTableIDs)
	if err != nil {
		return true, errors.Trace(err)
	}
	newArgs[3] = v
	v, err = json.Marshal(remainOldSchemaNames)
	if err != nil {
		return true, errors.Trace(err)
	}
	newArgs[4] = v

	newRawArgs, err := json.Marshal(newArgs)
	if err != nil {
		return true, errors.Trace(err)
	}
	job.RawArgs = newRawArgs
	job.BinlogInfo.MultipleTableInfos = newMultiTableInfos
	return false, nil
}

// handleJob handles the DDL job.
// It split rename tables DDL job and fill the job table name.
func (p *ddlJobPullerImpl) handleJob(job *timodel.Job) (skip bool, err error) {
	if job == nil {
		return false, nil
	}

	defer func() {
		if err != nil {
			log.Error("handle job error", zap.Error(err))
		}
	}()

	var isRenameTables bool
	switch job.Type {
	case timodel.ActionRenameTables:
		isRenameTables = true
		skip, err = p.handleRenameTables(job)
		if err != nil {
			return true, errors.Trace(err)
		}
		if skip {
			return true, nil
		}
	case timodel.ActionRenameTable:
		table, ok := p.schemaSnapshot.PhysicalTableByID(job.TableID)
		if !ok {
			log.Warn("ddl table not found in schema snapshot, ignore it",
				zap.Int64("tableID", job.TableID),
				zap.String("job", job.String()),
				zap.String("query", job.Query))
			return true, nil
		}
		job.TableName = table.Name.O
	default:
		if err := p.schemaSnapshot.FillSchemaName(job); err != nil {
			return true, errors.Trace(err)
		}
		job.TableName = job.BinlogInfo.TableInfo.Name.O
	}

	if !isRenameTables {
		if job.BinlogInfo.FinishedTS < p.resolvedTs ||
			p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.TableName) {
			log.Info("discard the ddl job",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.String("job", job.String()),
				zap.String("schema", job.SchemaName),
				zap.String("table", job.TableName),
				zap.String("query", job.Query),
				zap.Any("pullerResolvedTs", p.resolvedTs),
				zap.Any("jobFinishedTS", job.BinlogInfo.FinishedTS))
			p.metricDiscardedDDLCounter.Inc()
			return true, nil
		}
	}

	err = p.schemaSnapshot.HandleDDL(job)
	if err != nil {
		log.Error("handle ddl job failed", zap.String("job", job.String()), zap.Error(err))
		return true, errors.Trace(err)
	}
	return false, nil
}

// NewDDLJobPuller creates a new NewDDLJobPuller, which fetches events starting event start from checkpointTs.
func NewDDLJobPuller(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	checkpointTs uint64,
	cfg *config.KVClientConfig,
	replicaConfig *config.ReplicaConfig,
	changefeed model.ChangeFeedID,
) (DDLJobPuller, error) {
	var meta *timeta.Meta
	if kvStorage != nil {
		var err error
		meta, err = kv.GetSnapshotMeta(kvStorage, checkpointTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	schemaSnap, err := schema.NewSingleSnapshotFromMeta(meta, checkpointTs, replicaConfig.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	f, err := filter.NewFilter(replicaConfig, "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ddlJobPullerImpl{
		changefeedID:   changefeed,
		filter:         f,
		schemaSnapshot: schemaSnap,
		puller:         New(ctx, pdCli, grpcPool, regionCache, kvStorage, pdClock, checkpointTs, regionspan.GetAllDDLSpan(), cfg, changefeed),
		kvStorage:      kvStorage,
		outputCh:       make(chan *model.DDLJobEntry, defaultPullerOutputChanSize),
		metricDiscardedDDLCounter: discardedDDLCounter.
			WithLabelValues(changefeed.Namespace, changefeed.ID),
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
	ddlJobPuller DDLJobPuller

	mu             sync.Mutex
	resolvedTS     uint64
	pendingDDLJobs []*timodel.Job
	lastDDLJobID   int64
	cancel         context.CancelFunc

	changefeedID model.ChangeFeedID

	clock                      clock.Clock
	lastResolvedTsAdvancedTime time.Time
}

// NewDDLPuller return a puller for DDL Event
func NewDDLPuller(ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	up *upstream.Upstream,
	startTs uint64,
	changefeed model.ChangeFeedID,
) (DDLPuller, error) {
	// add "_ddl_puller" to make it different from table pullers.
	changefeed.ID += "_ddl_puller"

	var puller DDLJobPuller
	var err error
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
			replicaConfig,
			changefeed,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return &ddlPullerImpl{
		ddlJobPuller: puller,
		resolvedTS:   startTs,
		cancel:       func() {},
		clock:        clock.New(),
		changefeedID: changefeed,
	}, nil
}

func (h *ddlPullerImpl) handleDDLJobEntry(jobEntry *model.DDLJobEntry) error {
	if jobEntry.OpType == model.OpTypeResolved { // fizz: Resolved event 是一个空的 ddl 只是用来推进 resolvedTS
		if jobEntry.CRTs > atomic.LoadUint64(&h.resolvedTS) {
			h.lastResolvedTsAdvancedTime = h.clock.Now()
			atomic.StoreUint64(&h.resolvedTS, jobEntry.CRTs)
		}
		return nil
	}
	job, err := jobEntry.Job, jobEntry.Err
	if err != nil {
		return errors.Trace(err)
	}
	if job == nil {
		return nil
	}
	if job.ID == h.lastDDLJobID {
		log.Warn("ignore duplicated DDL job",
			zap.String("namespace", h.changefeedID.Namespace),
			zap.String("changefeed", h.changefeedID.ID),
			zap.String("query", job.Query),
			zap.Int64("jobID", job.ID),
			zap.Any("job", job))
		return nil
	}
	log.Info("receive new ddl job",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID),
		zap.String("query", job.Query),
		zap.Int64("jobID", job.ID),
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
		return h.ddlJobPuller.Run(ctx)
	})

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
			case e := <-h.ddlJobPuller.Output():
				if err := h.handleDDLJobEntry(e); err != nil {
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
		return atomic.LoadUint64(&h.resolvedTS), nil // fizz: 没有 ddl job 就返回上次的 resolvedTs
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
