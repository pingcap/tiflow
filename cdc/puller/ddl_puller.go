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
	// ddl puller should never filter any DDL jobs even if
	// the changefeed is in BDR mode, because the DDL jobs should
	// be filtered before they are sent to the sink
	ddLPullerFilterLoop = false
)

// DDLJobPuller is used to pull ddl job from TiKV.
// It's used by processor and ddlPullerImpl.
type DDLJobPuller interface {
	// Run starts the DDLJobPuller.
	Run(ctx context.Context) error
	// Output the DDL job entry, it contains the DDL job and the error.
	Output() <-chan *model.DDLJobEntry
}

// Note: All unexported methods of `ddlJobPullerImpl` should
// be called in the same one goroutine.
type ddlJobPullerImpl struct {
	changefeedID  model.ChangeFeedID
	puller        Puller
	kvStorage     tidbkv.Storage
	schemaStorage entry.SchemaStorage
	resolvedTs    uint64
	schemaVersion int64
	filter        filter.Filter
	// ddlJobsTable is initialized when receive the first concurrent DDL job.
	// It holds the info of table `tidb_ddl_jobs` of upstream TiDB.
	ddlJobsTable *model.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_jobs`.
	jobMetaColumnID           int64
	outputCh                  chan *model.DDLJobEntry
	metricDiscardedDDLCounter prometheus.Counter
}

// Run starts the DDLJobPuller.
func (p *ddlJobPullerImpl) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return errors.Trace(p.puller.Run(ctx))
	})

	rawDDLCh := memory.SortOutput(ctx, p.puller.Output())
	eg.Go(
		func() error {
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
					// Only nil in unit test case.
					if p.schemaStorage != nil {
						p.schemaStorage.AdvanceResolvedTs(ddlRawKV.CRTs)
					}
					if ddlRawKV.CRTs > p.getResolvedTs() {
						p.setResolvedTs(ddlRawKV.CRTs)
					}
				}

				job, err := p.unmarshalDDL(ddlRawKV)
				if err != nil {
					return errors.Trace(err)
				}

				if job != nil {
					skip, err := p.handleJob(job)
					if err != nil {
						return cerror.WrapError(cerror.ErrHandleDDLFailed,
							err, job.String(), job.Query, job.StartTS, job.StartTS)
					}
					log.Info("handle ddl job",
						zap.String("namespace", p.changefeedID.Namespace),
						zap.String("changefeed", p.changefeedID.ID),
						zap.String("query", job.Query),
						zap.Stringer("job", job), zap.Bool("skip", skip))
					if skip {
						continue
					}
				}

				jobEntry := &model.DDLJobEntry{
					Job:    job,
					OpType: ddlRawKV.OpType,
					CRTs:   ddlRawKV.CRTs,
					Err:    err,
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p.outputCh <- jobEntry:
				}
			}
		})
	return eg.Wait()
}

// Output the DDL job entry, it contains the DDL job and the error.
func (p *ddlJobPullerImpl) Output() <-chan *model.DDLJobEntry {
	return p.outputCh
}

func (p *ddlJobPullerImpl) getResolvedTs() uint64 {
	return atomic.LoadUint64(&p.resolvedTs)
}

func (p *ddlJobPullerImpl) setResolvedTs(ts uint64) {
	atomic.StoreUint64(&p.resolvedTs, ts)
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

	p.ddlJobsTable = model.WrapTableInfo(db.ID, db.Name.L, 0, tableInfo)
	p.jobMetaColumnID = col.ID
	return nil
}

func (p *ddlJobPullerImpl) unmarshalDDL(rawKV *model.RawKVEntry) (*timodel.Job, error) {
	if rawKV.OpType != model.OpTypePut {
		return nil, nil
	}
	if p.ddlJobsTable == nil && !entry.IsLegacyFormatJob(rawKV) {
		err := p.initJobTableMeta()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return entry.ParseDDLJob(p.ddlJobsTable, rawKV, p.jobMetaColumnID)
}

// handleRenameTables gets all the tables that are renamed
// in the DDL job out and filter them one by one,
// if all the tables are filtered, skip it.
func (p *ddlJobPullerImpl) handleRenameTables(job *timodel.Job) (skip bool, err error) {
	var (
		oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
		newTableNames, oldSchemaNames           []*timodel.CIStr
	)

	err = job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs,
		&newTableNames, &oldTableIDs, &oldSchemaNames)
	if err != nil {
		return true, errors.Trace(err)
	}

	var (
		remainOldSchemaIDs, remainNewSchemaIDs, remainOldTableIDs []int64
		remainNewTableNames, remainOldSchemaNames                 []*timodel.CIStr
	)

	multiTableInfos := job.BinlogInfo.MultipleTableInfos
	if len(multiTableInfos) != len(oldSchemaIDs) ||
		len(multiTableInfos) != len(newSchemaIDs) ||
		len(multiTableInfos) != len(newTableNames) ||
		len(multiTableInfos) != len(oldTableIDs) ||
		len(multiTableInfos) != len(oldSchemaNames) {
		return true, cerror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
	}

	// we filter subordinate rename table ddl by these principles:
	// 1. old table name matches the filter rule, remain it.
	// 2. old table name does not match and new table name matches the filter rule, return error.
	// 3. old table name and new table name do not match the filter rule, skip it.
	remainTables := make([]*timodel.TableInfo, 0, len(multiTableInfos))
	snap := p.schemaStorage.GetLastSnapshot()
	for i, tableInfo := range multiTableInfos {
		var shouldDiscardOldTable, shouldDiscardNewTable bool
		oldTable, ok := snap.PhysicalTableByID(tableInfo.ID)
		if !ok {
			shouldDiscardOldTable = true
		} else {
			shouldDiscardOldTable = p.filter.ShouldDiscardDDL(job.Type, oldSchemaNames[i].O, oldTable.Name.O)
		}

		newSchemaName, ok := snap.SchemaByID(newSchemaIDs[i])
		if !ok {
			// the new table name does not hit the filter rule, so we should discard the table.
			shouldDiscardNewTable = true
		} else {
			shouldDiscardNewTable = p.filter.ShouldDiscardDDL(job.Type, newSchemaName.Name.O, newTableNames[i].O)
		}

		if shouldDiscardOldTable && shouldDiscardNewTable {
			// skip a rename table ddl only when its old table name and new table name are both filtered.
			log.Info("RenameTables is filtered",
				zap.Int64("tableID", tableInfo.ID),
				zap.String("schema", oldSchemaNames[i].O),
				zap.String("query", job.Query))
			continue
		}
		if shouldDiscardOldTable && !shouldDiscardNewTable {
			// if old table is not in filter rule and its new name is in filter rule, return error.
			return true, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(tableInfo.ID, job.Query)
		}
		// old table name matches the filter rule, remain it.
		remainTables = append(remainTables, tableInfo)
		remainOldSchemaIDs = append(remainOldSchemaIDs, oldSchemaIDs[i])
		remainNewSchemaIDs = append(remainNewSchemaIDs, newSchemaIDs[i])
		remainOldTableIDs = append(remainOldTableIDs, oldTableIDs[i])
		remainNewTableNames = append(remainNewTableNames, newTableNames[i])
		remainOldSchemaNames = append(remainOldSchemaNames, oldSchemaNames[i])
	}

	if len(remainTables) == 0 {
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
	job.BinlogInfo.MultipleTableInfos = remainTables
	return false, nil
}

// handleJob handles the DDL job.
// It split rename tables DDL job and fill the job table name.
func (p *ddlJobPullerImpl) handleJob(job *timodel.Job) (skip bool, err error) {
	// Only nil in test.
	if p.schemaStorage == nil {
		return false, nil
	}

	defer func() {
		if skip && err == nil {
			log.Info("ddl job schema or table does not match, discard it",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.String("schema", job.SchemaName),
				zap.String("table", job.TableName),
				zap.String("query", job.Query),
				zap.String("job", job.String()))
			p.metricDiscardedDDLCounter.Inc()
		}
	}()

	snap := p.schemaStorage.GetLastSnapshot()
	// Do this first to fill the schema name to its origin schema name.
	if err := snap.FillSchemaName(job); err != nil {
		log.Info("failed to fill schema name for ddl job", zap.Error(err))
		// If we can't find a job's schema, check if it's been filtered.
		if p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.TableName) {
			return true, nil
		}
		return true, errors.Trace(err)
	}

	if job.BinlogInfo.FinishedTS <= p.getResolvedTs() ||
		job.BinlogInfo.SchemaVersion <= p.schemaVersion {
		log.Info("ddl job finishedTs less than puller resolvedTs,"+
			"discard the ddl job",
			zap.Uint64("jobFinishedTS", job.BinlogInfo.FinishedTS),
			zap.Uint64("pullerResolvedTs", p.getResolvedTs()),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.String("query", job.Query),
			zap.String("job", job.String()))
		p.metricDiscardedDDLCounter.Inc()
		return true, nil
	}

	switch job.Type {
	case timodel.ActionRenameTables:
		skip, err = p.handleRenameTables(job)
		if err != nil {
			return true, errors.Trace(err)
		}
	case timodel.ActionRenameTable:
		log.Info("rename table ddl job",
			zap.Int64("newSchemaID", job.SchemaID),
			zap.String("newSchemaName", job.SchemaName),
			zap.Int64("tableID", job.TableID),
			zap.String("oldTableName", job.BinlogInfo.TableInfo.Name.O),
			zap.String("newTableName", job.TableName),
		)
		oldTable, ok := snap.PhysicalTableByID(job.TableID)
		if !ok {
			// 1. If we can not find the old table, and the new table name is in filter rule, return error.
			discard := p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.BinlogInfo.TableInfo.Name.O)
			if !discard {
				return true, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(job.TableID, job.Query)
			}
			skip = true
		} else {
			log.Info("rename table ddl job",
				zap.String("oldTableName", oldTable.TableName.Table),
				zap.String("oldSchemaName", oldTable.TableName.Schema))
			// 2. If we can find the preTableInfo, we filter it by the old table name.
			skipByOldTableName := p.filter.ShouldDiscardDDL(job.Type, oldTable.TableName.Schema, oldTable.TableName.Table)
			skipByNewTableName := p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.BinlogInfo.TableInfo.Name.O)
			// 3. If its old table name is not in filter rule, and its new table name in filter rule, return error.
			if skipByOldTableName && !skipByNewTableName {
				return true, cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(job.TableID, job.Query)
			}
			if skipByOldTableName && skipByNewTableName {
				skip = true
				return true, nil
			}
		}
	default:
		// nil means it is a schema ddl job, it's no need to fill the table name.
		if job.BinlogInfo.TableInfo != nil {
			job.TableName = job.BinlogInfo.TableInfo.Name.O
		}
		skip = p.filter.ShouldDiscardDDL(job.Type, job.SchemaName, job.TableName)
	}

	if skip {
		return true, nil
	}

	err = p.schemaStorage.HandleDDLJob(job)
	if err != nil {
		log.Error("handle ddl job failed",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("query", job.Query),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.BinlogInfo.TableInfo.Name.O),
			zap.String("job", job.String()),
			zap.Error(err))
		return true, errors.Trace(err)
	}

	p.setResolvedTs(job.BinlogInfo.FinishedTS)
	p.schemaVersion = job.BinlogInfo.SchemaVersion

	return false, nil
}

func findDBByName(dbs []*timodel.DBInfo, name string) (*timodel.DBInfo, error) {
	for _, db := range dbs {
		if db.Name.L == name {
			return db, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find schema %s", name))
}

func findTableByName(tbls []*timodel.TableInfo, name string) (*timodel.TableInfo, error) {
	for _, t := range tbls {
		if t.Name.L == name {
			return t, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find table %s", name))
}

func findColumnByName(cols []*timodel.ColumnInfo, name string) (*timodel.ColumnInfo, error) {
	for _, c := range cols {
		if c.Name.L == name {
			return c, nil
		}
	}
	return nil, cerror.WrapError(
		cerror.ErrDDLSchemaNotFound,
		errors.Errorf("can't find column %s", name))
}

// NewDDLJobPuller creates a new NewDDLJobPuller,
// which fetches ddl events starting from checkpointTs.
func NewDDLJobPuller(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	checkpointTs uint64,
	cfg *config.ServerConfig,
	changefeed model.ChangeFeedID,
	schemaStorage entry.SchemaStorage,
	filter filter.Filter,
) (DDLJobPuller, error) {
	return &ddlJobPullerImpl{
		changefeedID:  changefeed,
		filter:        filter,
		schemaStorage: schemaStorage,
		puller: New(
			ctx,
			pdCli,
			grpcPool,
			regionCache,
			kvStorage,
			pdClock,
			checkpointTs,
			regionspan.GetAllDDLSpan(),
			cfg,
			changefeed,
			-1, DDLPullerTableName,
			ddLPullerFilterLoop,
		),
		kvStorage: kvStorage,
		outputCh:  make(chan *model.DDLJobEntry, defaultPullerOutputChanSize),
		metricDiscardedDDLCounter: discardedDDLCounter.
			WithLabelValues(changefeed.Namespace, changefeed.ID),
	}, nil
}

// DDLPuller is the interface for DDL Puller, used by owner only.
type DDLPuller interface {
	// Run runs the DDLPuller
	Run(ctx context.Context) error
	// PopFrontDDL returns and pops the first DDL job in the internal queue
	PopFrontDDL() (uint64, *timodel.Job)
	// ResolvedTs returns the resolved ts of the DDLPuller
	ResolvedTs() uint64
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
	schemaStorage entry.SchemaStorage,
	filter filter.Filter,
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
			config.GetGlobalServerConfig(),
			changefeed,
			schemaStorage,
			filter,
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
	if jobEntry.OpType == model.OpTypeResolved {
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
	log.Info("[ddl] handleDDLJobEntry", zap.String("job", job.String()))
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
	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	ctx = contextutil.PutRoleInCtx(ctx, util.RoleOwner)
	g.Go(func() error {
		return h.ddlJobPuller.Run(ctx)
	})

	ticker := h.clock.Ticker(ddlPullerStuckWarnDuration)
	defer ticker.Stop()

	g.Go(func() error {
		h.lastResolvedTsAdvancedTime = h.clock.Now()
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
	log.Info("close the ddl puller",
		zap.String("namespace", h.changefeedID.Namespace),
		zap.String("changefeed", h.changefeedID.ID))
	h.cancel()
}

func (h *ddlPullerImpl) ResolvedTs() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingDDLJobs) == 0 {
		return atomic.LoadUint64(&h.resolvedTS)
	}
	job := h.pendingDDLJobs[0]
	return job.BinlogInfo.FinishedTS
}
