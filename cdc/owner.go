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
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type ownership struct {
	lastTickTime time.Time
	tickTime     time.Duration
}

func newOwnership(tickTime time.Duration) ownership {
	minTickTime := 5 * time.Second
	if tickTime > minTickTime {
		log.Panic("ownership counter must be incearsed every 5 seconds")
	}
	return ownership{
		tickTime: minTickTime,
	}
}

func (o *ownership) inc() {
	now := time.Now()
	if now.Sub(o.lastTickTime) > o.tickTime {
		// Keep the value of promtheus expression `rate(counter)` = 1
		// Please also change alert rule in ticdc.rules.yml when change the expression value.
		ownershipCounter.Add(float64(o.tickTime / time.Second))
		o.lastTickTime = now
	}
}

type minGCSafePointCacheEntry struct {
	ts          model.Ts
	lastUpdated time.Time
}

func (o *Owner) getMinGCSafePointCache(ctx context.Context) model.Ts {
	if time.Now().After(o.minGCSafePointCache.lastUpdated.Add(MinGCSafePointCacheUpdateInterval)) {
		physicalTs, logicalTs, err := o.pdClient.GetTS(ctx)
		if err != nil {
			log.Warn("Fail to update minGCSafePointCache.", zap.Error(err))
			return o.minGCSafePointCache.ts
		}
		o.minGCSafePointCache.ts = oracle.ComposeTS(physicalTs-(o.gcTTL*1000), logicalTs)

		// o.pdGCSafePoint pd is the smallest gcSafePoint across all services.
		// If tikv_gc_life_time > gcTTL, means that tikv_gc_safe_point < o.minGCSafePointCache.ts here.
		// It also means that pd.pdGCSafePoint < o.minGCSafePointCache.ts here, we should use its value as the min value.
		// This ensures that when tikv_gc_life_time > gcTTL , cdc will not advance the gcSafePoint.
		if o.pdGCSafePoint < o.minGCSafePointCache.ts {
			o.minGCSafePointCache.ts = o.pdGCSafePoint
		}

		o.minGCSafePointCache.lastUpdated = time.Now()
	}
	return o.minGCSafePointCache.ts
}

// Owner manages the cdc cluster
type Owner struct {
	done        chan struct{}
	session     *concurrency.Session
	changeFeeds map[model.ChangeFeedID]*changeFeed
	// failInitFeeds record changefeeds that meet error during initialization
	failInitFeeds map[model.ChangeFeedID]struct{}
	// stoppedFeeds record changefeeds that meet running error
	stoppedFeeds              map[model.ChangeFeedID]*model.ChangeFeedStatus
	rebalanceTigger           map[model.ChangeFeedID]bool
	rebalanceForAllChangefeed bool
	manualScheduleCommand     map[model.ChangeFeedID][]*model.MoveTableJob
	rebalanceMu               sync.Mutex

	cfRWriter ChangeFeedRWriter

	l sync.RWMutex

	pdEndpoints []string
	grpcPool    kv.GrpcPool
	pdClient    pd.Client
	etcdClient  kv.CDCEtcdClient

	captureLoaded int32
	captures      map[model.CaptureID]*model.CaptureInfo

	adminJobs     []model.AdminJob
	adminJobsLock sync.Mutex

	stepDown func(ctx context.Context) error

	// gcTTL is the ttl of cdc gc safepoint ttl.
	gcTTL int64
	// last update gc safepoint time. zero time means has not updated or cleared
	gcSafepointLastUpdate time.Time
	// stores the ts obtained from PD and is updated every MinGCSafePointCacheUpdateInterval.
	minGCSafePointCache minGCSafePointCacheEntry
	// stores the actual gcSafePoint stored in pd
	pdGCSafePoint model.Ts
	// record last time that flushes all changefeeds' replication status
	lastFlushChangefeeds    time.Time
	flushChangefeedInterval time.Duration
	feedChangeNotifier      *notify.Notifier
}

const (
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	CDCServiceSafePointID = "ticdc"
	// GCSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
	GCSafepointUpdateInterval = 2 * time.Second
	// MinGCSafePointCacheUpdateInterval is the interval that update minGCSafePointCache
	MinGCSafePointCacheUpdateInterval = time.Second * 2
)

// NewOwner creates a new Owner instance
func NewOwner(
	ctx context.Context,
	pdClient pd.Client,
	grpcPool kv.GrpcPool,
	sess *concurrency.Session,
	gcTTL int64,
	flushChangefeedInterval time.Duration,
) (*Owner, error) {
	cli := kv.NewCDCEtcdClient(ctx, sess.Client())
	endpoints := sess.Client().Endpoints()

	failpoint.Inject("ownerFlushIntervalInject", func(val failpoint.Value) {
		flushChangefeedInterval = time.Millisecond * time.Duration(val.(int))
	})

	owner := &Owner{
		done:                    make(chan struct{}),
		session:                 sess,
		pdClient:                pdClient,
		grpcPool:                grpcPool,
		changeFeeds:             make(map[model.ChangeFeedID]*changeFeed),
		failInitFeeds:           make(map[model.ChangeFeedID]struct{}),
		stoppedFeeds:            make(map[model.ChangeFeedID]*model.ChangeFeedStatus),
		captures:                make(map[model.CaptureID]*model.CaptureInfo),
		rebalanceTigger:         make(map[model.ChangeFeedID]bool),
		manualScheduleCommand:   make(map[model.ChangeFeedID][]*model.MoveTableJob),
		pdEndpoints:             endpoints,
		cfRWriter:               cli,
		etcdClient:              cli,
		gcTTL:                   gcTTL,
		flushChangefeedInterval: flushChangefeedInterval,
		feedChangeNotifier:      new(notify.Notifier),
	}

	return owner, nil
}

func (o *Owner) addCapture(_ context.Context, info *model.CaptureInfo) {
	o.l.Lock()
	o.captures[info.ID] = info
	o.l.Unlock()
	o.rebalanceMu.Lock()
	o.rebalanceForAllChangefeed = true
	o.rebalanceMu.Unlock()
}

// When a table is moved from one capture to another, the workflow is as follows
// 1. Owner deletes the table from the original capture (we call it capture-1),
//    and adds an table operation record in the task status
// 2. The processor in capture-1 reads the operation record, and waits the table
//    checkpoint ts reaches the boundary ts in operation, which often equals to
//    the global resovled ts, larger the current checkpoint ts of this table.
// 3. After table checkpoint ts reaches boundary ts, capture-1 marks the table
//    operation as finished.
// 4. Owner reads the finished mark and re-dispatches this table to another capture.
//
// When capture-1 crashes between step-2 and step-3, this function should be
// called to let owner re dispatch the table. Besides owner could also crash at
// the same time, in that case this function should also be called. In addtition,
// this function only handles move table job: 1) the add table job persists both
// table replicaInfo and operation, we can recover enough information from table
// replicaInfo; 2) if a table is deleted from a capture and that capture crashes,
// we just ignore this table.
func (o *Owner) rebuildTableFromOperations(cf *changeFeed, taskStatus *model.TaskStatus, startTs uint64) {
	for tableID, op := range taskStatus.Operation {
		if op.Delete && op.Flag&model.OperFlagMoveTable > 0 {
			cf.orphanTables[tableID] = startTs
			if job, ok := cf.moveTableJobs[tableID]; ok {
				log.Info("remove outdated move table job", zap.Reflect("job", job), zap.Uint64("start-ts", startTs))
				delete(cf.moveTableJobs, tableID)
			}
		}
	}
}

func (o *Owner) removeCapture(ctx context.Context, info *model.CaptureInfo) {
	o.l.Lock()
	defer o.l.Unlock()

	delete(o.captures, info.ID)

	for _, feed := range o.changeFeeds {
		task, ok := feed.taskStatus[info.ID]
		if !ok {
			log.Warn("task status not found", zap.String("capture-id", info.ID), zap.String("changefeed", feed.id))
			continue
		}
		var startTs uint64
		pos, ok := feed.taskPositions[info.ID]
		if ok {
			startTs = pos.CheckPointTs
		} else {
			log.Warn("task position not found, fallback to use changefeed checkpointts",
				zap.String("capture-id", info.ID), zap.String("changefeed", feed.id))
			// maybe the processor hasn't added table yet, fallback to use the
			// global checkpoint ts as the start ts of the table.
			startTs = feed.status.CheckpointTs
		}

		for tableID, replicaInfo := range task.Tables {
			feed.orphanTables[tableID] = startTs
			if startTs < replicaInfo.StartTs {
				log.Warn("table startTs not consistent",
					zap.Uint64("table-start-ts", replicaInfo.StartTs),
					zap.Uint64("checkpoint-ts", startTs),
					zap.Reflect("status", feed.status))
				feed.orphanTables[tableID] = replicaInfo.StartTs
			}
		}

		o.rebuildTableFromOperations(feed, task, startTs)

		if err := o.etcdClient.LeaseGuardDeleteTaskStatus(ctx, feed.id, info.ID, o.session.Lease()); err != nil {
			log.Warn("failed to delete task status",
				zap.String("capture-id", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
		if err := o.etcdClient.LeaseGuardDeleteTaskPosition(ctx, feed.id, info.ID, o.session.Lease()); err != nil {
			log.Warn("failed to delete task position",
				zap.String("capture-id", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
		if err := o.etcdClient.LeaseGuardDeleteTaskWorkload(ctx, feed.id, info.ID, o.session.Lease()); err != nil {
			log.Warn("failed to delete task workload",
				zap.String("capture-id", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
		ownerMaintainTableNumGauge.DeleteLabelValues(feed.id, info.AdvertiseAddr, maintainTableTypeTotal)
		ownerMaintainTableNumGauge.DeleteLabelValues(feed.id, info.AdvertiseAddr, maintainTableTypeWip)
	}
}

func (o *Owner) addOrphanTable(cid model.CaptureID, tableID model.TableID, startTs model.Ts) {
	if cf, ok := o.changeFeeds[cid]; ok {
		cf.orphanTables[tableID] = startTs
	} else {
		log.Warn("changefeed not found", zap.String("changefeed", cid))
	}
}

func (o *Owner) newChangeFeed(
	ctx context.Context,
	id model.ChangeFeedID,
	processorsInfos model.ProcessorsInfos,
	taskPositions map[string]*model.TaskPosition,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) (cf *changeFeed, resultErr error) {
	log.Info("Find new changefeed", zap.Stringer("info", info),
		zap.String("changefeed", id), zap.Uint64("checkpoint ts", checkpointTs))
	if info.Config.CheckGCSafePoint {
		ensureTTL := int64(10 * 60)
		err := gc.EnsureChangefeedStartTsSafety(
			ctx, o.pdClient, id, ensureTTL, checkpointTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(nil, cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(nil, errors.New("failpoint injected retriable error"))
	})

	kvStore, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, checkpointTs, info.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	filter, err := filter.NewFilter(info.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlHandler := newDDLHandler(o.pdClient, o.grpcPool, kvStore, checkpointTs)
	defer func() {
		if resultErr != nil {
			ddlHandler.Close()
		}
	}()

	existingTables := make(map[model.TableID]model.Ts)
	for captureID, taskStatus := range processorsInfos {
		var checkpointTs uint64
		if pos, exist := taskPositions[captureID]; exist {
			checkpointTs = pos.CheckPointTs
		}
		for tableID, replicaInfo := range taskStatus.Tables {
			if replicaInfo.StartTs > checkpointTs {
				checkpointTs = replicaInfo.StartTs
			}
			existingTables[tableID] = checkpointTs
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if resultErr != nil {
			cancel()
		}
	}()
	schemas := make(map[model.SchemaID]tableIDMap)
	tables := make(map[model.TableID]model.TableName)
	partitions := make(map[model.TableID][]int64)
	orphanTables := make(map[model.TableID]model.Ts)
	sinkTableInfo := make([]*model.SimpleTableInfo, len(schemaSnap.CloneTables()))
	j := 0
	for tid, table := range schemaSnap.CloneTables() {
		j++
		if filter.ShouldIgnoreTable(table.Schema, table.Table) {
			continue
		}
		if info.Config.Cyclic.IsEnabled() && mark.IsMarkTable(table.Schema, table.Table) {
			// skip the mark table if cyclic is enabled
			continue
		}

		tables[tid] = table
		schema, ok := schemaSnap.SchemaByTableID(tid)
		if !ok {
			log.Warn("schema not found for table", zap.Int64("tid", tid))
		} else {
			sid := schema.ID
			if _, ok := schemas[sid]; !ok {
				schemas[sid] = make(tableIDMap)
			}
			schemas[sid][tid] = struct{}{}
		}
		tblInfo, ok := schemaSnap.TableByID(tid)
		if !ok {
			log.Warn("table not found for table ID", zap.Int64("tid", tid))
			continue
		}
		if !tblInfo.IsEligible(info.Config.ForceReplicate) {
			log.Warn("skip ineligible table", zap.Int64("tid", tid), zap.Stringer("table", table))
			continue
		}
		// `existingTables` are tables dispatched to a processor, however the
		// capture that this processor belongs to could have crashed or exited.
		// So we check this before task dispatching, but after the update of
		// changefeed schema information.
		if ts, ok := existingTables[tid]; ok {
			log.Info("ignore known table", zap.Int64("tid", tid), zap.Stringer("table", table), zap.Uint64("ts", ts))
			continue
		}
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			delete(partitions, tid)
			for _, partition := range pi.Definitions {
				id := partition.ID
				partitions[tid] = append(partitions[tid], id)
				if ts, ok := existingTables[id]; ok {
					log.Info("ignore known table partition", zap.Int64("tid", tid), zap.Int64("partitionID", id), zap.Stringer("table", table), zap.Uint64("ts", ts))
					continue
				}
				orphanTables[id] = checkpointTs
			}
		} else {
			orphanTables[tid] = checkpointTs
		}

		sinkTableInfo[j-1] = new(model.SimpleTableInfo)
		sinkTableInfo[j-1].TableID = tid
		sinkTableInfo[j-1].ColumnInfo = make([]*model.ColumnInfo, len(tblInfo.Cols()))
		sinkTableInfo[j-1].Schema = table.Schema
		sinkTableInfo[j-1].Table = table.Table

		for i, colInfo := range tblInfo.Cols() {
			sinkTableInfo[j-1].ColumnInfo[i] = new(model.ColumnInfo)
			sinkTableInfo[j-1].ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

	}
	errCh := make(chan error, 1)

	primarySink, err := sink.NewSink(ctx, id, info.SinkURI, filter, info.Config, info.Opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if resultErr != nil && primarySink != nil {
			// The Close of backend sink here doesn't use context, it is ok to pass
			// a canceled context here.
			primarySink.Close(ctx)
		}
	}()
	go func() {
		var err error
		select {
		case <-ctx.Done():
		case err = <-errCh:
			cancel()
		}
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Error("error on running changefeed", zap.Error(err), zap.String("changefeed", id))
		} else {
			log.Info("changefeed exited", zap.String("changfeed", id))
		}
	}()

	err = primarySink.Initialize(ctx, sinkTableInfo)
	if err != nil {
		log.Error("error on running owner", zap.Error(err))
	}

	var syncpointStore sink.SyncpointStore
	if info.SyncPointEnabled {
		syncpointStore, err = sink.NewSyncpointStore(ctx, id, info.SinkURI)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	cf = &changeFeed{
		info:          info,
		id:            id,
		ddlHandler:    ddlHandler,
		schema:        schemaSnap,
		schemas:       schemas,
		tables:        tables,
		partitions:    partitions,
		orphanTables:  orphanTables,
		toCleanTables: make(map[model.TableID]model.Ts),
		status: &model.ChangeFeedStatus{
			ResolvedTs:   0,
			CheckpointTs: checkpointTs,
		},
		appliedCheckpointTs: checkpointTs,
		scheduler:           scheduler.NewScheduler(info.Config.Scheduler.Tp),
		ddlState:            model.ChangeFeedSyncDML,
		ddlExecutedTs:       checkpointTs,
		targetTs:            info.GetTargetTs(),
		ddlTs:               0,
		updateResolvedTs:    true,
		startTimer:          make(chan bool),
		syncpointStore:      syncpointStore,
		syncCancel:          nil,
		taskStatus:          processorsInfos,
		taskPositions:       taskPositions,
		etcdCli:             o.etcdClient,
		leaseID:             o.session.Lease(),
		filter:              filter,
		sink:                primarySink,
		cyclicEnabled:       info.Config.Cyclic.IsEnabled(),
		lastRebalanceTime:   time.Now(),
		cancel:              cancel,
	}
	return cf, nil
}

// This is a compatibility hack between v4.0.0 and v4.0.1
// This function will try to decode the task status, if that throw a unmarshal error,
// it will remove the invalid task status
func (o *Owner) checkAndCleanTasksInfo(ctx context.Context) error {
	_, details, err := o.cfRWriter.GetChangeFeeds(ctx)
	if err != nil {
		return err
	}
	cleaned := false
	for changefeedID := range details {
		_, err := o.cfRWriter.GetAllTaskStatus(ctx, changefeedID)
		if err != nil {
			if cerror.ErrDecodeFailed.NotEqual(err) {
				return errors.Trace(err)
			}
			err := o.cfRWriter.LeaseGuardRemoveAllTaskStatus(ctx, changefeedID, o.session.Lease())
			if err != nil {
				return errors.Trace(err)
			}
			cleaned = true
		}
	}
	if cleaned {
		log.Warn("the task status is outdated, clean them")
	}
	return nil
}

func (o *Owner) loadChangeFeeds(ctx context.Context) error {
	_, details, err := o.cfRWriter.GetChangeFeeds(ctx)
	if err != nil {
		return err
	}
	errorFeeds := make(map[model.ChangeFeedID]*model.RunningError)
	for changeFeedID, cfInfoRawValue := range details {
		taskStatus, err := o.cfRWriter.GetAllTaskStatus(ctx, changeFeedID)
		if err != nil {
			return err
		}
		taskPositions, err := o.cfRWriter.GetAllTaskPositions(ctx, changeFeedID)
		if err != nil {
			return err
		}
		if cf, exist := o.changeFeeds[changeFeedID]; exist {
			cf.updateProcessorInfos(taskStatus, taskPositions)
			for _, pos := range taskPositions {
				// TODO: only record error of one capture,
				// is it necessary to record all captures' error
				if pos.Error != nil {
					errorFeeds[changeFeedID] = pos.Error
					break
				}
			}
			continue
		}

		// we find a new changefeed, init changefeed here.
		cfInfo := &model.ChangeFeedInfo{}
		err = cfInfo.Unmarshal(cfInfoRawValue.Value)
		if err != nil {
			return err
		}
		if cfInfo.State == model.StateFailed {
			if _, ok := o.failInitFeeds[changeFeedID]; ok {
				continue
			}
			log.Warn("changefeed is not in normal state", zap.String("changefeed", changeFeedID))
			o.failInitFeeds[changeFeedID] = struct{}{}
			continue
		}
		if _, ok := o.failInitFeeds[changeFeedID]; ok {
			log.Info("changefeed recovered from failure", zap.String("changefeed", changeFeedID))
			delete(o.failInitFeeds, changeFeedID)
		}
		needSave, canInit := cfInfo.CheckErrorHistory()
		if needSave {
			err := o.etcdClient.LeaseGuardSaveChangeFeedInfo(ctx, cfInfo, changeFeedID, o.session.Lease())
			if err != nil {
				return err
			}
		}
		if !canInit {
			// avoid too many logs here
			if time.Now().Unix()%60 == 0 {
				log.Warn("changefeed fails reach rate limit, try to initialize it later", zap.Int64s("history", cfInfo.ErrorHis))
			}
			continue
		}
		err = cfInfo.VerifyAndFix()
		if err != nil {
			return err
		}

		status, _, err := o.cfRWriter.GetChangeFeedStatus(ctx, changeFeedID)
		if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
			return err
		}
		if status != nil && status.AdminJobType.IsStopState() {
			if status.AdminJobType == model.AdminStop {
				if _, ok := o.stoppedFeeds[changeFeedID]; !ok {
					o.stoppedFeeds[changeFeedID] = status
				}
			}
			continue
		}

		// remaining task status means some processors are not exited, wait until
		// all these statuses cleaned. If the capture of pending processor loses
		// etcd session, the cleanUpStaleTasks will clean these statuses later.
		allMetadataCleaned := true
		allTaskStatus, err := o.etcdClient.GetAllTaskStatus(ctx, changeFeedID)
		if err != nil {
			return err
		}
		for _, taskStatus := range allTaskStatus {
			if taskStatus.AdminJobType == model.AdminStop || taskStatus.AdminJobType == model.AdminRemove {
				log.Info("stale task status is not deleted, wait metadata cleaned to create new changefeed",
					zap.Reflect("task status", taskStatus), zap.String("changefeed", changeFeedID))
				allMetadataCleaned = false
				break
			}
		}
		if !allMetadataCleaned {
			continue
		}

		checkpointTs := cfInfo.GetCheckpointTs(status)

		newCf, err := o.newChangeFeed(ctx, changeFeedID, taskStatus, taskPositions, cfInfo, checkpointTs)
		if err != nil {
			cfInfo.Error = &model.RunningError{
				Addr:    util.CaptureAddrFromCtx(ctx),
				Code:    "CDC-owner-1001",
				Message: err.Error(),
			}
			cfInfo.ErrorHis = append(cfInfo.ErrorHis, time.Now().UnixNano()/1e6)

			if cerror.ChangefeedFastFailError(err) {
				log.Error("create changefeed with fast fail error, mark changefeed as failed",
					zap.Error(err), zap.String("changefeed", changeFeedID))
				cfInfo.State = model.StateFailed
				err := o.etcdClient.LeaseGuardSaveChangeFeedInfo(ctx, cfInfo, changeFeedID, o.session.Lease())
				if err != nil {
					return err
				}
				continue
			}

			err2 := o.etcdClient.LeaseGuardSaveChangeFeedInfo(ctx, cfInfo, changeFeedID, o.session.Lease())
			if err2 != nil {
				return err2
			}
			// changefeed error has been recorded in etcd, log error here and
			// don't need to return an error.
			log.Warn("create changefeed failed, retry later",
				zap.String("changefeed", changeFeedID), zap.Error(err))
			continue
		}

		if newCf.info.SyncPointEnabled {
			log.Info("syncpoint is on, creating the sync table")
			// create the sync table
			err := newCf.syncpointStore.CreateSynctable(ctx)
			if err != nil {
				return err
			}
			newCf.startSyncPointTicker(ctx, newCf.info.SyncPointInterval)
		} else {
			log.Info("syncpoint is off")
		}

		o.changeFeeds[changeFeedID] = newCf
		delete(o.stoppedFeeds, changeFeedID)
	}
	o.adminJobsLock.Lock()
	for cfID, err := range errorFeeds {
		job := model.AdminJob{
			CfID:  cfID,
			Type:  model.AdminStop,
			Error: err,
		}
		o.adminJobs = append(o.adminJobs, job)
	}
	o.adminJobsLock.Unlock()
	return nil
}

func (o *Owner) balanceTables(ctx context.Context) error {
	rebalanceForAllChangefeed := false
	o.rebalanceMu.Lock()
	if o.rebalanceForAllChangefeed {
		rebalanceForAllChangefeed = true
		o.rebalanceForAllChangefeed = false
	}
	o.rebalanceMu.Unlock()
	for id, changefeed := range o.changeFeeds {
		rebalanceNow := false
		var scheduleCommands []*model.MoveTableJob
		o.rebalanceMu.Lock()
		if r, exist := o.rebalanceTigger[id]; exist {
			rebalanceNow = r
			delete(o.rebalanceTigger, id)
		}
		if rebalanceForAllChangefeed {
			rebalanceNow = true
		}
		if c, exist := o.manualScheduleCommand[id]; exist {
			scheduleCommands = c
			delete(o.manualScheduleCommand, id)
		}
		o.rebalanceMu.Unlock()
		err := changefeed.tryBalance(ctx, o.captures, rebalanceNow, scheduleCommands)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (o *Owner) flushChangeFeedInfos(ctx context.Context) error {
	// no running or stopped changefeed, clear gc safepoint.
	if len(o.changeFeeds) == 0 && len(o.stoppedFeeds) == 0 {
		if !o.gcSafepointLastUpdate.IsZero() {
			log.Info("clean service safe point", zap.String("service-id", CDCServiceSafePointID))
			_, err := o.pdClient.UpdateServiceGCSafePoint(ctx, CDCServiceSafePointID, 0, 0)
			if err != nil {
				log.Warn("failed to update service safe point", zap.Error(err))
			} else {
				o.gcSafepointLastUpdate = time.Time{}
			}
		}
		return nil
	}

	staleChangeFeeds := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, len(o.changeFeeds))
	gcSafePoint := uint64(math.MaxUint64)

	// get the lower bound of gcSafePoint
	minGCSafePoint := o.getMinGCSafePointCache(ctx)

	if len(o.changeFeeds) > 0 {
		snapshot := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, len(o.changeFeeds))
		for id, changefeed := range o.changeFeeds {
			snapshot[id] = changefeed.status
			if changefeed.status.CheckpointTs < gcSafePoint {
				gcSafePoint = changefeed.status.CheckpointTs
			}
			// 1. If changefeed's appliedCheckpoinTs <= minGCSafePoint, it means this changefeed is stagnant.
			// They are collected into this map, and then handleStaleChangeFeed() is called to deal with these stagnant changefeed.
			// A changefeed will not enter the map twice, because in run(),
			// handleAdminJob() will always be executed before flushChangeFeedInfos(),
			// ensuring that the previous changefeed in staleChangeFeeds has been stopped and removed from o.changeFeeds.
			// 2. We need the `<=` check here is because when a changefeed is stagnant, its checkpointTs will be updated to pd,
			// and it would be the minimum gcSafePoint across all services.
			// So as described above(line 92) minGCSafePoint = gcSafePoint = CheckpointTs would happens.
			// In this case, if we check `<` here , this changefeed will not be put into staleChangeFeeds, and its checkpoints will be updated to pd again and again.
			// This will cause the cdc's gcSafePoint never advance.
			// If we check `<=` here, when we encounter the changefeed again, we will put it into staleChangeFeeds.
			if changefeed.status.CheckpointTs <= minGCSafePoint {
				staleChangeFeeds[id] = changefeed.status
			}

			phyTs := oracle.ExtractPhysical(changefeed.status.CheckpointTs)
			changefeedCheckpointTsGauge.WithLabelValues(id).Set(float64(phyTs))
			// It is more accurate to get tso from PD, but in most cases we have
			// deployed NTP service, a little bias is acceptable here.
			changefeedCheckpointTsLagGauge.WithLabelValues(id).Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)
		}
		if time.Since(o.lastFlushChangefeeds) > o.flushChangefeedInterval {
			err := o.cfRWriter.LeaseGuardPutAllChangeFeedStatus(ctx, snapshot, o.session.Lease())
			if err != nil {
				return errors.Trace(err)
			}
			for id, changefeedStatus := range snapshot {
				o.changeFeeds[id].appliedCheckpointTs = changefeedStatus.CheckpointTs
			}
			o.lastFlushChangefeeds = time.Now()
		}
	}

	for _, status := range o.stoppedFeeds {
		// If a stopped changefeed's CheckpoinTs <= minGCSafePoint, means this changefeed is stagnant.
		// It should never be resumed. This part of the logic is in newChangeFeed()
		// So here we can skip it.
		if status.CheckpointTs <= minGCSafePoint {
			continue
		}

		if status.CheckpointTs < gcSafePoint {
			gcSafePoint = status.CheckpointTs
		}
	}

	// handle stagnant changefeed collected above
	err := o.handleStaleChangeFeed(ctx, staleChangeFeeds, minGCSafePoint)
	if err != nil {
		log.Warn("failed to handleStaleChangeFeed ", zap.Error(err))
	}

	if time.Since(o.gcSafepointLastUpdate) > GCSafepointUpdateInterval {
		actual, err := o.pdClient.UpdateServiceGCSafePoint(ctx, CDCServiceSafePointID, o.gcTTL, gcSafePoint)
		if err != nil {
			sinceLastUpdate := time.Since(o.gcSafepointLastUpdate)
			log.Warn("failed to update service safe point", zap.Error(err),
				zap.Duration("since-last-update", sinceLastUpdate))
			// We do not throw an error unless updating GC safepoint has been failing for more than gcTTL.
			if sinceLastUpdate >= time.Second*time.Duration(o.gcTTL) {
				return cerror.ErrUpdateServiceSafepointFailed.Wrap(err)
			}
		} else {
			o.pdGCSafePoint = actual
			o.gcSafepointLastUpdate = time.Now()
		}

		failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
			actual = uint64(val.(int))
		})

		if actual > gcSafePoint {
			// UpdateServiceGCSafePoint has failed.
			log.Warn("updating an outdated service safe point", zap.Uint64("checkpoint-ts", gcSafePoint), zap.Uint64("actual-safepoint", actual))

			for cfID, cf := range o.changeFeeds {
				if cf.status.CheckpointTs < actual {
					runningError := &model.RunningError{
						Addr:    util.CaptureAddrFromCtx(ctx),
						Code:    "CDC-owner-1001",
						Message: cerror.ErrServiceSafepointLost.GenWithStackByArgs(actual).Error(),
					}

					err := o.EnqueueJob(model.AdminJob{
						CfID:  cfID,
						Type:  model.AdminStop,
						Error: runningError,
					})
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	}
	return nil
}

// calcResolvedTs call calcResolvedTs of every changefeeds
func (o *Owner) calcResolvedTs(ctx context.Context) error {
	for id, cf := range o.changeFeeds {
		if err := cf.calcResolvedTs(ctx); err != nil {
			log.Error("fail to calculate checkpoint ts, so it will be stopped", zap.String("changefeed", cf.id), zap.Error(err))
			// error may cause by sink.EmitCheckpointTs`, just stop the changefeed at the moment
			// todo: make the method mentioned above more robust.
			var code string
			if rfcCode, ok := cerror.RFCCode(err); ok {
				code = string(rfcCode)
			} else {
				code = string(cerror.ErrOwnerUnknown.RFCCode())
			}

			job := model.AdminJob{
				CfID: id,
				Type: model.AdminStop,
				Error: &model.RunningError{
					Addr:    util.CaptureAddrFromCtx(ctx),
					Code:    code,
					Message: err.Error(),
				},
			}

			if err := o.EnqueueJob(job); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// handleDDL call handleDDL of every changefeeds
func (o *Owner) handleDDL(ctx context.Context) error {
	for _, cf := range o.changeFeeds {
		err := cf.handleDDL(ctx)
		if err != nil {
			var code string
			if terror, ok := err.(*errors.Error); ok {
				code = string(terror.RFCCode())
			} else {
				code = string(cerror.ErrExecDDLFailed.RFCCode())
			}
			err = o.EnqueueJob(model.AdminJob{
				CfID: cf.id,
				Type: model.AdminStop,
				Error: &model.RunningError{
					Addr:    util.CaptureAddrFromCtx(ctx),
					Code:    code,
					Message: err.Error(),
				},
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// handleSyncPoint call handleSyncPoint of every changefeeds
func (o *Owner) handleSyncPoint(ctx context.Context) error {
	for _, cf := range o.changeFeeds {
		if err := cf.handleSyncPoint(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// dispatchJob dispatches job to processors
// Note job type in this function contains pause, remove and finish
func (o *Owner) dispatchJob(ctx context.Context, job model.AdminJob) error {
	cf, ok := o.changeFeeds[job.CfID]
	if !ok {
		return cerror.ErrOwnerChangefeedNotFound.GenWithStackByArgs(job.CfID)
	}
	for captureID := range cf.taskStatus {
		newStatus, _, err := cf.etcdCli.LeaseGuardAtomicPutTaskStatus(
			ctx, cf.id, captureID, o.session.Lease(),
			func(modRevision int64, taskStatus *model.TaskStatus) (bool, error) {
				taskStatus.AdminJobType = job.Type
				return true, nil
			},
		)
		if err != nil {
			return errors.Trace(err)
		}
		cf.taskStatus[captureID] = newStatus.Clone()
	}
	// record admin job in changefeed status
	cf.status.AdminJobType = job.Type
	infos := map[model.ChangeFeedID]*model.ChangeFeedStatus{job.CfID: cf.status}
	err := o.cfRWriter.LeaseGuardPutAllChangeFeedStatus(ctx, infos, o.session.Lease())
	if err != nil {
		return errors.Trace(err)
	}
	cf.Close()
	// Only need to process stoppedFeeds with `AdminStop` command here.
	// For `AdminResume`, we remove stopped feed in changefeed initialization phase.
	// For `AdminRemove`, we need to update stoppedFeeds when removing a stopped changefeed.
	if job.Type == model.AdminStop {
		log.Debug("put changefeed into stoppedFeeds queue", zap.String("changefeed", job.CfID))
		o.stoppedFeeds[job.CfID] = cf.status
	}
	for captureID := range cf.taskStatus {
		capture, ok := o.captures[captureID]
		if !ok {
			log.Warn("capture not found", zap.String("capture-id", captureID))
			continue
		}
		ownerMaintainTableNumGauge.DeleteLabelValues(cf.id, capture.AdvertiseAddr, maintainTableTypeTotal)
		ownerMaintainTableNumGauge.DeleteLabelValues(cf.id, capture.AdvertiseAddr, maintainTableTypeWip)
	}
	delete(o.changeFeeds, job.CfID)
	return nil
}

func (o *Owner) collectChangefeedInfo(ctx context.Context, cid model.ChangeFeedID) (
	cf *changeFeed,
	status *model.ChangeFeedStatus,
	feedState model.FeedState,
	err error,
) {
	var ok bool
	cf, ok = o.changeFeeds[cid]
	if ok {
		return cf, cf.status, cf.info.State, nil
	}
	feedState = model.StateNormal

	var cfInfo *model.ChangeFeedInfo
	cfInfo, err = o.etcdClient.GetChangeFeedInfo(ctx, cid)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return
	}

	status, _, err = o.etcdClient.GetChangeFeedStatus(ctx, cid)
	if err != nil {
		if cerror.ErrChangeFeedNotExists.Equal(err) {
			// Only changefeed info exists and error field is not nil means
			// the changefeed has met error, mark it as failed.
			if cfInfo != nil && cfInfo.Error != nil {
				feedState = model.StateFailed
			}
		}
		return
	}
	switch status.AdminJobType {
	case model.AdminNone, model.AdminResume:
		if cfInfo != nil && cfInfo.Error != nil {
			feedState = model.StateFailed
		}
	case model.AdminStop:
		feedState = model.StateStopped
	case model.AdminRemove:
		feedState = model.StateRemoved
	case model.AdminFinish:
		feedState = model.StateFinished
	}
	return
}

func (o *Owner) checkClusterHealth(_ context.Context) error {
	// check whether a changefeed has finished by comparing checkpoint-ts and target-ts
	for _, cf := range o.changeFeeds {
		if cf.status.CheckpointTs == cf.info.GetTargetTs() {
			log.Info("changefeed replication finished", zap.String("changefeed", cf.id), zap.Uint64("checkpointTs", cf.status.CheckpointTs))
			err := o.EnqueueJob(model.AdminJob{
				CfID: cf.id,
				Type: model.AdminFinish,
			})
			if err != nil {
				return err
			}
		}
	}
	for _, cf := range o.changeFeeds {
		for captureID, pinfo := range cf.taskStatus {
			capture, ok := o.captures[captureID]
			if !ok {
				log.Warn("capture not found", zap.String("capture-id", captureID))
				continue
			}
			ownerMaintainTableNumGauge.WithLabelValues(cf.id, capture.AdvertiseAddr, maintainTableTypeTotal).Set(float64(len(pinfo.Tables)))
			ownerMaintainTableNumGauge.WithLabelValues(cf.id, capture.AdvertiseAddr, maintainTableTypeWip).Set(float64(len(pinfo.Operation)))
		}
	}
	// TODO: check processor normal exited
	return nil
}

func (o *Owner) handleAdminJob(ctx context.Context) error {
	removeIdx := 0
	o.adminJobsLock.Lock()
	defer func() {
		o.adminJobs = o.adminJobs[removeIdx:]
		o.adminJobsLock.Unlock()
	}()
	for i, job := range o.adminJobs {
		log.Info("handle admin job", zap.String("changefeed", job.CfID), zap.Stringer("type", job.Type))
		removeIdx = i + 1

		cf, status, feedState, err := o.collectChangefeedInfo(ctx, job.CfID)
		if err != nil {
			if cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			if feedState == model.StateFailed && job.Type == model.AdminRemove {
				// changefeed in failed state, but changefeed status has not
				// been created yet. Try to remove changefeed info only.
				err := o.etcdClient.LeaseGuardDeleteChangeFeedInfo(ctx, job.CfID, o.session.Lease())
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				log.Warn("invalid admin job, changefeed status not found", zap.String("changefeed", job.CfID))
			}
			continue
		}
		switch job.Type {
		case model.AdminStop:
			switch feedState {
			case model.StateStopped:
				log.Info("changefeed has been stopped, pause command will do nothing")
				continue
			case model.StateRemoved:
				log.Info("changefeed has been removed, pause command will do nothing")
				continue
			case model.StateFinished:
				log.Info("changefeed has finished, pause command will do nothing")
				continue
			}
			if cf == nil {
				log.Warn("invalid admin job, changefeed not found", zap.String("changefeed", job.CfID))
				continue
			}

			cf.info.AdminJobType = model.AdminStop
			cf.info.Error = job.Error
			if job.Error != nil {
				cf.info.ErrorHis = append(cf.info.ErrorHis, time.Now().UnixNano()/1e6)
			}

			err := o.etcdClient.LeaseGuardSaveChangeFeedInfo(ctx, cf.info, job.CfID, o.session.Lease())
			if err != nil {
				return errors.Trace(err)
			}
			err = o.dispatchJob(ctx, job)
			if err != nil {
				return errors.Trace(err)
			}
			cf.stopSyncPointTicker()
		case model.AdminRemove, model.AdminFinish:
			if cf != nil {
				cf.stopSyncPointTicker()
				err := o.dispatchJob(ctx, job)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				switch feedState {
				case model.StateRemoved, model.StateFinished:
					// remove a removed or finished changefeed
					if job.Opts != nil && job.Opts.ForceRemove {
						err := o.etcdClient.LeaseGuardRemoveChangeFeedStatus(ctx, job.CfID, o.session.Lease())
						if err != nil {
							return errors.Trace(err)
						}
					} else {
						log.Info("changefeed has been removed or finished, remove command will do nothing")
					}
					continue
				case model.StateStopped, model.StateFailed:
					// remove a paused or failed changefeed
					status.AdminJobType = model.AdminRemove
					err = o.etcdClient.LeaseGuardPutChangeFeedStatus(ctx, job.CfID, status, o.session.Lease())
					if err != nil {
						return errors.Trace(err)
					}
					delete(o.stoppedFeeds, job.CfID)
				default:
					return cerror.ErrChangefeedAbnormalState.GenWithStackByArgs(feedState, status)
				}
			}
			// remove changefeed info
			err := o.etcdClient.DeleteChangeFeedInfo(ctx, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}
			if job.Opts != nil && job.Opts.ForceRemove {
				// if `ForceRemove` is enabled, remove all information related to this changefeed
				err := o.etcdClient.LeaseGuardRemoveChangeFeedStatus(ctx, job.CfID, o.session.Lease())
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				// set ttl to changefeed status
				err = o.etcdClient.SetChangeFeedStatusTTL(ctx, job.CfID, 24*3600 /*24 hours*/)
				if err != nil {
					return errors.Trace(err)
				}
			}
		case model.AdminResume:
			// resume changefeed must read checkpoint from ChangeFeedStatus
			if cerror.ErrChangeFeedNotExists.Equal(err) {
				log.Warn("invalid admin job, changefeed not found", zap.String("changefeed", job.CfID))
				continue
			}
			if feedState == model.StateRemoved || feedState == model.StateFinished {
				log.Info("changefeed has been removed or finished, cannot be resumed anymore")
				continue
			}
			cfInfo, err := o.etcdClient.GetChangeFeedInfo(ctx, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}

			// set admin job in changefeed status to tell owner resume changefeed
			status.AdminJobType = model.AdminResume
			err = o.etcdClient.LeaseGuardPutChangeFeedStatus(ctx, job.CfID, status, o.session.Lease())
			if err != nil {
				return errors.Trace(err)
			}

			// set admin job in changefeed cfInfo to trigger each capture's changefeed list watch event
			cfInfo.AdminJobType = model.AdminResume
			// clear last running error
			cfInfo.State = model.StateNormal
			cfInfo.Error = nil
			err = o.etcdClient.LeaseGuardSaveChangeFeedInfo(ctx, cfInfo, job.CfID, o.session.Lease())
			if err != nil {
				return errors.Trace(err)
			}
			if config.NewReplicaImpl {
				// remove all positions because the old positions may be include an error
				err = o.etcdClient.RemoveAllTaskPositions(ctx, job.CfID)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		// TODO: we need a better admin job workflow. Supposing uses create
		// multiple admin jobs to a specific changefeed at the same time, such
		// as pause -> resume -> pause, should the one job handler waits for
		// the previous job finished? However it is difficult to distinguish
		// whether a job is totally finished in some cases, for example when
		// resuming a changefeed, seems we should mark the job finished if all
		// processors have started. Currently the owner only processes one
		// admin job in each tick loop as a workaround.
		break
	}
	return nil
}

func (o *Owner) throne(ctx context.Context) error {
	// Start a routine to keep watching on the liveness of
	// captures.
	o.startCaptureWatcher(ctx)
	return nil
}

// Close stops a running owner
func (o *Owner) Close(ctx context.Context, stepDown func(ctx context.Context) error) {
	// stepDown is called after exiting the main loop by the owner, it is useful
	// to clean up some resource, like dropping the leader key.
	o.stepDown = stepDown

	// Close and Run should be in separated goroutines
	// A channel is used here to synchronize the steps.

	// Single the Run function to exit
	select {
	case o.done <- struct{}{}:
	case <-ctx.Done():
	}

	// Wait until it exited
	select {
	case <-o.done:
	case <-ctx.Done():
	}
}

// Run the owner
// TODO avoid this tick style, this means we get `tickTime` latency here.
func (o *Owner) Run(ctx context.Context, tickTime time.Duration) error {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(errors.New("owner run with injected error"))
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		if err := o.watchCampaignKey(ctx); err != nil {
			cancel()
		}
	}()

	if err := o.throne(ctx); err != nil {
		return err
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	feedChangeReceiver, err := o.feedChangeNotifier.NewReceiver(tickTime)
	if err != nil {
		return err
	}
	defer feedChangeReceiver.Stop()
	o.watchFeedChange(ctx1)

	ownership := newOwnership(tickTime)
loop:
	for {
		select {
		case <-o.done:
			close(o.done)
			break loop
		case <-ctx.Done():
			// FIXME: cancel the context doesn't ensure all resources are destructed, is it reasonable?
			// Anyway we just break loop here to ensure the following destruction.
			err = ctx.Err()
			break loop
		case <-feedChangeReceiver.C:
			ownership.inc()
		}

		err = o.run(ctx)
		if err != nil {
			switch errors.Cause(err) {
			case context.DeadlineExceeded:
				// context timeout means the o.run doesn't finish in a safe owner
				// lease cycle, it is safe to retry. If the lease is revoked,
				// another run loop will detect it.
				continue loop
			case context.Canceled:
			default:
				log.Error("owner exited with error", zap.Error(err))
			}
			break loop
		}
	}
	for _, cf := range o.changeFeeds {
		cf.Close()
		changefeedCheckpointTsGauge.DeleteLabelValues(cf.id)
		changefeedCheckpointTsLagGauge.DeleteLabelValues(cf.id)
	}
	if o.stepDown != nil {
		if err := o.stepDown(ctx); err != nil {
			return err
		}
	}

	return err
}

// watchCampaignKey watches the aliveness of campaign owner key in etcd
func (o *Owner) watchCampaignKey(ctx context.Context) error {
	key := fmt.Sprintf("%s/%x", kv.CaptureOwnerKey, o.session.Lease())
restart:
	resp, err := o.etcdClient.Client.Get(ctx, key)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return cerror.ErrOwnerCampaignKeyDeleted.GenWithStackByArgs()
	}
	// watch the key change from the next revision relatived to the current
	wch := o.etcdClient.Client.Watch(ctx, key, clientv3.WithRev(resp.Header.Revision+1))
	for resp := range wch {
		err := resp.Err()
		if err != nil {
			if err != mvcc.ErrCompacted {
				log.Error("watch owner campaign key failed, restart the watcher", zap.Error(err))
			}
			goto restart
		}
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				log.Warn("owner campaign key deleted", zap.String("key", key))
				return cerror.ErrOwnerCampaignKeyDeleted.GenWithStackByArgs()
			}
		}
	}
	return nil
}

func (o *Owner) watchFeedChange(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			cctx, cancel := context.WithCancel(ctx)
			wch := o.etcdClient.Client.Watch(cctx, kv.TaskPositionKeyPrefix, clientv3.WithFilterDelete(), clientv3.WithPrefix())

			for resp := range wch {
				if resp.Err() != nil {
					log.Error("position watcher restarted with error", zap.Error(resp.Err()))
					break
				}

				// TODO: because the main loop has many serial steps, it is hard to do a partial update without change
				// majority logical. For now just to wakeup the main loop ASAP to reduce latency, the efficiency of etcd
				// operations should be resolved in future release.

				o.feedChangeNotifier.Notify()
			}
			cancel()
		}
	}()
}

func (o *Owner) run(ctx context.Context) error {
	// captureLoaded == 0 means capture information is not built, owner can't
	// run normal jobs now.
	if atomic.LoadInt32(&o.captureLoaded) == int32(0) {
		return nil
	}

	o.l.Lock()
	defer o.l.Unlock()

	var err error

	err = o.cleanUpStaleTasks(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.loadChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.balanceTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleDDL(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleSyncPoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleAdminJob(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.calcResolvedTs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// It is better for flushChangeFeedInfos to follow calcResolvedTs immediately,
	// because operations such as handleDDL and rebalancing rely on proper progress of the checkpoint in Etcd.
	err = o.flushChangeFeedInfos(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.checkClusterHealth(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// EnqueueJob adds an admin job
func (o *Owner) EnqueueJob(job model.AdminJob) error {
	switch job.Type {
	case model.AdminResume, model.AdminRemove, model.AdminStop, model.AdminFinish:
	default:
		return cerror.ErrInvalidAdminJobType.GenWithStackByArgs(job.Type)
	}
	o.adminJobsLock.Lock()
	o.adminJobs = append(o.adminJobs, job)
	o.adminJobsLock.Unlock()
	return nil
}

// TriggerRebalance triggers the rebalance in the specified changefeed
func (o *Owner) TriggerRebalance(changefeedID model.ChangeFeedID) {
	o.rebalanceMu.Lock()
	defer o.rebalanceMu.Unlock()
	o.rebalanceTigger[changefeedID] = true
	// TODO(leoppro) throw an error if the changefeed is not exist
}

// ManualSchedule moves the table from a capture to another capture
func (o *Owner) ManualSchedule(changefeedID model.ChangeFeedID, to model.CaptureID, tableID model.TableID) {
	o.rebalanceMu.Lock()
	defer o.rebalanceMu.Unlock()
	o.manualScheduleCommand[changefeedID] = append(o.manualScheduleCommand[changefeedID], &model.MoveTableJob{
		To:      to,
		TableID: tableID,
	})
}

func (o *Owner) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "** active changefeeds **:\n")
	for _, info := range o.changeFeeds {
		fmt.Fprintf(w, "%s\n", info)
	}
	fmt.Fprintf(w, "** stopped changefeeds **:\n")
	for _, feedStatus := range o.stoppedFeeds {
		fmt.Fprintf(w, "%+v\n", *feedStatus)
	}
	fmt.Fprintf(w, "\n** captures **:\n")
	for _, capture := range o.captures {
		fmt.Fprintf(w, "%+v\n", *capture)
	}
}

// cleanUpStaleTasks cleans up the task status which does not associated
// with an active processor. This function is not thread safe.
//
// When a new owner is elected, it does not know the events occurs before, like
// processor deletion. In this case, the new owner should check if the task
// status is stale because of the processor deletion.
func (o *Owner) cleanUpStaleTasks(ctx context.Context) error {
	_, changefeeds, err := o.etcdClient.GetChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for changeFeedID := range changefeeds {
		statuses, err := o.etcdClient.GetAllTaskStatus(ctx, changeFeedID)
		if err != nil {
			return errors.Trace(err)
		}
		positions, err := o.etcdClient.GetAllTaskPositions(ctx, changeFeedID)
		if err != nil {
			return errors.Trace(err)
		}
		workloads, err := o.etcdClient.GetAllTaskWorkloads(ctx, changeFeedID)
		if err != nil {
			return errors.Trace(err)
		}
		// in most cases statuses and positions have the same keys, or positions
		// are more than statuses, as we always delete task status first.
		captureIDs := make(map[string]struct{}, len(statuses))
		for captureID := range statuses {
			captureIDs[captureID] = struct{}{}
		}
		for captureID := range positions {
			captureIDs[captureID] = struct{}{}
		}
		for captureID := range workloads {
			captureIDs[captureID] = struct{}{}
		}

		log.Debug("cleanUpStaleTasks",
			zap.Reflect("statuses", statuses),
			zap.Reflect("positions", positions),
			zap.Reflect("workloads", workloads))

		for captureID := range captureIDs {
			if _, ok := o.captures[captureID]; !ok {
				status, ok1 := statuses[captureID]
				if ok1 {
					pos, taskPosFound := positions[captureID]
					if !taskPosFound {
						log.Warn("task position not found, fallback to use original start ts",
							zap.String("capture", captureID),
							zap.String("changefeed", changeFeedID),
							zap.Reflect("task status", status),
						)
					}
					for tableID, replicaInfo := range status.Tables {
						startTs := replicaInfo.StartTs
						if taskPosFound {
							if startTs < pos.CheckPointTs {
								startTs = pos.CheckPointTs
							}
						}
						o.addOrphanTable(changeFeedID, tableID, startTs)
					}
					if cf, ok := o.changeFeeds[changeFeedID]; ok {
						o.rebuildTableFromOperations(cf, status, cf.status.CheckpointTs)
					}
				}

				if err := o.etcdClient.LeaseGuardDeleteTaskStatus(ctx, changeFeedID, captureID, o.session.Lease()); err != nil {
					return errors.Trace(err)
				}
				if err := o.etcdClient.LeaseGuardDeleteTaskPosition(ctx, changeFeedID, captureID, o.session.Lease()); err != nil {
					return errors.Trace(err)
				}
				if err := o.etcdClient.LeaseGuardDeleteTaskWorkload(ctx, changeFeedID, captureID, o.session.Lease()); err != nil {
					return errors.Trace(err)
				}
				log.Info("cleanup stale task", zap.String("capture-id", captureID), zap.String("changefeed", changeFeedID))
			}
		}
	}
	return nil
}

func (o *Owner) watchCapture(ctx context.Context) error {
	ctx = clientv3.WithRequireLeader(ctx)

	failpoint.Inject("sleep-before-watch-capture", nil)

	// When an owner just starts, changefeed information is not updated at once.
	// Supposing a crashed capture should be removed now, the owner will miss deleting
	// task status and task position if changefeed information is not loaded.
	// If the task positions and status decode failed, remove them.
	if err := o.checkAndCleanTasksInfo(ctx); err != nil {
		return errors.Trace(err)
	}
	o.l.Lock()
	if err := o.loadChangeFeeds(ctx); err != nil {
		o.l.Unlock()
		return errors.Trace(err)
	}
	o.l.Unlock()

	rev, captureList, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	captures := make(map[model.CaptureID]*model.CaptureInfo)
	for _, c := range captureList {
		captures[c.ID] = c
	}
	// before watching, rebuild events according to
	// the existed captures. This is necessary because
	// the etcd events may be compacted.
	if err := o.rebuildCaptureEvents(ctx, captures); err != nil {
		return errors.Trace(err)
	}

	log.Info("monitoring captures",
		zap.String("key", kv.CaptureInfoKeyPrefix),
		zap.Int64("rev", rev))
	ch := o.etcdClient.Client.Watch(ctx, kv.CaptureInfoKeyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(rev+1),
		clientv3.WithPrevKV())

	for resp := range ch {
		err := resp.Err()
		failpoint.Inject("restart-capture-watch", func() {
			err = mvcc.ErrCompacted
		})
		if err != nil {
			return cerror.WrapError(cerror.ErrOwnerEtcdWatch, resp.Err())
		}
		for _, ev := range resp.Events {
			c := &model.CaptureInfo{}
			switch ev.Type {
			case clientv3.EventTypeDelete:
				if err := c.Unmarshal(ev.PrevKv.Value); err != nil {
					return errors.Trace(err)
				}
				log.Info("delete capture",
					zap.String("capture-id", c.ID),
					zap.String("capture", c.AdvertiseAddr))
				o.removeCapture(ctx, c)
			case clientv3.EventTypePut:
				if !ev.IsCreate() {
					continue
				}
				if err := c.Unmarshal(ev.Kv.Value); err != nil {
					return errors.Trace(err)
				}
				log.Info("add capture",
					zap.String("capture-id", c.ID),
					zap.String("capture", c.AdvertiseAddr))
				o.addCapture(ctx, c)
			}
		}
	}
	return nil
}

func (o *Owner) rebuildCaptureEvents(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	for _, c := range captures {
		o.addCapture(ctx, c)
	}
	for _, c := range o.captures {
		if _, ok := captures[c.ID]; !ok {
			o.removeCapture(ctx, c)
		}
	}
	// captureLoaded is used to check whether the owner can execute cleanup stale tasks job.
	// Because at the very beginning of a new owner, it doesn't have capture information in
	// memory, cleanup stale tasks could have a false positive (where positive means owner
	// should cleanup the stale task of a specific capture). After the first time of capture
	// rebuild, even the etcd compaction and watch capture is rerun, we don't need to check
	// captureLoaded anymore because existing tasks must belong to a capture which is still
	// maintained in owner's memory.
	atomic.StoreInt32(&o.captureLoaded, 1)

	// clean up stale tasks each time before watch capture event starts,
	// for two reasons:
	// 1. when a new owner is elected, it must clean up stale task status and positions.
	// 2. when error happens in owner's capture event watch, the owner just resets
	//    the watch loop, with the following two steps:
	//    1) load all captures from PD, having a revision for data
	//	  2) start a new watch from revision in step1
	//    the step-2 may meet an error such as ErrCompacted, and we will continue
	//    from step-1, however other capture may crash just after step-2 returns
	//    and before step-1 starts, the longer time gap between step-2 to step-1,
	//    missing a crashed capture is more likely to happen.
	o.l.Lock()
	defer o.l.Unlock()
	return errors.Trace(o.cleanUpStaleTasks(ctx))
}

func (o *Owner) startCaptureWatcher(ctx context.Context) {
	log.Info("start to watch captures")
	go func() {
		rl := rate.NewLimiter(0.05, 2)
		for {
			err := rl.Wait(ctx)
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					return
				}
				log.Error("capture watcher wait limit token error", zap.Error(err))
				return
			}
			if err := o.watchCapture(ctx); err != nil {
				// When the watching routine returns, the error must not
				// be nil, it may be caused by a temporary error or a context
				// error(ctx.Err())
				if ctx.Err() != nil {
					if errors.Cause(ctx.Err()) != context.Canceled {
						// The context error indicates the termination of the owner
						log.Error("watch capture failed", zap.Error(ctx.Err()))
					} else {
						log.Info("watch capture exited")
					}
					return
				}
				log.Warn("watch capture returned", zap.Error(err))
				// Otherwise, a temporary error occurred(ErrCompact),
				// restart the watching routine.
			}
		}
	}()
}

// handle the StaleChangeFeed
// By setting the AdminJob type to AdminStop and the Error code to indicate that the changefeed is stagnant.
func (o *Owner) handleStaleChangeFeed(ctx context.Context, staleChangeFeeds map[model.ChangeFeedID]*model.ChangeFeedStatus, minGCSafePoint uint64) error {
	for id, status := range staleChangeFeeds {
		err := cerror.ErrSnapshotLostByGC.GenWithStackByArgs(status.CheckpointTs, minGCSafePoint)
		log.Warn("changefeed checkpoint is lagging too much, so it will be stopped.", zap.String("changefeed", id), zap.Error(err))
		runningError := &model.RunningError{
			Addr:    util.CaptureAddrFromCtx(ctx),
			Code:    string(cerror.ErrSnapshotLostByGC.RFCCode()), // changefeed is stagnant
			Message: err.Error(),
		}

		err = o.EnqueueJob(model.AdminJob{
			CfID:  id,
			Type:  model.AdminStop,
			Error: runningError,
		})
		if err != nil {
			return errors.Trace(err)
		}
		delete(staleChangeFeeds, id)
	}
	return nil
}
