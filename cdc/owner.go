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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

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
	credential  *security.Credential
	pdClient    pd.Client
	etcdClient  kv.CDCEtcdClient

	captures map[model.CaptureID]*model.CaptureInfo

	adminJobs     []model.AdminJob
	adminJobsLock sync.Mutex

	stepDown func(ctx context.Context) error

	// gcTTL is the ttl of cdc gc safepoint ttl.
	gcTTL int64
	// last update gc safepoint time. zero time means has not updated or cleared
	gcSafepointLastUpdate time.Time
}

const (
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	CDCServiceSafePointID = "ticdc"
	// GCSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
	GCSafepointUpdateInterval = time.Duration(2 * time.Second)
)

// NewOwner creates a new Owner instance
func NewOwner(pdClient pd.Client, credential *security.Credential, sess *concurrency.Session, gcTTL int64) (*Owner, error) {
	cli := kv.NewCDCEtcdClient(sess.Client())
	endpoints := sess.Client().Endpoints()

	owner := &Owner{
		done:                  make(chan struct{}),
		session:               sess,
		pdClient:              pdClient,
		credential:            credential,
		changeFeeds:           make(map[model.ChangeFeedID]*changeFeed),
		failInitFeeds:         make(map[model.ChangeFeedID]struct{}),
		stoppedFeeds:          make(map[model.ChangeFeedID]*model.ChangeFeedStatus),
		captures:              make(map[model.CaptureID]*model.CaptureInfo),
		rebalanceTigger:       make(map[model.ChangeFeedID]bool),
		manualScheduleCommand: make(map[model.ChangeFeedID][]*model.MoveTableJob),
		pdEndpoints:           endpoints,
		cfRWriter:             cli,
		etcdClient:            cli,
		gcTTL:                 gcTTL,
	}

	return owner, nil
}

func (o *Owner) addCapture(info *model.CaptureInfo) {
	o.l.Lock()
	o.captures[info.ID] = info
	o.l.Unlock()
	o.rebalanceMu.Lock()
	o.rebalanceForAllChangefeed = true
	o.rebalanceMu.Unlock()
}

func (o *Owner) removeCapture(info *model.CaptureInfo) {
	o.l.Lock()
	defer o.l.Unlock()

	delete(o.captures, info.ID)

	for _, feed := range o.changeFeeds {
		task, ok := feed.taskStatus[info.ID]
		if !ok {
			log.Warn("task status not found", zap.String("capture", info.ID), zap.String("changefeed", feed.id))
			continue
		}
		var startTs uint64
		pos, ok := feed.taskPositions[info.ID]
		if ok {
			startTs = pos.CheckPointTs
		} else {
			log.Warn("task position not found, fallback to use changefeed checkpointts",
				zap.String("capture", info.ID), zap.String("changefeed", feed.id))
			// maybe the processor hasn't added table yet, fallback to use the
			// global checkpoint ts as the start ts of the table.
			startTs = feed.status.CheckpointTs
		}

		for tableID := range task.Tables {
			feed.orphanTables[tableID] = startTs
		}

		ctx := context.TODO()
		if err := o.etcdClient.DeleteTaskStatus(ctx, feed.id, info.ID); err != nil {
			log.Warn("failed to delete task status",
				zap.String("capture", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
		if err := o.etcdClient.DeleteTaskPosition(ctx, feed.id, info.ID); err != nil {
			log.Warn("failed to delete task position",
				zap.String("capture", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
		if err := o.etcdClient.DeleteTaskWorkload(ctx, feed.id, info.ID); err != nil {
			log.Warn("failed to delete task workload",
				zap.String("capture", info.ID), zap.String("changefeed", feed.id), zap.Error(err))
		}
	}
}

func (o *Owner) addOrphanTable(cid model.CaptureID, tableID model.TableID, startTs model.Ts) {
	o.l.Lock()
	defer o.l.Unlock()
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
	checkpointTs uint64) (*changeFeed, error) {
	log.Info("Find new changefeed", zap.Stringer("info", info),
		zap.String("id", id), zap.Uint64("checkpoint ts", checkpointTs))

	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(nil, tikv.ErrGCTooEarly.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(nil, errors.New("failpoint injected retriable error"))
	})

	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(o.pdEndpoints, ","), o.credential)
	if err != nil {
		return nil, err
	}
	meta, err := kv.GetSnapshotMeta(kvStore, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	filter, err := filter.NewFilter(info.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if info.Engine == model.SortInFile {
		err = os.MkdirAll(info.SortDir, 0755)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = util.IsDirAndWritable(info.SortDir)
		if err != nil {
			return nil, err
		}
	}

	ddlHandler := newDDLHandler(o.pdClient, o.credential, kvStore, checkpointTs)

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
		if !tblInfo.IsEligible() {
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

		for i, colInfo := range tblInfo.Cols() {
			sinkTableInfo[j-1].ColumnInfo[i] = new(model.ColumnInfo)
			sinkTableInfo[j-1].ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

	}
	errCh := make(chan error, 1)

	sink, err := sink.NewSink(ctx, id, info.SinkURI, filter, info.Config, info.Opts, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	go func() {
		err := <-errCh
		if errors.Cause(err) != context.Canceled {
			log.Error("error on running owner", zap.Error(err))
		} else {
			log.Info("owner exited")
		}
		cancel()
	}()

	err = sink.Initialize(ctx, sinkTableInfo)
	if err != nil {
		log.Error("error on running owner", zap.Error(err))
	}

	cf := &changeFeed{
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
		scheduler:         scheduler.NewScheduler(info.Config.Scheduler.Tp),
		ddlState:          model.ChangeFeedSyncDML,
		ddlExecutedTs:     checkpointTs,
		targetTs:          info.GetTargetTs(),
		taskStatus:        processorsInfos,
		taskPositions:     taskPositions,
		etcdCli:           o.etcdClient,
		filter:            filter,
		sink:              sink,
		cyclicEnabled:     info.Config.Cyclic.IsEnabled(),
		lastRebalanceTime: time.Now(),
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
			err := o.cfRWriter.RemoveAllTaskStatus(ctx, changefeedID)
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
			err := o.etcdClient.SaveChangeFeedInfo(ctx, cfInfo, changeFeedID)
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
		checkpointTs := cfInfo.GetCheckpointTs(status)

		newCf, err := o.newChangeFeed(ctx, changeFeedID, taskStatus, taskPositions, cfInfo, checkpointTs)
		if err != nil {
			cfInfo.Error = &model.RunningError{
				Addr:    util.CaptureAddrFromCtx(ctx),
				Code:    "CDC-owner-1001",
				Message: err.Error(),
			}
			cfInfo.ErrorHis = append(cfInfo.ErrorHis, time.Now().UnixNano()/1e6)

			if filter.ChangefeedFastFailError(err) {
				log.Error("create changefeed with fast fail error, mark changefeed as failed",
					zap.Error(err), zap.String("changefeedid", changeFeedID))
				cfInfo.State = model.StateFailed
				err := o.etcdClient.SaveChangeFeedInfo(ctx, cfInfo, changeFeedID)
				if err != nil {
					return err
				}
				continue
			}

			err2 := o.etcdClient.SaveChangeFeedInfo(ctx, cfInfo, changeFeedID)
			if err2 != nil {
				return err2
			}
			// changefeed error has been recorded in etcd, log error here and
			// don't need to return an error.
			log.Warn("create changefeed failed, retry later",
				zap.String("changefeed", changeFeedID), zap.Error(err))
			continue
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
			_, err := o.pdClient.UpdateServiceGCSafePoint(ctx, CDCServiceSafePointID, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			o.gcSafepointLastUpdate = *new(time.Time)
		}
		return nil
	}

	minCheckpointTs := uint64(math.MaxUint64)
	if len(o.changeFeeds) > 0 {
		snapshot := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, len(o.changeFeeds))
		for id, changefeed := range o.changeFeeds {
			snapshot[id] = changefeed.status
			if changefeed.status.CheckpointTs < minCheckpointTs {
				minCheckpointTs = changefeed.status.CheckpointTs
			}
		}
		err := o.cfRWriter.PutAllChangeFeedStatus(ctx, snapshot)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, status := range o.stoppedFeeds {
		if status.CheckpointTs < minCheckpointTs {
			minCheckpointTs = status.CheckpointTs
		}
	}
	if time.Since(o.gcSafepointLastUpdate) > GCSafepointUpdateInterval {
		_, err := o.pdClient.UpdateServiceGCSafePoint(ctx, CDCServiceSafePointID, o.gcTTL, minCheckpointTs)
		if err != nil {
			log.Info("failed to update service safe point", zap.Error(err))
			return errors.Trace(err)
		}
		o.gcSafepointLastUpdate = time.Now()
	}
	return nil
}

// calcResolvedTs call calcResolvedTs of every changefeeds
func (o *Owner) calcResolvedTs(ctx context.Context) error {
	for _, cf := range o.changeFeeds {
		if err := cf.calcResolvedTs(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleDDL call handleDDL of every changefeeds
func (o *Owner) handleDDL(ctx context.Context) error {
	for _, cf := range o.changeFeeds {
		err := cf.handleDDL(ctx, o.captures)
		if err != nil {
			if cerror.ErrExecDDLFailed.NotEqual(err) {
				return errors.Trace(err)
			}
			err = o.EnqueueJob(model.AdminJob{
				CfID: cf.id,
				Type: model.AdminStop,
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// dispatchJob dispatches job to processors
func (o *Owner) dispatchJob(ctx context.Context, job model.AdminJob) error {
	cf, ok := o.changeFeeds[job.CfID]
	if !ok {
		return errors.Errorf("changefeed %s not found in owner cache", job.CfID)
	}
	for captureID := range cf.taskStatus {
		newStatus, _, err := cf.etcdCli.AtomicPutTaskStatus(ctx, cf.id, captureID, func(modRevision int64, taskStatus *model.TaskStatus) (bool, error) {
			taskStatus.AdminJobType = job.Type
			return true, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		cf.taskStatus[captureID] = newStatus.Clone()
	}
	// record admin job in changefeed status
	cf.status.AdminJobType = job.Type
	infos := map[model.ChangeFeedID]*model.ChangeFeedStatus{job.CfID: cf.status}
	err := o.cfRWriter.PutAllChangeFeedStatus(ctx, infos)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO Closing the resource should not be here
	err = cf.ddlHandler.Close()
	log.Info("stop changefeed ddl handler", zap.String("changefeed id", job.CfID), logutil.ZapErrorFilter(err, context.Canceled))
	err = cf.sink.Close()
	log.Info("stop changefeed sink", zap.String("changefeed id", job.CfID), logutil.ZapErrorFilter(err, context.Canceled))
	// Only need to process stoppedFeeds with `AdminStop` command here.
	// For `AdminResume`, we remove stopped feed in changefeed initialization phase.
	// For `AdminRemove`, we need to update stoppedFeeds when removing a stopped changefeed.
	if job.Type == model.AdminStop {
		o.stoppedFeeds[job.CfID] = cf.status
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
				err := o.etcdClient.DeleteChangeFeedInfo(ctx, job.CfID)
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
			err := o.etcdClient.SaveChangeFeedInfo(ctx, cf.info, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}

			err = o.dispatchJob(ctx, job)
			if err != nil {
				return errors.Trace(err)
			}
		case model.AdminRemove, model.AdminFinish:
			if cf != nil {
				err := o.dispatchJob(ctx, job)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				switch feedState {
				case model.StateRemoved, model.StateFinished:
					// remove a removed or finished changefeed
					if job.Opts != nil && job.Opts.ForceRemove {
						err := o.etcdClient.RemoveChangeFeedStatus(ctx, job.CfID)
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
					err = o.etcdClient.PutChangeFeedStatus(ctx, job.CfID, status)
					if err != nil {
						return errors.Trace(err)
					}
					delete(o.stoppedFeeds, job.CfID)
				default:
					return errors.Errorf("changefeed in abnormal state: %s, replication status: %+v", feedState, status)
				}
			}
			// remove changefeed info
			err := o.etcdClient.DeleteChangeFeedInfo(ctx, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}
			if job.Opts != nil && job.Opts.ForceRemove {
				// if `ForceRemove` is enabled, remove all information related to this changefeed
				err := o.etcdClient.RemoveChangeFeedStatus(ctx, job.CfID)
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
			err = o.etcdClient.PutChangeFeedStatus(ctx, job.CfID, status)
			if err != nil {
				return errors.Trace(err)
			}

			// set admin job in changefeed cfInfo to trigger each capture's changefeed list watch event
			cfInfo.AdminJobType = model.AdminResume
			// clear last running error
			cfInfo.State = model.StateNormal
			cfInfo.Error = nil
			err = o.etcdClient.SaveChangeFeedInfo(ctx, cfInfo, job.CfID)
			if err != nil {
				return errors.Trace(err)
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
	// A channel is used here to sychronize the steps.

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

	if err := o.throne(ctx); err != nil {
		return err
	}

	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	changedFeeds := o.watchFeedChange(ctx1)

	ticker := time.NewTicker(tickTime)
	defer ticker.Stop()

	var err error
loop:
	for {
		select {
		case <-o.done:
			close(o.done)
			break loop
		case <-ctx.Done():
			return ctx.Err()
		case <-changedFeeds:
		case <-ticker.C:
		}

		err = o.run(ctx)
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				log.Error("owner exited with error", zap.Error(err))
			}
			break loop
		}
	}
	if o.stepDown != nil {
		if err := o.stepDown(ctx); err != nil {
			return err
		}
	}

	return err
}

func (o *Owner) watchFeedChange(ctx context.Context) chan struct{} {
	output := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			wch := o.etcdClient.Client.Watch(ctx, kv.TaskPositionKeyPrefix, clientv3.WithFilterDelete(), clientv3.WithPrefix())

			for resp := range wch {
				if resp.Err() != nil {
					log.Error("position watcher restarted with error", zap.Error(resp.Err()))
					break
				}

				// TODO: because the main loop has many serial steps, it is hard to do a partial update without change
				// majority logical. For now just to wakeup the main loop ASAP to reduce latency, the efficiency of etcd
				// operations should be resolved in future release.
				output <- struct{}{}
			}
		}
	}()
	return output
}

func (o *Owner) run(ctx context.Context) error {
	o.l.Lock()
	defer o.l.Unlock()

	err := o.loadChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.balanceTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.calcResolvedTs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleDDL(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleAdminJob(ctx)
	if err != nil {
		return errors.Trace(err)
	}

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
		return errors.Errorf("invalid admin job type: %d", job.Type)
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
	for _, info := range o.changeFeeds {
		// fmt.Fprintf(w, "%+v\n", *info)
		fmt.Fprintf(w, "%s\n", info)
	}
}

// cleanUpStaleTasks cleans up the task status which does not associated
// with an active processor.
//
// When a new owner is elected, it does not know the events occurs before, like
// processor deletion. In this case, the new owner should check if the task
// status is stale because of the processor deletion.
func (o *Owner) cleanUpStaleTasks(ctx context.Context, captures []*model.CaptureInfo) error {
	_, changefeeds, err := o.etcdClient.GetChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	active := make(map[string]struct{})
	for _, c := range captures {
		active[c.ID] = struct{}{}
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

		for captureID := range captureIDs {
			if _, ok := active[captureID]; !ok {
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
							startTs = pos.CheckPointTs
						}
						o.addOrphanTable(changeFeedID, tableID, startTs)
					}
				}

				if err := o.etcdClient.DeleteTaskStatus(ctx, changeFeedID, captureID); err != nil {
					return errors.Trace(err)
				}
				if err := o.etcdClient.DeleteTaskPosition(ctx, changeFeedID, captureID); err != nil {
					return errors.Trace(err)
				}
				if err := o.etcdClient.DeleteTaskWorkload(ctx, changeFeedID, captureID); err != nil {
					return errors.Trace(err)
				}
				log.Info("cleanup stale task", zap.String("captureid", captureID), zap.String("changefeedid", changeFeedID))
			}
		}
	}
	return nil
}

func (o *Owner) watchCapture(ctx context.Context) error {
	ctx = clientv3.WithRequireLeader(ctx)

	failpoint.Inject("sleep-before-watch-capture", nil)

	// When an owner just starts, changefeed information is not updated at once.
	// Supposing a crased capture should be removed now, the owner will miss deleting
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

	rev, captures, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return errors.Trace(err)
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
			return errors.Trace(resp.Err())
		}
		for _, ev := range resp.Events {
			c := &model.CaptureInfo{}
			switch ev.Type {
			case clientv3.EventTypeDelete:
				if err := c.Unmarshal(ev.PrevKv.Value); err != nil {
					return errors.Trace(err)
				}
				log.Debug("capture deleted",
					zap.String("capture-id", c.ID),
					zap.String("advertise-addr", c.AdvertiseAddr))
				o.removeCapture(c)
			case clientv3.EventTypePut:
				if !ev.IsCreate() {
					continue
				}
				if err := c.Unmarshal(ev.Kv.Value); err != nil {
					return errors.Trace(err)
				}
				log.Debug("capture added",
					zap.String("capture-id", c.ID),
					zap.String("advertise-addr", c.AdvertiseAddr))
				o.addCapture(c)
			}
		}
	}
	return nil
}

func (o *Owner) rebuildCaptureEvents(ctx context.Context, captures []*model.CaptureInfo) error {
	current := make(map[string]*model.CaptureInfo)
	for _, c := range captures {
		current[c.ID] = c
		o.addCapture(c)
	}
	for _, c := range o.captures {
		if _, ok := current[c.ID]; !ok {
			o.removeCapture(c)
		}
	}
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
	//    missing a crashed capture is more likey to happen.
	return errors.Trace(o.cleanUpStaleTasks(ctx, captures))
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
				// Otherwise, a temporary error occured(ErrCompact),
				// restart the watching routine.
			}
		}
	}()
}
