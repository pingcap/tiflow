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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Owner manages the cdc cluster
type Owner struct {
	done                       chan struct{}
	session                    *concurrency.Session
	changeFeeds                map[model.ChangeFeedID]*changeFeed
	failedFeeds                map[model.ChangeFeedID]struct{}
	rebanlanceTigger           map[model.ChangeFeedID]bool
	rebanlanceForAllChangefeed bool
	manualScheduleCommand      map[model.ChangeFeedID][]*model.MoveTableJob
	rebanlanceMu               sync.Mutex

	cfRWriter ChangeFeedRWriter

	l sync.RWMutex

	pdEndpoints []string
	pdClient    pd.Client
	etcdClient  kv.CDCEtcdClient

	captures map[model.CaptureID]*model.CaptureInfo

	adminJobs     []model.AdminJob
	adminJobsLock sync.Mutex

	stepDown func(ctx context.Context) error

	// gcTTL is the ttl of cdc gc safepoint ttl.
	gcTTL int64
}

// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
const CDCServiceSafePointID = "ticdc"

// NewOwner creates a new Owner instance
func NewOwner(pdClient pd.Client, sess *concurrency.Session, gcTTL int64) (*Owner, error) {
	cli := kv.NewCDCEtcdClient(sess.Client())
	endpoints := sess.Client().Endpoints()

	owner := &Owner{
		done:                  make(chan struct{}),
		session:               sess,
		pdClient:              pdClient,
		changeFeeds:           make(map[model.ChangeFeedID]*changeFeed),
		failedFeeds:           make(map[model.ChangeFeedID]struct{}),
		captures:              make(map[model.CaptureID]*model.CaptureInfo),
		rebanlanceTigger:      make(map[model.ChangeFeedID]bool),
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
	o.rebanlanceMu.Lock()
	o.rebanlanceForAllChangefeed = true
	o.rebanlanceMu.Unlock()
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
	log.Info("Find new changefeed", zap.Reflect("info", info),
		zap.String("id", id), zap.Uint64("checkpoint ts", checkpointTs))

	failpoint.Inject("NewChangefeedError", func() {
		failpoint.Return(nil, tikv.ErrGCTooEarly.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})

	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(o.pdEndpoints, ","))
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

	ddlHandler := newDDLHandler(o.pdClient, kvStore, checkpointTs)

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
	for tid, table := range schemaSnap.CloneTables() {
		if filter.ShouldIgnoreTable(table.Schema, table.Table) {
			continue
		}
		if info.Config.Cyclic.IsEnabled() && cyclic.IsMarkTable(table.Schema, table.Table) {
			// skip the mark table if cyclic is enabled
			continue
		}

		tables[tid] = table
		if ts, ok := existingTables[tid]; ok {
			log.Debug("ignore known table", zap.Int64("tid", tid), zap.Stringer("table", table), zap.Uint64("ts", ts))
			continue
		}
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
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			delete(partitions, tid)
			for _, partition := range pi.Definitions {
				id := partition.ID
				if ts, ok := existingTables[id]; ok {
					log.Debug("ignore known table partition", zap.Int64("tid", tid), zap.Int64("partitionID", id), zap.Stringer("table", table), zap.Uint64("ts", ts))
					continue
				}
				partitions[tid] = append(partitions[tid], partition.ID)
				orphanTables[id] = checkpointTs
			}
		} else {
			orphanTables[tid] = checkpointTs
		}

	}
	errCh := make(chan error, 1)

	sink, err := sink.NewSink(ctx, info.SinkURI, filter, info.Config, info.Opts, errCh)
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
		scheduler:          scheduler.NewScheduler(info.Config.Scheduler.Tp),
		ddlState:           model.ChangeFeedSyncDML,
		ddlExecutedTs:      checkpointTs,
		targetTs:           info.GetTargetTs(),
		taskStatus:         processorsInfos,
		taskPositions:      taskPositions,
		etcdCli:            o.etcdClient,
		filter:             filter,
		sink:               sink,
		cyclicEnabled:      info.Config.Cyclic.IsEnabled(),
		lastRebanlanceTime: time.Now(),
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
		switch errors.Cause(err) {
		case model.ErrDecodeFailed:
			err := o.cfRWriter.RemoveAllTaskStatus(ctx, changefeedID)
			if err != nil {
				return errors.Trace(err)
			}
			cleaned = true
		case nil:
		default:
			return errors.Trace(err)
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
			continue
		}

		// we find a new changefeed, init changefeed here.
		cfInfo := &model.ChangeFeedInfo{}
		err = cfInfo.Unmarshal(cfInfoRawValue.Value)
		if err != nil {
			return err
		}
		if cfInfo.State == model.StateFailed {
			if _, ok := o.failedFeeds[changeFeedID]; ok {
				continue
			}
			log.Warn("changefeed is not in normal state", zap.String("changefeed", changeFeedID))
			o.failedFeeds[changeFeedID] = struct{}{}
			continue
		}
		if _, ok := o.failedFeeds[changeFeedID]; ok {
			log.Info("changefeed recovered from failure", zap.String("changefeed", changeFeedID))
			delete(o.failedFeeds, changeFeedID)
		}
		err = cfInfo.VerifyAndFix()
		if err != nil {
			return err
		}

		status, _, err := o.cfRWriter.GetChangeFeedStatus(ctx, changeFeedID)
		if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
			return err
		}
		if status != nil && (status.AdminJobType == model.AdminStop || status.AdminJobType == model.AdminRemove) {
			continue
		}
		checkpointTs := cfInfo.GetCheckpointTs(status)

		newCf, err := o.newChangeFeed(ctx, changeFeedID, taskStatus, taskPositions, cfInfo, checkpointTs)
		if err != nil {
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
			return errors.Annotatef(err, "create change feed %s", changeFeedID)
		}
		o.changeFeeds[changeFeedID] = newCf
	}
	return nil
}

func (o *Owner) balanceTables(ctx context.Context) error {
	rebanlanceForAllChangefeed := false
	o.rebanlanceMu.Lock()
	if o.rebanlanceForAllChangefeed {
		rebanlanceForAllChangefeed = true
		o.rebanlanceForAllChangefeed = false
	}
	o.rebanlanceMu.Unlock()
	for id, changefeed := range o.changeFeeds {
		rebanlanceNow := false
		var scheduleCommands []*model.MoveTableJob
		o.rebanlanceMu.Lock()
		if r, exist := o.rebanlanceTigger[id]; exist {
			rebanlanceNow = r
			delete(o.rebanlanceTigger, id)
		}
		if rebanlanceForAllChangefeed {
			rebanlanceNow = true
		}
		if c, exist := o.manualScheduleCommand[id]; exist {
			scheduleCommands = c
			delete(o.manualScheduleCommand, id)
		}
		o.rebanlanceMu.Unlock()
		err := changefeed.tryBalance(ctx, o.captures, rebanlanceNow, scheduleCommands)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (o *Owner) flushChangeFeedInfos(ctx context.Context) error {
	if len(o.changeFeeds) == 0 {
		return nil
	}
	snapshot := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, len(o.changeFeeds))
	minCheckpointTs := uint64(math.MaxUint64)
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
	_, err = o.pdClient.UpdateServiceGCSafePoint(ctx, CDCServiceSafePointID, o.gcTTL, minCheckpointTs)
	if err != nil {
		log.Info("failed to update service safe point", zap.Error(err))
		return errors.Trace(err)
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
		switch errors.Cause(err) {
		case nil:
			continue
		case model.ErrExecDDLFailed:
			err = o.EnqueueJob(model.AdminJob{
				CfID: cf.id,
				Type: model.AdminStop,
			})
			if err != nil {
				return errors.Trace(err)
			}
		default:
			return errors.Trace(err)
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
		newStatus, err := cf.etcdCli.AtomicPutTaskStatus(ctx, cf.id, captureID, func(taskStatus *model.TaskStatus) error {
			taskStatus.AdminJobType = job.Type
			return nil
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
	log.Info("stop changefeed ddl handler", zap.String("changefeed id", job.CfID), util.ZapErrorFilter(err, context.Canceled))
	err = cf.sink.Close()
	log.Info("stop changefeed sink", zap.String("changefeed id", job.CfID), util.ZapErrorFilter(err, context.Canceled))
	delete(o.changeFeeds, job.CfID)
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
		switch job.Type {
		case model.AdminStop:
			// update ChangeFeedDetail to tell capture ChangeFeedDetail watcher to cleanup
			cf, ok := o.changeFeeds[job.CfID]
			if !ok {
				log.Warn("invalid admin job, changefeed not found", zap.String("changefeed", job.CfID))
				continue
			}
			cf.info.AdminJobType = model.AdminStop
			err := o.etcdClient.SaveChangeFeedInfo(ctx, cf.info, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}

			err = o.dispatchJob(ctx, job)
			if err != nil {
				return errors.Trace(err)
			}
		case model.AdminRemove:
			err := o.dispatchJob(ctx, job)
			if err != nil {
				return errors.Trace(err)
			}

			// remove changefeed info
			err = o.etcdClient.DeleteChangeFeedInfo(ctx, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}
			// set ttl to changefeed status
			err = o.etcdClient.SetChangeFeedStatusTTL(ctx, job.CfID, 24*3600 /*24 hours*/)
			if err != nil {
				return errors.Trace(err)
			}
		case model.AdminResume:
			cfStatus, _, err := o.etcdClient.GetChangeFeedStatus(ctx, job.CfID)
			if errors.Cause(err) == model.ErrChangeFeedNotExists {
				log.Warn("invalid admin job, changefeed not found", zap.String("changefeed", job.CfID))
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}
			cfInfo, err := o.etcdClient.GetChangeFeedInfo(ctx, job.CfID)
			if errors.Cause(err) == model.ErrChangeFeedNotExists {
				log.Warn("invalid admin job, changefeed not found", zap.String("changefeed", job.CfID))
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}

			// set admin job in changefeed status to tell owner resume changefeed
			cfStatus.AdminJobType = model.AdminResume
			err = o.etcdClient.PutChangeFeedStatus(ctx, job.CfID, cfStatus)
			if err != nil {
				return errors.Trace(err)
			}

			// set admin job in changefeed cfInfo to trigger each capture's changefeed list watch event
			cfInfo.AdminJobType = model.AdminResume
			err = o.etcdClient.SaveChangeFeedInfo(ctx, cfInfo, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}
		}
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

		err := o.run(ctx)
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

	return nil
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
	return nil
}

// EnqueueJob adds an admin job
func (o *Owner) EnqueueJob(job model.AdminJob) error {
	switch job.Type {
	case model.AdminResume:
	case model.AdminStop, model.AdminRemove:
		_, ok := o.changeFeeds[job.CfID]
		if !ok {
			return errors.Errorf("changefeed [%s] not found", job.CfID)
		}
	default:
		return errors.Errorf("invalid admin job type: %d", job.Type)
	}
	o.adminJobsLock.Lock()
	o.adminJobs = append(o.adminJobs, job)
	o.adminJobsLock.Unlock()
	return nil
}

// TriggerRebanlance triggers the rebalance in the specified changefeed
func (o *Owner) TriggerRebanlance(changefeedID model.ChangeFeedID) {
	o.rebanlanceMu.Lock()
	defer o.rebanlanceMu.Unlock()
	o.rebanlanceTigger[changefeedID] = true
	// TODO(leoppro) throw an error if the changefeed is not exist
}

// ManualSchedule moves the table from a capture to another capture
func (o *Owner) ManualSchedule(changefeedID model.ChangeFeedID, to model.CaptureID, tableID model.TableID) {
	o.rebanlanceMu.Lock()
	defer o.rebanlanceMu.Unlock()
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
				log.Debug("cleanup stale task", zap.String("captureid", captureID), zap.String("changefeedid", changeFeedID))
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
		rl := rate.NewLimiter(0.05, 5)
		for {
			if !rl.Allow() {
				log.Error("owner capture watcher exceeds rate limit")
				time.Sleep(10 * time.Second)
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
