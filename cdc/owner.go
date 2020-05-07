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
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
)

// Owner manages the cdc cluster
type Owner struct {
	done        chan struct{}
	session     *concurrency.Session
	changeFeeds map[model.ChangeFeedID]*changeFeed

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

const cdcServiceSafePointID = "ticdc"

// NewOwner creates a new Owner instance
func NewOwner(sess *concurrency.Session, gcTTL int64) (*Owner, error) {
	cli := kv.NewCDCEtcdClient(sess.Client())
	endpoints := sess.Client().Endpoints()
	pdClient, err := pd.NewClient(endpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	owner := &Owner{
		done:        make(chan struct{}),
		session:     sess,
		pdClient:    pdClient,
		changeFeeds: make(map[model.ChangeFeedID]*changeFeed),
		captures:    make(map[model.CaptureID]*model.CaptureInfo),
		pdEndpoints: endpoints,
		cfRWriter:   cli,
		etcdClient:  cli,
		gcTTL:       gcTTL,
	}

	return owner, nil
}

func (o *Owner) addCapture(info *model.CaptureInfo) {
	o.l.Lock()
	o.captures[info.ID] = info
	o.l.Unlock()
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

		for _, table := range task.TableInfos {
			feed.orphanTables[table.ID] = model.ProcessTableInfo{
				ID:      table.ID,
				StartTs: startTs,
			}
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

func (o *Owner) addOrphanTable(cid string, table model.ProcessTableInfo) {
	o.l.Lock()
	defer o.l.Unlock()
	if cf, ok := o.changeFeeds[cid]; ok {
		cf.orphanTables[table.ID] = table
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

	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(o.pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, errors.Trace(err)
	}

	filter, err := filter.NewFilter(info.GetConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemaStorage, err := entry.NewSchemaStorage(jobs, filter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage.FlushIneligibleTables(checkpointTs)
	schemaStorage.AdvanceResolvedTs(checkpointTs)

	ddlHandler := newDDLHandler(o.pdClient, kvStore, checkpointTs)

	existingTables := make(map[uint64]uint64)
	for captureID, taskStatus := range processorsInfos {
		var checkpointTs uint64
		if pos, exist := taskPositions[captureID]; exist {
			checkpointTs = pos.CheckPointTs
		}
		for _, tbl := range taskStatus.TableInfos {
			if tbl.StartTs > checkpointTs {
				checkpointTs = tbl.StartTs
			}
			existingTables[tbl.ID] = checkpointTs
		}
	}

	schemas := make(map[uint64]tableIDMap)
	tables := make(map[uint64]entry.TableName)
	orphanTables := make(map[uint64]model.ProcessTableInfo)
	snap, err := schemaStorage.GetSnapshot(ctx, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for tid, table := range snap.CloneTables() {
		if filter.ShouldIgnoreTable(table.Schema, table.Table) {
			continue
		}

		tables[tid] = table
		if ts, ok := existingTables[tid]; ok {
			log.Debug("ignore known table", zap.Uint64("tid", tid), zap.Stringer("table", table), zap.Uint64("ts", ts))
			continue
		}
		schema, ok := snap.SchemaByTableID(int64(tid))
		if !ok {
			log.Warn("schema not found for table", zap.Uint64("tid", tid))
		} else {
			sid := uint64(schema.ID)
			if _, ok := schemas[sid]; !ok {
				schemas[sid] = make(tableIDMap)
			}
			schemas[sid][tid] = struct{}{}
		}
		orphanTables[tid] = model.ProcessTableInfo{
			ID:      tid,
			StartTs: checkpointTs,
		}
	}

	sink, err := sink.NewSink(ctx, info.SinkURI, filter, info.GetConfig(), info.Opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	go func() {
		ctx := util.SetOwnerInCtx(context.TODO())
		if err := sink.Run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("failed to close sink", zap.Error(err))
		}
	}()

	cf := &changeFeed{
		info:                 info,
		id:                   id,
		ddlHandler:           ddlHandler,
		schema:               schemaStorage,
		schemas:              schemas,
		tables:               tables,
		orphanTables:         orphanTables,
		waitingConfirmTables: make(map[uint64]string),
		toCleanTables:        make(map[uint64]struct{}),
		status: &model.ChangeFeedStatus{
			ResolvedTs:   0,
			CheckpointTs: checkpointTs,
		},
		ddlState:      model.ChangeFeedSyncDML,
		ddlJobHistory: jobs,
		ddlExecutedTs: checkpointTs,
		targetTs:      info.GetTargetTs(),
		taskStatus:    processorsInfos,
		taskPositions: taskPositions,
		infoWriter:    storage.NewOwnerTaskStatusEtcdWriter(o.etcdClient),
		filter:        filter,
		sink:          sink,
	}
	return cf, nil
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

		// we find a new changefeed, init changefeed info here.
		status, err := o.cfRWriter.GetChangeFeedStatus(ctx, changeFeedID)
		if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
			return err
		}
		if status != nil && (status.AdminJobType == model.AdminStop || status.AdminJobType == model.AdminRemove) {
			continue
		}

		cfInfo := &model.ChangeFeedInfo{}
		err = cfInfo.Unmarshal(cfInfoRawValue.Value)
		if err != nil {
			return err
		}
		checkpointTs := cfInfo.GetCheckpointTs(status)

		newCf, err := o.newChangeFeed(ctx, changeFeedID, taskStatus, taskPositions, cfInfo, checkpointTs)
		if err != nil {
			return errors.Annotatef(err, "create change feed %s", changeFeedID)
		}
		o.changeFeeds[changeFeedID] = newCf
	}
	return nil
}

func (o *Owner) balanceTables(ctx context.Context) error {
	for _, changefeed := range o.changeFeeds {
		err := changefeed.tryBalance(ctx, o.captures)
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
	_, err = o.pdClient.UpdateServiceGCSafePoint(ctx, cdcServiceSafePointID, o.gcTTL, minCheckpointTs)
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
	for captureID, pinfo := range cf.taskStatus {
		pinfo.TablePLock = nil
		pinfo.TableCLock = nil
		pinfo.AdminJobType = job.Type
		_, err := cf.infoWriter.Write(ctx, cf.id, captureID, pinfo, false)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// record admin job in changefeed status
	cf.status.AdminJobType = job.Type
	infos := map[model.ChangeFeedID]*model.ChangeFeedStatus{job.CfID: cf.status}
	err := o.cfRWriter.PutAllChangeFeedStatus(ctx, infos)
	if err != nil {
		return errors.Trace(err)
	}
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
		switch job.Type {
		case model.AdminStop:
			// update ChangeFeedDetail to tell capture ChangeFeedDetail watcher to cleanup
			cf, ok := o.changeFeeds[job.CfID]
			if !ok {
				return errors.Errorf("changefeed %s not found in owner cache", job.CfID)
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
		case model.AdminResume:
			cfStatus, err := o.etcdClient.GetChangeFeedStatus(ctx, job.CfID)
			if err != nil {
				return errors.Trace(err)
			}
			cfInfo, err := o.etcdClient.GetChangeFeedInfo(ctx, job.CfID)
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
		removeIdx = i + 1
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := o.throne(ctx); err != nil {
		return err
	}
loop:
	for {
		select {
		case <-o.done:
			close(o.done)
			break loop
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tickTime):
			err := o.run(ctx)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					log.Error("owner exited with error", zap.Error(err))
				}
				break loop
			}
		}
	}
	if o.stepDown != nil {
		if err := o.stepDown(ctx); err != nil {
			return err
		}
	}

	return nil
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
		// in most cases statuses and positions have the same keys, or positions
		// are more than statuses, as we always delete task status first.
		captureIDs := make(map[string]struct{}, len(statuses))
		for captureID, status := range statuses {
			captureIDs[captureID] = struct{}{}
			pos, taskPosFound := positions[captureID]
			if !taskPosFound {
				log.Warn("task position not found, fallback to use original start ts",
					zap.String("capture", captureID),
					zap.String("changefeed", changeFeedID),
					zap.Reflect("task status", status),
				)
			}
			for _, table := range status.TableInfos {
				var startTs uint64
				if taskPosFound {
					startTs = pos.CheckPointTs
				} else {
					startTs = table.StartTs
				}
				o.addOrphanTable(changeFeedID, model.ProcessTableInfo{
					ID:      table.ID,
					StartTs: startTs,
				})
			}
		}
		for captureID := range positions {
			captureIDs[captureID] = struct{}{}
		}

		for captureID := range captureIDs {
			if _, ok := active[captureID]; !ok {
				if err := o.etcdClient.DeleteTaskStatus(ctx, changeFeedID, captureID); err != nil {
					return errors.Trace(err)
				}
				if err := o.etcdClient.DeleteTaskPosition(ctx, changeFeedID, captureID); err != nil {
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
					zap.String("captureid", c.ID))
				o.removeCapture(c)
			case clientv3.EventTypePut:
				if !ev.IsCreate() {
					continue
				}
				if err := c.Unmarshal(ev.Kv.Value); err != nil {
					return errors.Trace(err)
				}
				log.Debug("capture added",
					zap.String("captureid", c.ID))
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
		for {
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
