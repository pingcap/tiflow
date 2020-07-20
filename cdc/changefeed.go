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
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

type tableIDMap = map[model.TableID]struct{}

// OwnerDDLHandler defines the ddl handler for Owner
// which can pull ddl jobs and execute ddl jobs
type OwnerDDLHandler interface {
	// PullDDL pulls the ddl jobs and returns resolvedTs of DDL Puller and job list.
	PullDDL() (resolvedTs uint64, jobs []*timodel.Job, err error)

	// Close cancels the executing of OwnerDDLHandler and releases resource
	Close() error
}

// ChangeFeedRWriter defines the Reader and Writer for changeFeed
type ChangeFeedRWriter interface {

	// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
	GetChangeFeeds(ctx context.Context) (int64, map[string]*mvccpb.KeyValue, error)

	// GetAllTaskStatus queries all task status of a changefeed, and returns a map
	// mapping from captureID to TaskStatus
	GetAllTaskStatus(ctx context.Context, changefeedID string) (model.ProcessorsInfos, error)

	// RemoveAllTaskStatus removes all task status of a changefeed
	RemoveAllTaskStatus(ctx context.Context, changefeedID string) error

	// GetAllTaskPositions queries all task positions of a changefeed, and returns a map
	// mapping from captureID to TaskPositions
	GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error)

	// RemoveAllTaskPositions removes all task partitions of a changefeed
	RemoveAllTaskPositions(ctx context.Context, changefeedID string) error

	// GetChangeFeedStatus queries the checkpointTs and resovledTs of a given changefeed
	GetChangeFeedStatus(ctx context.Context, id string) (*model.ChangeFeedStatus, int64, error)
	// PutAllChangeFeedStatus the changefeed info to storage such as etcd.
	PutAllChangeFeedStatus(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error
}

type changeFeed struct {
	id     string
	info   *model.ChangeFeedInfo
	status *model.ChangeFeedStatus

	schema        *entry.SingleSchemaSnapshot
	ddlState      model.ChangeFeedDDLState
	targetTs      uint64
	taskStatus    model.ProcessorsInfos
	taskPositions map[model.CaptureID]*model.TaskPosition
	filter        *filter.Filter
	sink          sink.Sink
	scheduler     scheduler.Scheduler

	cyclicEnabled bool

	ddlHandler    OwnerDDLHandler
	ddlResolvedTs uint64
	ddlJobHistory []*timodel.Job
	ddlExecutedTs uint64

	schemas map[model.SchemaID]tableIDMap
	tables  map[model.TableID]model.TableName
	// value of partitions is the slice of partitions ID.
	partitions         map[model.TableID][]int64
	orphanTables       map[model.TableID]model.Ts
	toCleanTables      map[model.TableID]model.Ts
	moveTableJobs      map[model.TableID]*model.MoveTableJob
	manualMoveCommands []*model.MoveTableJob
	rebalanceNextTick  bool

	lastRebalanceTime time.Time

	etcdCli kv.CDCEtcdClient
}

// String implements fmt.Stringer interface.
func (c *changeFeed) String() string {
	format := "{\n ID: %s\n info: %+v\n status: %+v\n State: %v\n ProcessorInfos: %+v\n tables: %+v\n orphanTables: %+v\n toCleanTables: %v\n ddlResolvedTs: %d\n ddlJobHistory: %+v\n}\n\n"
	s := fmt.Sprintf(format,
		c.id, c.info, c.status, c.ddlState, c.taskStatus, c.tables,
		c.orphanTables, c.toCleanTables, c.ddlResolvedTs, c.ddlJobHistory)

	if len(c.ddlJobHistory) > 0 {
		job := c.ddlJobHistory[0]
		s += fmt.Sprintf("next to exec job: %s query: %s\n\n", job, job.Query)
	}

	return s
}

func (c *changeFeed) updateProcessorInfos(processInfos model.ProcessorsInfos, positions map[string]*model.TaskPosition) {
	c.taskStatus = processInfos
	c.taskPositions = positions
}

func (c *changeFeed) addSchema(schemaID model.SchemaID) {
	if _, ok := c.schemas[schemaID]; ok {
		log.Warn("add schema already exists", zap.Int64("schemaID", schemaID))
		return
	}
	c.schemas[schemaID] = make(map[model.TableID]struct{})
}

func (c *changeFeed) dropSchema(schemaID model.SchemaID, targetTs model.Ts) {
	if schema, ok := c.schemas[schemaID]; ok {
		for tid := range schema {
			c.removeTable(schemaID, tid, targetTs)
		}
	}
	delete(c.schemas, schemaID)
}

func (c *changeFeed) addTable(sid model.SchemaID, tid model.TableID, startTs model.Ts, table model.TableName, tblInfo *timodel.TableInfo) {
	if c.filter.ShouldIgnoreTable(table.Schema, table.Table) {
		return
	}
	if c.cyclicEnabled && mark.IsMarkTable(table.Schema, table.Table) {
		return
	}

	if _, ok := c.tables[tid]; ok {
		log.Warn("add table already exists", zap.Int64("tableID", tid), zap.Stringer("table", table))
		return
	}

	if !entry.WrapTableInfo(sid, table.Schema, tblInfo).IsEligible() {
		log.Warn("skip ineligible table", zap.Int64("tid", tid), zap.Stringer("table", table))
		return
	}

	if _, ok := c.schemas[sid]; !ok {
		c.schemas[sid] = make(tableIDMap)
	}
	c.schemas[sid][tid] = struct{}{}
	c.tables[tid] = table
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		delete(c.partitions, tid)
		for _, partition := range pi.Definitions {
			c.partitions[tid] = append(c.partitions[tid], partition.ID)
			c.orphanTables[partition.ID] = startTs
		}
	} else {
		c.orphanTables[tid] = startTs
	}
}

func (c *changeFeed) removeTable(sid model.SchemaID, tid model.TableID, targetTs model.Ts) {
	if _, ok := c.schemas[sid]; ok {
		delete(c.schemas[sid], tid)
	}
	delete(c.tables, tid)

	removeFunc := func(id int64) {
		if _, ok := c.orphanTables[id]; ok {
			delete(c.orphanTables, id)
		} else {
			c.toCleanTables[id] = targetTs
		}
	}

	if pids, ok := c.partitions[tid]; ok {
		for _, id := range pids {
			removeFunc(id)
		}
		delete(c.partitions, tid)
	} else {
		removeFunc(tid)
	}
}

func (c *changeFeed) updatePartition(tblInfo *timodel.TableInfo, startTs uint64) {
	tid := tblInfo.ID
	partitionsID, ok := c.partitions[tid]
	if !ok || len(partitionsID) == 0 {
		return
	}
	oldIDs := make(map[int64]struct{}, len(partitionsID))
	for _, pid := range partitionsID {
		oldIDs[pid] = struct{}{}
	}

	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return
	}
	newPartitionIDs := make([]int64, 0, len(pi.Definitions))
	for _, partition := range pi.Definitions {
		pid := partition.ID
		_, ok := c.orphanTables[pid]
		if !ok {
			// new partition.
			c.orphanTables[pid] = startTs
		}
		delete(oldIDs, partition.ID)
		newPartitionIDs = append(newPartitionIDs, partition.ID)
	}
	// update the table partition IDs.
	c.partitions[tid] = newPartitionIDs

	// drop partition.
	for pid := range oldIDs {
		if _, ok := c.orphanTables[pid]; ok {
			delete(c.orphanTables, pid)
		} else {
			c.toCleanTables[pid] = startTs
		}
	}
}

func (c *changeFeed) tryBalance(ctx context.Context, captures map[string]*model.CaptureInfo, rebalanceNow bool,
	manualMoveCommands []*model.MoveTableJob) error {
	err := c.balanceOrphanTables(ctx, captures)
	if err != nil {
		return errors.Trace(err)
	}
	c.manualMoveCommands = append(c.manualMoveCommands, manualMoveCommands...)
	if rebalanceNow {
		c.rebalanceNextTick = true
	}
	err = c.handleManualMoveTableJobs(ctx, captures)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.rebalanceTables(ctx, captures)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.handleMoveTableJobs(ctx, captures)
	return errors.Trace(err)
}

func findTaskStatusWithTable(infos model.ProcessorsInfos, tableID model.TableID) (captureID model.CaptureID, info *model.TaskStatus, ok bool) {
	for cid, info := range infos {
		for tid := range info.Tables {
			if tid == tableID {
				return cid, info, true
			}
		}
	}

	return "", nil, false
}

func (c *changeFeed) balanceOrphanTables(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	if len(captures) == 0 {
		return nil
	}
	for _, status := range c.taskStatus {
		if status.SomeOperationsUnapplied() {
			return nil
		}
	}

	newTaskStatus := make(map[model.CaptureID]*model.TaskStatus, len(captures))
	captureIDs := make(map[model.CaptureID]struct{}, len(captures))
	cleanedTables := make(map[model.TableID]struct{})
	addedTables := make(map[model.TableID]struct{})
	for cid := range captures {
		captureIDs[cid] = struct{}{}
	}
	c.scheduler.AlignCapture(captureIDs)

	for id, targetTs := range c.toCleanTables {
		captureID, taskStatus, ok := findTaskStatusWithTable(c.taskStatus, id)
		if !ok {
			log.Warn("ignore clean table id", zap.Int64("id", id))
			delete(c.toCleanTables, id)
			continue
		}
		status, exist := newTaskStatus[captureID]
		if !exist {
			status = taskStatus.Clone()
		}
		status.RemoveTable(id, targetTs)
		newTaskStatus[captureID] = status
		cleanedTables[id] = struct{}{}
	}

	operations := c.scheduler.DistributeTables(c.orphanTables)
	for captureID, operation := range operations {
		status, exist := newTaskStatus[captureID]
		if !exist {
			taskStatus := c.taskStatus[captureID]
			if taskStatus == nil {
				status = new(model.TaskStatus)
			} else {
				status = taskStatus.Clone()
			}
			newTaskStatus[captureID] = status
		}
		schemaSnapshot := c.schema
		for tableID, op := range operation {
			var orphanMarkTableID model.TableID
			tableName, found := schemaSnapshot.GetTableNameByID(tableID)
			if !found {
				log.Warn("balance orphan tables delay, table not found",
					zap.String("changefeed", c.id),
					zap.Int64("tableID", tableID))
				continue
			}
			if c.cyclicEnabled {
				markTableSchameName, markTableTableName := mark.GetMarkTableName(tableName.Schema, tableName.Table)
				orphanMarkTableID, found = schemaSnapshot.GetTableIDByName(markTableSchameName, markTableTableName)
				if !found {
					// Mark table is not created yet, skip and wait.
					log.Info("balance orphan tables delay, wait mark table",
						zap.String("changefeed", c.id),
						zap.Int64("tableID", tableID),
						zap.String("markTableName", markTableTableName))
					continue
				}
			}
			info := &model.TableReplicaInfo{
				StartTs:     op.BoundaryTs,
				MarkTableID: orphanMarkTableID,
			}
			status.AddTable(tableID, info, op.BoundaryTs)
			addedTables[tableID] = struct{}{}
		}
	}

	err := c.updateTaskStatus(ctx, newTaskStatus)
	if err != nil {
		return errors.Trace(err)
	}

	for tableID := range cleanedTables {
		delete(c.toCleanTables, tableID)
	}
	for tableID := range addedTables {
		delete(c.orphanTables, tableID)
	}

	return nil
}

func (c *changeFeed) updateTaskStatus(ctx context.Context, taskStatus map[model.CaptureID]*model.TaskStatus) error {
	for captureID, status := range taskStatus {
		newStatus, err := c.etcdCli.AtomicPutTaskStatus(ctx, c.id, captureID, func(taskStatus *model.TaskStatus) error {
			if taskStatus.SomeOperationsUnapplied() {
				return errors.Errorf("waiting to processor handle the operation finished time out")
			}
			taskStatus.Tables = status.Tables
			taskStatus.Operation = status.Operation
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		c.taskStatus[captureID] = newStatus.Clone()
		log.Info("dispatch table success", zap.String("captureID", captureID), zap.Stringer("status", status))
	}
	return nil
}

func (c *changeFeed) handleManualMoveTableJobs(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	if len(captures) == 0 {
		return nil
	}
	if len(c.moveTableJobs) > 0 {
		return nil
	}
	for len(c.manualMoveCommands) > 0 {
		moveJob := c.manualMoveCommands[0]
		if _, exist := c.moveTableJobs[moveJob.TableID]; exist {
			break
		}
		c.manualMoveCommands = c.manualMoveCommands[1:]
		moveJob.From = ""
		for captureID, taskStatus := range c.taskStatus {
			if _, exist := taskStatus.Tables[moveJob.TableID]; exist {
				moveJob.From = captureID
				break
			}
		}
		if moveJob.From == "" {
			log.Warn("invalid manual move job, the table is not found", zap.Reflect("job", moveJob))
			continue
		}
		if moveJob.To == moveJob.From {
			log.Warn("invalid manual move job, the table is already exists in the target capture", zap.Reflect("job", moveJob))
			continue
		}
		if _, exist := captures[moveJob.To]; !exist {
			log.Warn("invalid manual move job, the target capture is not found", zap.Reflect("job", moveJob))
			continue
		}
		if c.moveTableJobs == nil {
			c.moveTableJobs = make(map[model.TableID]*model.MoveTableJob)
		}
		c.moveTableJobs[moveJob.TableID] = moveJob
		log.Info("received the manual move table job", zap.Reflect("job", moveJob))
	}
	return nil
}

func (c *changeFeed) rebalanceTables(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	if len(captures) == 0 {
		return nil
	}
	if len(c.moveTableJobs) != 0 {
		return nil
	}
	for _, status := range c.taskStatus {
		if status.SomeOperationsUnapplied() {
			return nil
		}
	}
	timeToRebalance := time.Since(c.lastRebalanceTime) > time.Duration(c.info.Config.Scheduler.PollingTime)*time.Minute
	timeToRebalance = timeToRebalance && c.info.Config.Scheduler.PollingTime > 0

	if !c.rebalanceNextTick && !timeToRebalance {
		return nil
	}
	c.lastRebalanceTime = time.Now()
	c.rebalanceNextTick = false

	captureIDs := make(map[model.CaptureID]struct{}, len(captures))
	for cid := range captures {
		captureIDs[cid] = struct{}{}
		workloads, err := c.etcdCli.GetTaskWorkload(ctx, c.id, cid)
		if err != nil {
			return errors.Trace(err)
		}
		c.scheduler.ResetWorkloads(cid, workloads)
	}
	c.scheduler.AlignCapture(captureIDs)

	_, moveTableJobs := c.scheduler.CalRebalanceOperates(0)
	log.Info("rebalance operations", zap.Reflect("moveTableJobs", moveTableJobs))
	c.moveTableJobs = moveTableJobs
	return nil
}

func (c *changeFeed) handleMoveTableJobs(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	if len(captures) == 0 {
		return nil
	}
	if len(c.moveTableJobs) == 0 {
		return nil
	}
	for _, status := range c.taskStatus {
		if status.SomeOperationsUnapplied() {
			return nil
		}
	}
	newTaskStatus := make(map[model.CaptureID]*model.TaskStatus, len(captures))
	cloneStatus := func(captureID model.CaptureID) (*model.TaskStatus, bool) {
		status, exist := newTaskStatus[captureID]
		if !exist {
			taskStatus := c.taskStatus[captureID]
			if taskStatus == nil {
				if _, exist := captures[captureID]; !exist {
					return nil, false
				}
				status = new(model.TaskStatus)
			} else {
				status = taskStatus.Clone()
			}
			newTaskStatus[captureID] = status
		}
		return status, true
	}
	for tableID, job := range c.moveTableJobs {
		switch job.Status {
		case model.MoveTableStatusNone:
			// delete table from original capture
			status, exist := cloneStatus(job.From)
			if !exist {
				delete(c.moveTableJobs, tableID)
				log.Warn("ignored the move job, the source capture is not found", zap.Reflect("job", job))
				continue
			}
			replicaInfo, exist := status.RemoveTable(tableID, c.status.CheckpointTs)
			if !exist {
				delete(c.moveTableJobs, tableID)
				log.Warn("ignored the move job, the table is not exist in the source capture", zap.Reflect("job", job))
				continue
			}
			replicaInfo.StartTs = c.status.CheckpointTs
			job.TableReplicaInfo = replicaInfo
			job.Status = model.MoveTableStatusDeleted
			log.Info("handle the move job, remove table from the source capture", zap.Reflect("job", job))
		case model.MoveTableStatusDeleted:
			// add table to target capture
			status, exist := cloneStatus(job.To)
			if !exist {
				// the target capture is not exist, add table to orphanTables.
				c.orphanTables[tableID] = job.TableReplicaInfo.StartTs
				log.Warn("the target capture is not exist, sent the table to orphanTables", zap.Reflect("job", job))
				continue
			}
			status.AddTable(tableID, job.TableReplicaInfo, job.TableReplicaInfo.StartTs)
			job.Status = model.MoveTableStatusFinished
			delete(c.moveTableJobs, tableID)
			log.Info("handle the move job, add table to the target capture", zap.Reflect("job", job))
		}
	}
	err := c.updateTaskStatus(ctx, newTaskStatus)
	return errors.Trace(err)
}

func (c *changeFeed) applyJob(ctx context.Context, job *timodel.Job) (skip bool, err error) {
	schemaID := job.SchemaID
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil && c.schema.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID) {
		tableID := job.BinlogInfo.TableInfo.ID
		if _, exist := c.tables[tableID]; exist {
			c.removeTable(schemaID, tableID, job.BinlogInfo.FinishedTS)
		}
		return true, nil
	}

	err = func() error {
		// case table id set may change
		switch job.Type {
		case timodel.ActionCreateSchema:
			c.addSchema(schemaID)
		case timodel.ActionDropSchema:
			c.dropSchema(schemaID, job.BinlogInfo.FinishedTS)
		case timodel.ActionCreateTable, timodel.ActionRecoverTable:
			addID := job.BinlogInfo.TableInfo.ID
			tableName, exist := c.schema.GetTableNameByID(job.BinlogInfo.TableInfo.ID)
			if !exist {
				return errors.NotFoundf("table(%d)", addID)
			}
			c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, tableName, job.BinlogInfo.TableInfo)
		case timodel.ActionDropTable:
			dropID := job.TableID
			c.removeTable(schemaID, dropID, job.BinlogInfo.FinishedTS)
		case timodel.ActionRenameTable:
			tableName, exist := c.schema.GetTableNameByID(job.TableID)
			if !exist {
				return errors.NotFoundf("table(%d)", job.TableID)
			}
			// no id change just update name
			c.tables[job.TableID] = tableName
		case timodel.ActionTruncateTable:
			dropID := job.TableID
			c.removeTable(schemaID, dropID, job.BinlogInfo.FinishedTS)

			tableName, exist := c.schema.GetTableNameByID(job.BinlogInfo.TableInfo.ID)
			if !exist {
				return errors.NotFoundf("table(%d)", job.BinlogInfo.TableInfo.ID)
			}
			addID := job.BinlogInfo.TableInfo.ID
			c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, tableName, job.BinlogInfo.TableInfo)
		case timodel.ActionTruncateTablePartition, timodel.ActionAddTablePartition, timodel.ActionDropTablePartition:
			c.updatePartition(job.BinlogInfo.TableInfo, job.BinlogInfo.FinishedTS)
		}
		return nil
	}()
	if err != nil {
		log.Error("failed to applyJob, start to print debug info", zap.Error(err))
		c.schema.PrintStatus(log.Error)
	}
	return false, err
}

// handleDDL check if we can change the status to be `ChangeFeedExecDDL` and execute the DDL asynchronously
// if the status is in ChangeFeedWaitToExecDDL.
// After executing the DDL successfully, the status will be changed to be ChangeFeedSyncDML.
func (c *changeFeed) handleDDL(ctx context.Context, captures map[string]*model.CaptureInfo) error {
	if c.ddlState != model.ChangeFeedWaitToExecDDL {
		return nil
	}
	if len(c.ddlJobHistory) == 0 {
		log.Fatal("ddl job history can not be empty in changefeed when should to execute DDL")
	}
	todoDDLJob := c.ddlJobHistory[0]

	// Check if all the checkpointTs of capture are achieving global resolvedTs(which is equal to todoDDLJob.FinishedTS)
	if len(c.taskStatus) > len(c.taskPositions) {
		return nil
	}

	if c.status.CheckpointTs != todoDDLJob.BinlogInfo.FinishedTS {
		log.Debug("wait checkpoint ts",
			zap.Uint64("checkpoint ts", c.status.CheckpointTs),
			zap.Uint64("finish ts", todoDDLJob.BinlogInfo.FinishedTS),
			zap.String("ddl query", todoDDLJob.Query))
		return nil
	}

	log.Info("apply job", zap.Stringer("job", todoDDLJob),
		zap.String("schema", todoDDLJob.SchemaName),
		zap.String("query", todoDDLJob.Query),
		zap.Uint64("ts", todoDDLJob.BinlogInfo.FinishedTS))

	err := c.schema.HandleDDL(todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.schema.FillSchemaName(todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}

	ddlEvent := new(model.DDLEvent)
	ddlEvent.FromJob(todoDDLJob)

	// Execute DDL Job asynchronously
	c.ddlState = model.ChangeFeedExecDDL

	// TODO consider some newly added DDL types such as `ActionCreateSequence`
	skip, err := c.applyJob(ctx, todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		log.Info("ddl job ignored", zap.String("changefeed", c.id), zap.Reflect("job", todoDDLJob))
		c.ddlJobHistory = c.ddlJobHistory[1:]
		c.ddlExecutedTs = todoDDLJob.BinlogInfo.FinishedTS
		c.ddlState = model.ChangeFeedSyncDML
		return nil
	}

	err = c.balanceOrphanTables(ctx, captures)
	if err != nil {
		return errors.Trace(err)
	}
	executed := false
	if !c.cyclicEnabled || c.info.Config.Cyclic.SyncDDL {
		ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
		log.Debug("DDL processed to make special features mysql-compatible", zap.String("query", ddlEvent.Query))
		err = c.sink.EmitDDLEvent(ctx, ddlEvent)
		// If DDL executing failed, pause the changefeed and print log, rather
		// than return an error and break the running of this owner.
		if err != nil {
			if errors.Cause(err) != model.ErrorDDLEventIgnored {
				c.ddlState = model.ChangeFeedDDLExecuteFailed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", c.id),
					zap.Error(err),
					zap.Reflect("ddlJob", todoDDLJob))
				return errors.Trace(model.ErrExecDDLFailed)
			}
		} else {
			executed = true
		}
	}
	if executed {
		log.Info("Execute DDL succeeded", zap.String("changefeed", c.id), zap.Reflect("ddlJob", todoDDLJob))
	} else {
		log.Info("Execute DDL ignored", zap.String("changefeed", c.id), zap.Reflect("ddlJob", todoDDLJob))
	}

	c.ddlJobHistory = c.ddlJobHistory[1:]
	c.ddlExecutedTs = todoDDLJob.BinlogInfo.FinishedTS
	c.ddlState = model.ChangeFeedSyncDML
	return nil
}

// calcResolvedTs update every changefeed's resolve ts and checkpoint ts.
func (c *changeFeed) calcResolvedTs(ctx context.Context) error {
	if c.ddlState != model.ChangeFeedSyncDML && c.ddlState != model.ChangeFeedWaitToExecDDL {
		log.Debug("skip update resolved ts", zap.String("ddlState", c.ddlState.String()))
		return nil
	}

	minResolvedTs := c.targetTs
	minCheckpointTs := c.targetTs

	prevMinResolvedTs := c.targetTs
	prevMinCheckpointTs := c.targetTs
	checkUpdateTs := func() {
		if prevMinCheckpointTs != minCheckpointTs {
			log.L().WithOptions(zap.AddCallerSkip(1)).Debug("min checkpoint updated",
				zap.Uint64("prevMinCheckpointTs", prevMinCheckpointTs),
				zap.Uint64("minCheckpointTs", minCheckpointTs))
			prevMinCheckpointTs = minCheckpointTs
		}
		if prevMinResolvedTs != minResolvedTs {
			log.L().WithOptions(zap.AddCallerSkip(1)).Debug("min resolved updated",
				zap.Uint64("prevMinResolvedTs", prevMinResolvedTs),
				zap.Uint64("minResolvedTs", minResolvedTs))
			prevMinResolvedTs = minResolvedTs
		}
	}

	if len(c.taskPositions) < len(c.taskStatus) {
		log.Debug("skip update resolved ts",
			zap.Int("taskPositions", len(c.taskPositions)),
			zap.Int("taskStatus", len(c.taskStatus)))
		return nil
	}
	if len(c.taskPositions) == 0 {
		minCheckpointTs = c.status.ResolvedTs
	} else {
		// calc the min of all resolvedTs in captures
		for _, position := range c.taskPositions {
			if minResolvedTs > position.ResolvedTs {
				minResolvedTs = position.ResolvedTs
			}

			if minCheckpointTs > position.CheckPointTs {
				minCheckpointTs = position.CheckPointTs
			}
		}
	}
	checkUpdateTs()

	for captureID, status := range c.taskStatus {
		appliedTs := status.AppliedTs()
		if minCheckpointTs > appliedTs {
			minCheckpointTs = appliedTs
		}
		if minResolvedTs > appliedTs {
			minResolvedTs = appliedTs
		}
		if appliedTs != math.MaxUint64 {
			log.Info("some operation is still unapplied",
				zap.String("captureID", captureID),
				zap.Uint64("appliedTs", appliedTs),
				zap.Stringer("status", status))
		}
	}
	checkUpdateTs()

	for _, startTs := range c.orphanTables {
		if minCheckpointTs > startTs {
			minCheckpointTs = startTs
		}
		if minResolvedTs > startTs {
			minResolvedTs = startTs
		}
	}
	checkUpdateTs()

	for _, targetTs := range c.toCleanTables {
		if minCheckpointTs > targetTs {
			minCheckpointTs = targetTs
		}
		if minResolvedTs > targetTs {
			minResolvedTs = targetTs
		}
	}
	checkUpdateTs()

	// if minResolvedTs is greater than ddlResolvedTs,
	// it means that ddlJobHistory in memory is not intact,
	// there are some ddl jobs which finishedTs is smaller than minResolvedTs we don't know.
	// so we need to call `pullDDLJob`, update the ddlJobHistory and ddlResolvedTs.
	if minResolvedTs > c.ddlResolvedTs {
		if err := c.pullDDLJob(); err != nil {
			return errors.Trace(err)
		}

		if minResolvedTs > c.ddlResolvedTs {
			minResolvedTs = c.ddlResolvedTs
		}
	}

	// if minResolvedTs is greater than the finishedTS of ddl job which is not executed,
	// we need to execute this ddl job
	for len(c.ddlJobHistory) > 0 && c.ddlJobHistory[0].BinlogInfo.FinishedTS <= c.ddlExecutedTs {
		c.ddlJobHistory = c.ddlJobHistory[1:]
	}
	if len(c.ddlJobHistory) > 0 && minResolvedTs > c.ddlJobHistory[0].BinlogInfo.FinishedTS {
		minResolvedTs = c.ddlJobHistory[0].BinlogInfo.FinishedTS
		c.ddlState = model.ChangeFeedWaitToExecDDL
	}

	// if downstream sink is the MQ sink, the MQ sink do not promise that checkpoint is less than globalResolvedTs
	if minCheckpointTs > minResolvedTs {
		minCheckpointTs = minResolvedTs
	}
	checkUpdateTs()

	var tsUpdated bool

	if minResolvedTs > c.status.ResolvedTs {
		c.status.ResolvedTs = minResolvedTs
		tsUpdated = true
	}

	if minCheckpointTs > c.status.CheckpointTs {
		c.status.CheckpointTs = minCheckpointTs
		// when the `c.ddlState` is `model.ChangeFeedWaitToExecDDL`,
		// some DDL is waiting to executed, we can't ensure whether the DDL has been executed.
		// so we can't emit checkpoint to sink
		if c.ddlState != model.ChangeFeedWaitToExecDDL {
			err := c.sink.EmitCheckpointTs(ctx, minCheckpointTs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		tsUpdated = true
	}
	checkUpdateTs()

	if tsUpdated {
		log.Debug("update changefeed", zap.String("id", c.id),
			zap.Uint64("checkpoint ts", c.status.CheckpointTs),
			zap.Uint64("resolved ts", c.status.ResolvedTs))
	}
	return nil
}

func (c *changeFeed) pullDDLJob() error {
	ddlResolvedTs, ddlJobs, err := c.ddlHandler.PullDDL()
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlResolvedTs = ddlResolvedTs
	for _, ddl := range ddlJobs {
		if c.filter.ShouldDiscardDDL(ddl.Type) {
			log.Info("discard the ddl job", zap.Int64("jobID", ddl.ID), zap.String("query", ddl.Query))
			continue
		}
		c.ddlJobHistory = append(c.ddlJobHistory, ddl)
	}
	return nil
}
