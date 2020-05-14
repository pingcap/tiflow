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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/filter"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

type tableIDMap = map[uint64]struct{}

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

	// GetAllTaskPositions queries all task positions of a changefeed, and returns a map
	// mapping from captureID to TaskPositions
	GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error)

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
	taskPositions map[string]*model.TaskPosition
	filter        *filter.Filter
	sink          sink.Sink

	cyclicEnabled bool

	ddlHandler    OwnerDDLHandler
	ddlResolvedTs uint64
	ddlJobHistory []*timodel.Job
	ddlExecutedTs uint64

	schemas              map[uint64]tableIDMap
	tables               map[uint64]entry.TableName
	orphanTables         map[uint64]model.ProcessTableInfo
	waitingConfirmTables map[uint64]string
	toCleanTables        map[uint64]struct{}
	infoWriter           *storage.OwnerTaskStatusEtcdWriter
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

func (c *changeFeed) addSchema(schemaID uint64) {
	if _, ok := c.schemas[schemaID]; ok {
		log.Warn("add schema already exists", zap.Uint64("schemaID", schemaID))
		return
	}
	c.schemas[schemaID] = make(map[uint64]struct{})
}

func (c *changeFeed) dropSchema(schemaID uint64) {
	if schema, ok := c.schemas[schemaID]; ok {
		for tid := range schema {
			c.removeTable(schemaID, tid)
		}
	}
	delete(c.schemas, schemaID)
}

func (c *changeFeed) addTable(sid, tid, startTs uint64, table entry.TableName) {
	if c.filter.ShouldIgnoreTable(table.Schema, table.Table) {
		return
	}

	if _, ok := c.tables[tid]; ok {
		log.Warn("add table already exists", zap.Uint64("tableID", tid), zap.Stringer("table", table))
		return
	}

	if _, ok := c.schemas[sid]; !ok {
		c.schemas[sid] = make(tableIDMap)
	}
	c.schemas[sid][tid] = struct{}{}
	c.tables[tid] = table
	c.orphanTables[tid] = model.ProcessTableInfo{
		ID:      tid,
		StartTs: startTs,
	}
}

func (c *changeFeed) removeTable(sid, tid uint64) {
	if _, ok := c.schemas[sid]; ok {
		delete(c.schemas[sid], tid)
	}
	delete(c.tables, tid)

	if _, ok := c.orphanTables[tid]; ok {
		delete(c.orphanTables, tid)
	} else {
		c.toCleanTables[tid] = struct{}{}
	}
}

func (c *changeFeed) selectCapture(captures map[string]*model.CaptureInfo, toAppend map[string][]*model.ProcessTableInfo) string {
	return c.minimumTablesCapture(captures, toAppend)
}

func (c *changeFeed) minimumTablesCapture(captures map[string]*model.CaptureInfo, toAppend map[string][]*model.ProcessTableInfo) string {
	if len(captures) == 0 {
		return ""
	}

	var minID string
	minCount := math.MaxInt64
	for id := range captures {
		var tableCount int
		if pinfo, ok := c.taskStatus[id]; ok {
			tableCount += len(pinfo.TableInfos)
		}
		if append, ok := toAppend[id]; ok {
			tableCount += len(append)
		}
		if tableCount < minCount {
			minID = id
			minCount = tableCount
		}
	}

	return minID
}

func (c *changeFeed) tryBalance(ctx context.Context, captures map[string]*model.CaptureInfo) error {
	c.cleanTables(ctx)
	return c.balanceOrphanTables(ctx, captures)
}

func (c *changeFeed) restoreTableInfos(infoSnapshot *model.TaskStatus, captureID string) {
	// the capture information maybe deleted during table cleaning
	if _, ok := c.taskStatus[captureID]; !ok {
		log.Warn("ignore restore table info, task status for capture not found", zap.String("captureID", captureID))
	}
	c.taskStatus[captureID].TableInfos = infoSnapshot.TableInfos
}

func (c *changeFeed) cleanTables(ctx context.Context) {
	var cleanIDs []uint64

cleanLoop:
	for id := range c.toCleanTables {
		captureID, taskStatus, ok := findTaskStatusWithTable(c.taskStatus, id)
		if !ok {
			log.Warn("ignore clean table id", zap.Uint64("id", id))
			cleanIDs = append(cleanIDs, id)
			continue
		}

		infoClone := taskStatus.Clone()
		taskStatus.RemoveTable(id)

		newInfo, err := c.infoWriter.Write(ctx, c.id, captureID, taskStatus, true)
		if err == nil {
			c.taskStatus[captureID] = newInfo
		}
		switch errors.Cause(err) {
		case model.ErrFindPLockNotCommit:
			c.restoreTableInfos(infoClone, captureID)
			log.Info("write table info delay, wait plock resolve",
				zap.String("changefeed", c.id),
				zap.String("capture", captureID))
		case nil:
			log.Info("cleanup table success",
				zap.Uint64("table id", id),
				zap.String("capture id", captureID))
			log.Debug("after remove", zap.Stringer("task status", taskStatus))
			cleanIDs = append(cleanIDs, id)
		default:
			c.restoreTableInfos(infoClone, captureID)
			log.Error("fail to put sub changefeed info", zap.Error(err))
			break cleanLoop
		}
	}

	for _, id := range cleanIDs {
		delete(c.toCleanTables, id)
	}
}

func findTaskStatusWithTable(infos model.ProcessorsInfos, tableID uint64) (captureID string, info *model.TaskStatus, ok bool) {
	for id, info := range infos {
		for _, table := range info.TableInfos {
			if table.ID == tableID {
				return id, info, true
			}
		}
	}

	return "", nil, false
}

func (c *changeFeed) balanceOrphanTables(ctx context.Context, captures map[string]*model.CaptureInfo) error {
	if len(captures) == 0 {
		return nil
	}

	for tableID, captureID := range c.waitingConfirmTables {
		lockStatus, err := c.infoWriter.CheckLock(ctx, c.id, captureID)
		if err != nil {
			return errors.Trace(err)
		}
		switch lockStatus {
		case model.TableNoLock:
			log.Debug("no c-lock", zap.Uint64("tableID", tableID), zap.String("captureID", captureID))
			delete(c.waitingConfirmTables, tableID)
		case model.TablePLock:
			log.Debug("waiting the c-lock", zap.Uint64("tableID", tableID), zap.String("captureID", captureID))
		case model.TablePLockCommited:
			log.Debug("delete the c-lock", zap.Uint64("tableID", tableID), zap.String("captureID", captureID))
			delete(c.waitingConfirmTables, tableID)
		}
	}

	appendTableInfos := make(map[string][]*model.ProcessTableInfo, len(captures))
	schemaSnapshot := c.schema
	for tableID, orphan := range c.orphanTables {
		var orphanMarkTable *model.ProcessTableInfo
		if c.cyclicEnabled {
			tableName, found := schemaSnapshot.GetTableNameByID(int64(tableID))
			if !found || cyclic.IsMarkTable(tableName.Schema, tableName.Table) {
				// Skip, mark tables should not be balanced alone.
				continue
			}
			// TODO(neil) we could just use c.tables.
			markTableSchameName, markTableTableName := cyclic.MarkTableName(tableName.Schema, tableName.Table)
			id, found := schemaSnapshot.GetTableIDByName(markTableSchameName, markTableTableName)
			if !found {
				// Mark table is not created yet, skip and wait.
				log.Info("balance table info delay, wait mark table",
					zap.String("changefeed", c.id),
					zap.Uint64("tableID", tableID),
					zap.String("markTableName", markTableTableName))
				continue
			}
			orphanMarkTable = &model.ProcessTableInfo{
				ID:      uint64(id),
				StartTs: orphan.StartTs,
			}
		}

		captureID := c.selectCapture(captures, appendTableInfos)
		if len(captureID) == 0 {
			return nil
		}
		info := appendTableInfos[captureID]
		info = append(info, &model.ProcessTableInfo{
			ID:      tableID,
			StartTs: orphan.StartTs,
		})
		// Table and mark table must be balanced to the same capture.
		if orphanMarkTable != nil {
			info = append(info, orphanMarkTable)
		}
		appendTableInfos[captureID] = info
	}

	for captureID, tableInfos := range appendTableInfos {
		status := c.taskStatus[captureID]
		if status == nil {
			status = new(model.TaskStatus)
		}
		statusClone := status.Clone()
		status.TableInfos = append(status.TableInfos, tableInfos...)

		newInfo, err := c.infoWriter.Write(ctx, c.id, captureID, status, true)
		if err == nil {
			c.taskStatus[captureID] = newInfo
		}
		switch errors.Cause(err) {
		case model.ErrFindPLockNotCommit:
			c.restoreTableInfos(statusClone, captureID)
			log.Info("write table info delay, wait plock resolve",
				zap.String("changefeed", c.id),
				zap.String("capture", captureID))
		case nil:
			log.Info("dispatch table success",
				zap.Reflect("tableInfos", tableInfos),
				zap.String("capture", captureID))
			for _, tableInfo := range tableInfos {
				delete(c.orphanTables, tableInfo.ID)
				c.waitingConfirmTables[tableInfo.ID] = captureID
			}
		default:
			c.restoreTableInfos(statusClone, captureID)
			log.Error("fail to put sub changefeed info", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *changeFeed) applyJob(ctx context.Context, job *timodel.Job) (skip bool, err error) {
	schemaID := uint64(job.SchemaID)
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil && c.schema.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID) {
		tableID := uint64(job.BinlogInfo.TableInfo.ID)
		if _, exist := c.tables[tableID]; exist {
			c.removeTable(schemaID, tableID)
		}
		return true, nil
	}

	err = func() error {
		// case table id set may change
		switch job.Type {
		case timodel.ActionCreateSchema:
			c.addSchema(schemaID)
		case timodel.ActionDropSchema:
			c.dropSchema(schemaID)
		case timodel.ActionCreateTable, timodel.ActionRecoverTable:
			addID := uint64(job.BinlogInfo.TableInfo.ID)
			tableName, exist := c.schema.GetTableNameByID(job.BinlogInfo.TableInfo.ID)
			if !exist {
				return errors.NotFoundf("table(%d)", addID)
			}
			c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, tableName)
		case timodel.ActionDropTable:
			dropID := uint64(job.TableID)
			c.removeTable(schemaID, dropID)
		case timodel.ActionRenameTable:
			tableName, exist := c.schema.GetTableNameByID(job.TableID)
			if !exist {
				return errors.NotFoundf("table(%d)", job.TableID)
			}
			// no id change just update name
			c.tables[uint64(job.TableID)] = tableName
		case timodel.ActionTruncateTable:
			dropID := uint64(job.TableID)
			c.removeTable(schemaID, dropID)

			tableName, exist := c.schema.GetTableNameByID(job.BinlogInfo.TableInfo.ID)
			if !exist {
				return errors.NotFoundf("table(%d)", job.BinlogInfo.TableInfo.ID)
			}
			addID := uint64(job.BinlogInfo.TableInfo.ID)
			c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, tableName)
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
		c.ddlJobHistory = c.ddlJobHistory[1:]
		c.ddlExecutedTs = todoDDLJob.BinlogInfo.FinishedTS
		c.ddlState = model.ChangeFeedSyncDML
		return nil
	}

	err = c.balanceOrphanTables(ctx, captures)
	if err != nil {
		return errors.Trace(err)
	}

	err = c.sink.EmitDDLEvent(ctx, ddlEvent)
	// If DDL executing failed, pause the changefeed and print log, rather
	// than return an error and break the running of this owner.
	if err != nil {
		c.ddlState = model.ChangeFeedDDLExecuteFailed
		log.Error("Execute DDL failed",
			zap.String("ChangeFeedID", c.id),
			zap.Error(err),
			zap.Reflect("ddlJob", todoDDLJob))
		return errors.Trace(model.ErrExecDDLFailed)
	}
	log.Info("Execute DDL succeeded",
		zap.String("ChangeFeedID", c.id),
		zap.Reflect("ddlJob", todoDDLJob))

	if c.ddlState != model.ChangeFeedExecDDL {
		log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
			zap.String("ChangeFeedID", c.id),
			zap.String("ChangeFeedDDLState", c.ddlState.String()))
	}
	c.ddlJobHistory = c.ddlJobHistory[1:]
	c.ddlExecutedTs = todoDDLJob.BinlogInfo.FinishedTS
	c.ddlState = model.ChangeFeedSyncDML
	return nil
}

// calcResolvedTs update every changefeed's resolve ts and checkpoint ts.
func (c *changeFeed) calcResolvedTs(ctx context.Context) error {
	if c.ddlState != model.ChangeFeedSyncDML && c.ddlState != model.ChangeFeedWaitToExecDDL {
		return nil
	}

	// ProcessorInfos don't contains the whole set table id now.
	if len(c.waitingConfirmTables) > 0 {
		log.Debug("skip calcResolvedTs",
			zap.Reflect("waitingConfirmTables", c.orphanTables))
		return nil
	}

	minResolvedTs := c.targetTs
	minCheckpointTs := c.targetTs

	if len(c.taskPositions) < len(c.taskStatus) {
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

	for _, orphanTable := range c.orphanTables {
		if minCheckpointTs > orphanTable.StartTs {
			minCheckpointTs = orphanTable.StartTs
		}
		if minResolvedTs > orphanTable.StartTs {
			minResolvedTs = orphanTable.StartTs
		}
	}

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
