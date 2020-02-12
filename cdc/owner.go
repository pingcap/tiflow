// Copyright 2019 PingCAP, Inc.
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
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pmodel "github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	markProcessorDownTime      = time.Minute
	captureInfoWatchRetryDelay = time.Millisecond * 500
)

type tableIDMap = map[uint64]struct{}

// OwnerDDLHandler defines the ddl handler for Owner
// which can pull ddl jobs and execute ddl jobs
type OwnerDDLHandler interface {
	// PullDDL pulls the ddl jobs and returns resolvedTs of DDL Puller and job list.
	PullDDL() (resolvedTs uint64, jobs []*model.DDL, err error)

	// ExecDDL executes the ddl job
	ExecDDL(ctx context.Context, sinkURI string, txn model.Txn) error

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
	GetChangeFeedStatus(ctx context.Context, id string) (*model.ChangeFeedStatus, error)
	// PutAllChangeFeedStatus the changefeed info to storage such as etcd.
	PutAllChangeFeedStatus(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error
}

type changeFeed struct {
	id     string
	info   *model.ChangeFeedInfo
	status *model.ChangeFeedStatus

	schema                  *schema.Storage
	ddlState                model.ChangeFeedDDLState
	targetTs                uint64
	taskStatus              model.ProcessorsInfos
	taskPositions           map[string]*model.TaskPosition
	processorLastUpdateTime map[string]time.Time
	filter                  *txnFilter

	ddlHandler    OwnerDDLHandler
	ddlResolvedTs uint64
	ddlJobHistory []*model.DDL

	schemas       map[uint64]tableIDMap
	tables        map[uint64]schema.TableName
	orphanTables  map[uint64]model.ProcessTableInfo
	toCleanTables map[uint64]struct{}
	infoWriter    *storage.OwnerTaskStatusEtcdWriter
}

// String implements fmt.Stringer interface.
func (c *changeFeed) String() string {
	format := "{\n ID: %s\n info: %+v\n status: %+v\n State: %v\n ProcessorInfos: %+v\n tables: %+v\n orphanTables: %+v\n toCleanTables: %v\n ddlResolvedTs: %d\n ddlJobHistory: %+v\n}\n\n"
	s := fmt.Sprintf(format,
		c.id, c.info, c.status, c.ddlState, c.taskStatus, c.tables,
		c.orphanTables, c.toCleanTables, c.ddlResolvedTs, c.ddlJobHistory)

	if len(c.ddlJobHistory) > 0 {
		job := c.ddlJobHistory[0]
		s += fmt.Sprintf("next to exec job: %s query: %s\n\n", job, job.Job.Query)
	}

	return s
}

func (c *changeFeed) updateProcessorInfos(processInfos model.ProcessorsInfos, positions map[string]*model.TaskPosition) {
	for cid, position := range positions {
		if _, ok := c.processorLastUpdateTime[cid]; !ok {
			c.processorLastUpdateTime[cid] = time.Now()
			continue
		}

		oldPosition, ok := c.taskPositions[cid]
		if !ok || oldPosition.ResolvedTs != position.ResolvedTs || oldPosition.CheckPointTs != position.CheckPointTs {
			c.processorLastUpdateTime[cid] = time.Now()
		}
	}

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

func (c *changeFeed) reAddTable(id, startTs uint64) {
	c.orphanTables[id] = model.ProcessTableInfo{
		ID:      id,
		StartTs: startTs,
	}
}

func (c *changeFeed) addTable(sid, tid, startTs uint64, table schema.TableName) {
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

func (c *changeFeed) selectCapture(captures map[string]*model.CaptureInfo) string {
	return c.minimumTablesCapture(captures)
}

func (c *changeFeed) minimumTablesCapture(captures map[string]*model.CaptureInfo) string {
	if len(captures) == 0 {
		return ""
	}

	for id := range captures {
		// We have not dispatch any table to this capture yet.
		if _, ok := c.taskStatus[id]; !ok {
			return id
		}
	}

	var minCount int = math.MaxInt64
	var minID string

	for id, pinfo := range c.taskStatus {
		if len(pinfo.TableInfos) < minCount {
			minID = id
			minCount = len(pinfo.TableInfos)
		}
	}

	return minID
}

func (c *changeFeed) tryBalance(ctx context.Context, captures map[string]*model.CaptureInfo) {
	c.cleanTables(ctx)
	c.banlanceOrphanTables(ctx, captures)
}

func (c *changeFeed) restoreTableInfos(infoSnapshot *model.TaskStatus, captureID string) {
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

func (c *changeFeed) banlanceOrphanTables(ctx context.Context, captures map[string]*model.CaptureInfo) {
	if len(captures) == 0 {
		return
	}

	for tableID, orphan := range c.orphanTables {
		captureID := c.selectCapture(captures)
		if len(captureID) == 0 {
			return
		}

		info := c.taskStatus[captureID]
		if info == nil {
			info = new(model.TaskStatus)
		}
		infoClone := info.Clone()
		info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{
			ID:      tableID,
			StartTs: orphan.StartTs,
		})

		newInfo, err := c.infoWriter.Write(ctx, c.id, captureID, info, false)
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
			log.Info("dispatch table success",
				zap.Uint64("table id", tableID),
				zap.Uint64("start ts", orphan.StartTs),
				zap.String("capture", captureID))
			delete(c.orphanTables, tableID)
		default:
			c.restoreTableInfos(infoClone, captureID)
			log.Error("fail to put sub changefeed info", zap.Error(err))
			return
		}
	}
}

func (c *changeFeed) applyJob(job *pmodel.Job) error {
	log.Info("apply job", zap.String("sql", job.Query), zap.Int64("job id", job.ID))

	schamaName, tableName, _, err := c.schema.HandleDDL(job)
	if err != nil {
		return errors.Trace(err)
	}

	schemaID := uint64(job.SchemaID)
	// case table id set may change
	switch job.Type {
	case pmodel.ActionCreateSchema:
		c.addSchema(schemaID)
	case pmodel.ActionDropSchema:
		c.dropSchema(schemaID)
	case pmodel.ActionCreateTable, pmodel.ActionRecoverTable:
		addID := uint64(job.BinlogInfo.TableInfo.ID)
		c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, schema.TableName{Schema: schamaName, Table: tableName})
	case pmodel.ActionDropTable:
		dropID := uint64(job.TableID)
		c.removeTable(schemaID, dropID)
	case pmodel.ActionRenameTable:
		// no id change just update name
		c.tables[uint64(job.TableID)] = schema.TableName{Schema: schamaName, Table: tableName}
	case pmodel.ActionTruncateTable:
		dropID := uint64(job.TableID)
		c.removeTable(schemaID, dropID)

		addID := uint64(job.BinlogInfo.TableInfo.ID)
		c.addTable(schemaID, addID, job.BinlogInfo.FinishedTS, schema.TableName{Schema: schamaName, Table: tableName})
	default:
	}

	return nil
}

type ownerImpl struct {
	changeFeeds       map[model.ChangeFeedID]*changeFeed
	markDownProcessor []*model.ProcInfoSnap

	cfRWriter ChangeFeedRWriter

	l sync.RWMutex

	pdEndpoints []string
	pdClient    pd.Client
	etcdClient  kv.CDCEtcdClient
	manager     roles.Manager

	captureWatchC      <-chan *CaptureInfoWatchResp
	cancelWatchCapture func()
	captures           map[model.CaptureID]*model.CaptureInfo

	adminJobs     []model.AdminJob
	adminJobsLock sync.Mutex
}

// NewOwner creates a new ownerImpl instance
func NewOwner(pdEndpoints []string, cli kv.CDCEtcdClient, manager roles.Manager) (*ownerImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())
	infos, watchC, err := newCaptureInfoWatch(ctx, cli)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	captures := make(map[model.CaptureID]*model.CaptureInfo, len(infos))
	for _, info := range infos {
		captures[info.ID] = info
	}

	pdClient, err := pd.NewClient(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	owner := &ownerImpl{
		pdEndpoints:        pdEndpoints,
		pdClient:           pdClient,
		changeFeeds:        make(map[model.ChangeFeedID]*changeFeed),
		cfRWriter:          cli,
		etcdClient:         cli,
		manager:            manager,
		captureWatchC:      watchC,
		captures:           captures,
		cancelWatchCapture: cancel,
	}

	return owner, nil
}

func (o *ownerImpl) addCapture(info *model.CaptureInfo) {
	o.l.Lock()
	o.captures[info.ID] = info
	o.l.Unlock()
}

func (o *ownerImpl) handleMarkdownProcessor(ctx context.Context) {
	var deletedCapture = make(map[string]struct{})
	remainProcs := make([]*model.ProcInfoSnap, 0)
	for _, snap := range o.markDownProcessor {
		changefeed, ok := o.changeFeeds[snap.CfID]
		if !ok {
			log.Error("changefeed not found in owner cache, can't rebalance",
				zap.String("changefeedID", snap.CfID))
			continue
		}
		for _, tbl := range snap.Tables {
			changefeed.reAddTable(tbl.ID, tbl.StartTs)
		}
		err := o.etcdClient.DeleteTaskStatus(ctx, snap.CfID, snap.CaptureID)
		if err != nil {
			log.Warn("failed to delete processor info",
				zap.String("changefeedID", snap.CfID),
				zap.String("captureID", snap.CaptureID),
				zap.Error(err),
			)
			remainProcs = append(remainProcs, snap)
			continue
		}
		err = o.etcdClient.DeleteTaskPosition(ctx, snap.CfID, snap.CaptureID)
		if err != nil {
			log.Warn("failed to delete task position",
				zap.String("changefeedID", snap.CfID),
				zap.String("captureID", snap.CaptureID),
				zap.Error(err),
			)
			remainProcs = append(remainProcs, snap)
			continue
		}
		deletedCapture[snap.CaptureID] = struct{}{}
	}
	o.markDownProcessor = remainProcs

	for id := range deletedCapture {
		err := o.etcdClient.DeleteCaptureInfo(ctx, id)
		if err != nil {
			log.Warn("failed to delete capture info", zap.Error(err))
		}
	}
}

func (o *ownerImpl) removeCapture(info *model.CaptureInfo) {
	o.l.Lock()
	defer o.l.Unlock()

	delete(o.captures, info.ID)

	for _, feed := range o.changeFeeds {
		pinfo, ok := feed.taskStatus[info.ID]
		if !ok {
			continue
		}
		pos, ok := feed.taskPositions[info.ID]
		if !ok {
			continue
		}

		for _, table := range pinfo.TableInfos {
			feed.orphanTables[table.ID] = model.ProcessTableInfo{
				ID:      table.ID,
				StartTs: pos.CheckPointTs,
			}
		}

		ctx := context.TODO()
		if err := o.etcdClient.DeleteTaskStatus(ctx, feed.id, info.ID); err != nil {
			log.Warn("failed to delete task status", zap.Error(err))
		}
		if err := o.etcdClient.DeleteTaskPosition(ctx, feed.id, info.ID); err != nil {
			log.Warn("failed to delete task position", zap.Error(err))
		}

	}
}

func (o *ownerImpl) resetCaptureInfoWatcher(ctx context.Context) error {
	infos, watchC, err := newCaptureInfoWatch(ctx, o.etcdClient)
	if err != nil {
		return errors.Trace(err)
	}
	for _, info := range infos {
		// use addCapture is ok, old info will be covered
		o.addCapture(info)
	}
	o.captureWatchC = watchC
	return nil
}

func (o *ownerImpl) handleWatchCapture() error {
	for resp := range o.captureWatchC {
		if resp.Err != nil {
			return errors.Trace(resp.Err)
		}

		if resp.IsDelete {
			o.removeCapture(resp.Info)
		} else {
			o.addCapture(resp.Info)
		}
	}

	log.Info("handleWatchCapture quit")
	return nil
}

func (o *ownerImpl) newChangeFeed(id model.ChangeFeedID, processorsInfos model.ProcessorsInfos, taskPositions map[string]*model.TaskPosition, info *model.ChangeFeedInfo, checkpointTs uint64) (*changeFeed, error) {
	log.Info("Find new changefeed", zap.Reflect("info", info),
		zap.String("id", id), zap.Uint64("checkpoint ts", checkpointTs))

	schemaStorage, err := createSchemaStore(o.pdEndpoints)
	if err != nil {
		return nil, errors.Annotate(err, "create schema store failed")
	}

	err = schemaStorage.HandlePreviousDDLJobIfNeed(checkpointTs)
	if err != nil {
		return nil, errors.Annotate(err, "handle ddl job failed")
	}

	ddlHandler := newDDLHandler(o.pdClient, checkpointTs)

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

	filter, err := newTxnFilter(info.GetConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemas := make(map[uint64]tableIDMap)
	tables := make(map[uint64]schema.TableName)
	orphanTables := make(map[uint64]model.ProcessTableInfo)
	for tid, table := range schemaStorage.CloneTables() {
		if filter.ShouldIgnoreTable(table.Schema, table.Table) {
			continue
		}

		tables[tid] = table
		if ts, ok := existingTables[tid]; ok {
			log.Debug("ignore known table", zap.Uint64("tid", tid), zap.Stringer("table", table), zap.Uint64("ts", ts))
			continue
		}
		schema, ok := schemaStorage.SchemaByTableID(int64(tid))
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

	cf := &changeFeed{
		info:                    info,
		id:                      id,
		ddlHandler:              ddlHandler,
		schema:                  schemaStorage,
		schemas:                 schemas,
		tables:                  tables,
		orphanTables:            orphanTables,
		toCleanTables:           make(map[uint64]struct{}),
		processorLastUpdateTime: make(map[string]time.Time),
		status: &model.ChangeFeedStatus{
			ResolvedTs:   0,
			CheckpointTs: checkpointTs,
		},
		ddlState:      model.ChangeFeedSyncDML,
		targetTs:      info.GetTargetTs(),
		taskStatus:    processorsInfos,
		taskPositions: taskPositions,
		infoWriter:    storage.NewOwnerTaskStatusEtcdWriter(o.etcdClient),
		filter:        filter,
	}
	return cf, nil
}

func (o *ownerImpl) loadChangeFeeds(ctx context.Context) error {
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
			for id, info := range cf.taskStatus {
				lastUpdateTime, exist := cf.processorLastUpdateTime[id]
				if !exist {
					lastUpdateTime = time.Now()
					cf.processorLastUpdateTime[id] = lastUpdateTime
				}
				if time.Since(lastUpdateTime) > markProcessorDownTime {
					var checkpointTs uint64
					if pos, exist := taskPositions[id]; exist {
						checkpointTs = pos.CheckPointTs
					}
					snap := info.Snapshot(changeFeedID, id, checkpointTs)
					o.markDownProcessor = append(o.markDownProcessor, snap)
					log.Info("markdown processor", zap.String("id", id),
						zap.Reflect("info", info), zap.Time("update time", lastUpdateTime))
				}
			}
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

		newCf, err := o.newChangeFeed(changeFeedID, taskStatus, taskPositions, cfInfo, checkpointTs)
		if err != nil {
			return errors.Annotatef(err, "create change feed %s", changeFeedID)
		}
		o.changeFeeds[changeFeedID] = newCf
	}

	for _, changefeed := range o.changeFeeds {
		changefeed.tryBalance(ctx, o.captures)
	}

	return nil
}

func (o *ownerImpl) flushChangeFeedInfos(ctx context.Context) error {
	snapshot := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, len(o.changeFeeds))
	for id, changefeed := range o.changeFeeds {
		snapshot[id] = changefeed.status
	}
	return errors.Trace(o.cfRWriter.PutAllChangeFeedStatus(ctx, snapshot))
}

func (c *changeFeed) pullDDLJob() error {
	ddlResolvedTs, ddlJobs, err := c.ddlHandler.PullDDL()
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlResolvedTs = ddlResolvedTs
	c.ddlJobHistory = append(c.ddlJobHistory, ddlJobs...)
	return nil
}

// calcResolvedTs update every changefeed's resolve ts and checkpoint ts.
func (c *changeFeed) calcResolvedTs() error {
	if c.ddlState != model.ChangeFeedSyncDML {
		return nil
	}

	// ProcessorInfos don't contains the whole set table id now.
	if len(c.orphanTables) > 0 {
		return nil
	}

	minResolvedTs := c.targetTs
	minCheckpointTs := c.targetTs

	if len(c.tables) == 0 {
		minCheckpointTs = c.status.CheckpointTs
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
	if len(c.ddlJobHistory) > 0 && minResolvedTs > c.ddlJobHistory[0].Job.BinlogInfo.FinishedTS {
		minResolvedTs = c.ddlJobHistory[0].Job.BinlogInfo.FinishedTS
		c.ddlState = model.ChangeFeedWaitToExecDDL
	}

	var tsUpdated bool

	if minResolvedTs > c.status.ResolvedTs {
		c.status.ResolvedTs = minResolvedTs
		tsUpdated = true
	}

	if minCheckpointTs > c.status.CheckpointTs {
		c.status.CheckpointTs = minCheckpointTs
		tsUpdated = true
	}

	if tsUpdated {
		log.Debug("update changefeed", zap.String("id", c.id),
			zap.Uint64("checkpoint ts", minCheckpointTs),
			zap.Uint64("resolved ts", minResolvedTs))
	}
	return nil
}

// calcResolvedTs call calcResolvedTs of every changefeeds
func (o *ownerImpl) calcResolvedTs() error {
	for _, cf := range o.changeFeeds {
		if err := cf.calcResolvedTs(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleDDL call handleDDL of every changefeeds
func (o *ownerImpl) handleDDL(ctx context.Context) error {
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
	for cid, pInfo := range c.taskPositions {
		if pInfo.CheckPointTs != todoDDLJob.Job.BinlogInfo.FinishedTS {
			log.Debug("wait checkpoint ts", zap.String("cid", cid),
				zap.Uint64("checkpoint ts", pInfo.CheckPointTs),
				zap.Uint64("finish ts", todoDDLJob.Job.BinlogInfo.FinishedTS))
			return nil
		}
	}

	// Execute DDL Job asynchronously
	c.ddlState = model.ChangeFeedExecDDL
	log.Debug("apply job", zap.Stringer("job", todoDDLJob.Job),
		zap.String("query", todoDDLJob.Job.Query),
		zap.Uint64("ts", todoDDLJob.Job.BinlogInfo.FinishedTS))

	err := c.applyJob(todoDDLJob.Job)
	if err != nil {
		return errors.Trace(err)
	}

	c.banlanceOrphanTables(context.Background(), captures)
	ddlTxn := model.Txn{Ts: todoDDLJob.Job.BinlogInfo.FinishedTS, DDL: todoDDLJob}
	if c.filter.ShouldIgnoreTxn(&ddlTxn) {
		log.Info(
			"DDL txn ignored",
			zap.Int64("ID", todoDDLJob.Job.ID),
			zap.String("query", todoDDLJob.Job.Query),
			zap.Uint64("ts", ddlTxn.Ts),
		)
	} else {
		c.filter.FilterTxn(&ddlTxn)
		if ddlTxn.DDL == nil {
			log.Warn(
				"DDL ignored",
				zap.Int64("ID", todoDDLJob.Job.ID),
				zap.String("query", todoDDLJob.Job.Query),
				zap.Uint64("ts", todoDDLJob.Job.BinlogInfo.FinishedTS),
			)
		} else {
			err = c.ddlHandler.ExecDDL(ctx, c.info.SinkURI, ddlTxn)
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
		}
	}
	if c.ddlState != model.ChangeFeedExecDDL {
		log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
			zap.String("ChangeFeedID", c.id),
			zap.String("ChangeFeedDDLState", c.ddlState.String()))
	}
	c.ddlJobHistory = c.ddlJobHistory[1:]
	c.ddlState = model.ChangeFeedSyncDML
	return nil
}

// dispatchJob dispatches job to processors
func (o *ownerImpl) dispatchJob(ctx context.Context, job model.AdminJob) error {
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
	delete(o.changeFeeds, job.CfID)
	return nil
}

func (o *ownerImpl) handleAdminJob(ctx context.Context) error {
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

// TODO avoid this tick style, this means we get `tickTime` latency here.
func (o *ownerImpl) Run(ctx context.Context, tickTime time.Duration) error {
	defer o.cancelWatchCapture()
	handleWatchCaptureC := make(chan error, 1)
	rl := rate.NewLimiter(0.1, 5)
	go func() {
		var err error
		for {
			if !rl.Allow() {
				err = errors.New("capture info watcher exceeds rate limit")
				break
			}
			err = o.handleWatchCapture()
			if errors.Cause(err) != mvcc.ErrCompacted {
				break
			}
			log.Warn("capture info watcher retryable error", zap.Error(err))
			time.Sleep(captureInfoWatchRetryDelay)
			err = o.resetCaptureInfoWatcher(ctx)
			if err != nil {
				break
			}
		}
		if err != nil {
			handleWatchCaptureC <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-handleWatchCaptureC:
			return errors.Annotate(err, "handleWatchCapture failed")
		case <-time.After(tickTime):
			if !o.IsOwner(ctx) {
				continue
			}
			err := o.run(ctx)
			// owner may be evicted during running, ignore the context canceled error directly
			if err != nil && errors.Cause(err) != context.Canceled {
				return err
			}
		}
	}
}

func (o *ownerImpl) run(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-cctx.Done():
		case <-o.manager.RetireNotify():
			cancel()
		}
	}()

	o.l.Lock()
	defer o.l.Unlock()

	o.handleMarkdownProcessor(cctx)

	err := o.loadChangeFeeds(cctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.calcResolvedTs()
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleDDL(cctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.handleAdminJob(cctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.flushChangeFeedInfos(cctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (o *ownerImpl) IsOwner(_ context.Context) bool {
	return o.manager.IsOwner()
}

func (o *ownerImpl) EnqueueJob(job model.AdminJob) error {
	if !o.manager.IsOwner() {
		return errors.Trace(concurrency.ErrElectionNotLeader)
	}
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

func (o *ownerImpl) writeDebugInfo(w io.Writer) {
	for _, info := range o.changeFeeds {
		// fmt.Fprintf(w, "%+v\n", *info)
		fmt.Fprintf(w, "%s\n", info)
	}
}
