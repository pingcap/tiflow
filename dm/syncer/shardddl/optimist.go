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

package shardddl

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/schemacmp"
	filter "github.com/pingcap/tidb/util/table-filter"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/pkg/dm/message"
	dmproto "github.com/pingcap/tiflow/engine/pkg/dm/proto"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Optimist interface {
	Init(sourceTables map[string]map[string]map[string]map[string]struct{}) error
	Tables() [][]filter.Table
	Reset()
	ConstructInfo(upSchema, upTable, downSchema, downTable string,
		ddls []string, tiBefore *model.TableInfo, tisAfter []*model.TableInfo,
	) optimism.Info
	PutInfo(info optimism.Info) (int64, error)
	AddTable(info optimism.Info) (int64, error)
	RemoveTable(info optimism.Info) (int64, error)
	GetOperation(ctx context.Context, info optimism.Info, rev int64) (optimism.Operation, error)
	GetRedirectOperation(ctx context.Context, info optimism.Info, rev int64)
	DoneOperation(op optimism.Operation) error
	PendingInfo() *optimism.Info
	PendingOperation() *optimism.Operation
	PendingRedirectOperation() (*optimism.Operation, string)
	DoneRedirectOperation(targetTableID string)
}

// OptimistDM used to coordinate the shard DDL migration in optimism mode.
type OptimistDM struct {
	mu sync.RWMutex

	logger log.Logger
	cli    *clientv3.Client
	task   string
	source string

	tables optimism.SourceTables

	// the shard DDL info which is pending to handle.
	pendingInfo *optimism.Info
	// the shard DDL lock operation which is pending to handle.
	pendingOp *optimism.Operation
	// the shard DDL lock redirect operations which are pending to handle.
	// one target table -> one redirect operation
	pendingRedirectOps        map[string]*optimism.Operation
	pendingRedirectCancelFunc map[string]context.CancelFunc
}

// NewOptimist creates a new Optimist instance.
func NewOptimist(pLogger *log.Logger, cli *clientv3.Client, messageAgent message.MessageAgent, task, source, jobID string) Optimist {
	if messageAgent != nil {
		return NewOptimistEngine(pLogger, messageAgent, task, source, jobID)
	}
	return NewOptimistDM(pLogger, cli, task, source)
}

func NewOptimistDM(pLogger *log.Logger, cli *clientv3.Client, task, source string) *OptimistDM {
	return &OptimistDM{
		logger: pLogger.WithFields(zap.String("component", "shard DDL optimist")),
		cli:    cli,
		task:   task,
		source: source,
	}
}

// Init initializes the optimist with source tables.
// NOTE: this will PUT the initial source tables into etcd (and overwrite any previous existing tables).
// NOTE: we do not remove source tables for `stop-task` now, may need to handle it for `remove-meta`.
func (o *OptimistDM) Init(sourceTables map[string]map[string]map[string]map[string]struct{}) error {
	o.tables = optimism.NewSourceTables(o.task, o.source)
	for downSchema, downTables := range sourceTables {
		for downTable, upSchemas := range downTables {
			for upSchema, upTables := range upSchemas {
				for upTable := range upTables {
					o.tables.AddTable(upSchema, upTable, downSchema, downTable)
				}
			}
		}
	}
	_, err := optimism.PutSourceTables(o.cli, o.tables)
	return err
}

// Tables clone and return tables
// first one is sourceTable, second one is targetTable.
func (o *OptimistDM) Tables() [][]filter.Table {
	o.mu.Lock()
	defer o.mu.Unlock()

	tbls := make([][]filter.Table, 0)
	for downSchema, downTables := range o.tables.Tables {
		for downTable, upSchemas := range downTables {
			for upSchema, upTables := range upSchemas {
				for upTable := range upTables {
					tbls = append(tbls, []filter.Table{{Schema: upSchema, Name: upTable}, {Schema: downSchema, Name: downTable}})
				}
			}
		}
	}
	return tbls
}

// Reset resets the internal state of the optimist.
func (o *OptimistDM) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.pendingInfo = nil
	o.pendingOp = nil
	o.pendingRedirectOps = make(map[string]*optimism.Operation)
	o.pendingRedirectCancelFunc = make(map[string]context.CancelFunc)
}

// ConstructInfo constructs a shard DDL info.
func (o *OptimistDM) ConstructInfo(upSchema, upTable, downSchema, downTable string,
	ddls []string, tiBefore *model.TableInfo, tisAfter []*model.TableInfo,
) optimism.Info {
	return optimism.NewInfo(o.task, o.source, upSchema, upTable, downSchema, downTable, ddls, tiBefore, tisAfter)
}

// PutInfo puts the shard DDL info into etcd and returns the revision.
func (o *OptimistDM) PutInfo(info optimism.Info) (int64, error) {
	rev, err := optimism.PutInfo(o.cli, info)
	if err != nil {
		return 0, err
	}

	o.mu.Lock()
	o.pendingInfo = &info
	o.mu.Unlock()

	return rev, nil
}

// AddTable adds the table for the info into source tables,
// this is often called for `CREATE TABLE`.
func (o *OptimistDM) AddTable(info optimism.Info) (int64, error) {
	o.tables.AddTable(info.UpSchema, info.UpTable, info.DownSchema, info.DownTable)
	return optimism.PutSourceTables(o.cli, o.tables)
}

// RemoveTable removes the table for the info from source tables,
// this is often called for `DROP TABLE`.
func (o *OptimistDM) RemoveTable(info optimism.Info) (int64, error) {
	o.tables.RemoveTable(info.UpSchema, info.UpTable, info.DownSchema, info.DownTable)
	return optimism.PutSourceTables(o.cli, o.tables)
}

// GetOperation gets the shard DDL lock operation relative to the shard DDL info.
func (o *OptimistDM) GetOperation(ctx context.Context, info optimism.Info, rev int64) (optimism.Operation, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	ch := make(chan optimism.Operation, 1)
	errCh := make(chan error, 1)
	go optimism.WatchOperationPut(ctx2, o.cli, o.task, o.source, info.UpSchema, info.UpTable, rev, ch, errCh)

	select {
	case op := <-ch:
		o.mu.Lock()
		o.pendingOp = &op
		o.mu.Unlock()
		return op, nil
	case err := <-errCh:
		return optimism.Operation{}, err
	case <-ctx.Done():
		return optimism.Operation{}, ctx.Err()
	}
}

func (o *OptimistDM) GetRedirectOperation(ctx context.Context, info optimism.Info, rev int64) {
	ctx2, cancel2 := context.WithCancel(ctx)

	ch := make(chan optimism.Operation, 1)
	errCh := make(chan error, 1)
	targetTableID := utils.GenTableID(&filter.Table{Schema: info.DownSchema, Name: info.DownTable})
	o.mu.Lock()
	o.pendingRedirectCancelFunc[targetTableID] = cancel2
	o.mu.Unlock()

	go func() {
		o.logger.Info("start to wait redirect operation", zap.Stringer("info", info), zap.Int64("revision", rev))
		for {
			op, rev2, err := optimism.GetOperation(o.cli, o.task, o.source, info.UpSchema, info.UpTable)
			if err != nil {
				o.logger.Warn("fail to get redirect operation", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			// check whether operation is valid
			if op.Task == o.task && rev2 >= rev {
				switch op.ConflictStage {
				case optimism.ConflictResolved, optimism.ConflictNone:
					o.saveRedirectOperation(targetTableID, &op)
					return
				}
			}
			ctx3, cancel3 := context.WithCancel(ctx2)
			go optimism.WatchOperationPut(ctx3, o.cli, o.task, o.source, info.UpSchema, info.UpTable, rev2+1, ch, errCh)
			select {
			case op = <-ch:
				cancel3()
				switch op.ConflictStage {
				case optimism.ConflictResolved, optimism.ConflictNone:
					o.saveRedirectOperation(targetTableID, &op)
					return
				}
			case err := <-errCh:
				cancel3()
				o.logger.Warn("fail to watch redirect operation", zap.Error(err))
				time.Sleep(time.Second)
			case <-ctx.Done():
				cancel3()
				return
			}
		}
	}()
}

// DoneOperation marks the shard DDL lock operation as done.
func (o *OptimistDM) DoneOperation(op optimism.Operation) error {
	op.Done = true
	_, _, err := etcdutil.DoTxnWithRepeatable(o.cli, func(_ *tcontext.Context, cli *clientv3.Client) (interface{}, error) {
		_, _, err := optimism.PutOperation(cli, false, op, 0)
		return nil, err
	})
	if err != nil {
		return err
	}

	o.mu.Lock()
	o.pendingInfo = nil
	o.pendingOp = nil
	o.mu.Unlock()

	return nil
}

// PendingInfo returns the shard DDL info which is pending to handle.
func (o *OptimistDM) PendingInfo() *optimism.Info {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.pendingInfo == nil {
		return nil
	}
	info := *o.pendingInfo
	return &info
}

// PendingOperation returns the shard DDL lock operation which is pending to handle.
func (o *OptimistDM) PendingOperation() *optimism.Operation {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.pendingOp == nil {
		return nil
	}
	op := *o.pendingOp
	return &op
}

// PendingRedirectOperation returns the shard DDL lock redirect operation which is pending to handle.
func (o *OptimistDM) PendingRedirectOperation() (*optimism.Operation, string) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for targetTableID, op := range o.pendingRedirectOps {
		return op, targetTableID
	}
	return nil, ""
}

// saveRedirectOperation saves the redirect shard DDL lock operation.
func (o *OptimistDM) saveRedirectOperation(targetTableID string, op *optimism.Operation) {
	o.logger.Info("receive redirection operation from master", zap.Stringer("op", op))
	o.mu.Lock()
	if _, ok := o.pendingRedirectCancelFunc[targetTableID]; ok {
		o.pendingRedirectCancelFunc[targetTableID]()
		o.pendingRedirectOps[targetTableID] = op
	}
	o.mu.Unlock()
}

// DoneRedirectOperation marks the redirect shard DDL lock operation as done.
func (o *OptimistDM) DoneRedirectOperation(targetTableID string) {
	o.mu.Lock()
	if cancelFunc, ok := o.pendingRedirectCancelFunc[targetTableID]; ok {
		cancelFunc()
	}
	delete(o.pendingRedirectCancelFunc, targetTableID)
	delete(o.pendingRedirectOps, targetTableID)
	o.mu.Unlock()
}

// CheckPersistentData check and fix the persistent data.
//
// NOTE: currently this function is not used because user will meet error at early version
// if set unsupported case-sensitive.
func (o *OptimistDM) CheckPersistentData(source string, schemas map[string]string, tables map[string]map[string]string) error {
	if o.cli == nil {
		return nil
	}
	err := optimism.CheckSourceTables(o.cli, source, schemas, tables)
	if err != nil {
		return err
	}

	err = optimism.CheckDDLInfos(o.cli, source, schemas, tables)
	if err != nil {
		return err
	}

	err = optimism.CheckOperations(o.cli, source, schemas, tables)
	if err != nil {
		return err
	}

	return optimism.CheckColumns(o.cli, source, schemas, tables)
}

type OptimistEngine struct {
	mu     sync.RWMutex
	logger log.Logger

	task         string
	source       string
	jobID        string
	messageAgent message.MessageAgent

	// the shard DDL info which is pending to handle.
	pendingInfo *optimism.Info
	// the shard DDL lock operation which is pending to handle.
	pendingOp          *optimism.Operation
	pendingRedirectOps map[string]*optimism.Operation
	waitingRedirectOps map[metadata.SourceTable]struct{}
}

func NewOptimistEngine(pLogger *log.Logger, messageAgent message.MessageAgent, task, source, jobID string) *OptimistEngine {
	return &OptimistEngine{
		logger:       pLogger.WithFields(zap.String("component", "shard DDL optimist")),
		messageAgent: messageAgent,
		task:         task,
		source:       source,
		jobID:        jobID,
	}
}

func (o *OptimistEngine) Init(sourceTables map[string]map[string]map[string]map[string]struct{}) error {
	return nil
}

func (o *OptimistEngine) Tables() [][]filter.Table {
	return nil
}

func (o *OptimistEngine) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.pendingInfo = nil
	o.pendingOp = nil
	o.pendingRedirectOps = make(map[string]*optimism.Operation)
	o.waitingRedirectOps = make(map[metadata.SourceTable]struct{})
}

func (o *OptimistEngine) ConstructInfo(upSchema, upTable, downSchema, downTable string,
	ddls []string, tiBefore *model.TableInfo, tisAfter []*model.TableInfo,
) optimism.Info {
	return optimism.NewInfo(o.task, o.source, upSchema, upTable, downSchema, downTable, ddls, tiBefore, tisAfter)
}

func (o *OptimistEngine) PutInfo(info optimism.Info) (int64, error) {
	tables := make([]string, 0, len(info.TableInfosAfter)+1)
	tables = append(tables, schemacmp.Encode(info.TableInfoBefore).String())
	for _, ti := range info.TableInfosAfter {
		tables = append(tables, schemacmp.Encode(ti).String())
	}
	req := &dmproto.CoordinateDDLRequest{
		TargetTable: metadata.TargetTable{Schema: info.DownSchema, Table: info.DownTable},
		SourceTable: metadata.SourceTable{Source: info.Source, Schema: info.UpSchema, Table: info.UpTable},
		Tables:      tables,
		DDLs:        info.DDLs,
		Type:        metadata.OtherDDL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := o.messageAgent.SendRequest(ctx, o.jobID, dmproto.CoordinateDDL, req)
	if err != nil {
		return 0, err
	}

	coordinateResp := resp.(*dmproto.CoordinateDDLResponse)

	o.mu.Lock()
	o.pendingInfo = &info
	o.pendingOp = &optimism.Operation{
		ID:            utils.GenDDLLockID(info.Task, info.DownSchema, info.DownTable),
		Task:          info.Task,
		Source:        info.Source,
		UpSchema:      info.UpSchema,
		UpTable:       info.UpTable,
		DDLs:          coordinateResp.DDLs,
		ConflictStage: coordinateResp.ConflictStage,
		ConflictMsg:   coordinateResp.ErrorMsg,
		Done:          false,
	}
	o.mu.Unlock()
	return 0, nil
}

func (o *OptimistEngine) AddTable(info optimism.Info) (int64, error) {
	tables := make([]string, 0, len(info.TableInfosAfter)+1)
	for _, ti := range info.TableInfosAfter {
		tables = append(tables, schemacmp.Encode(ti).String())
	}
	req := &dmproto.CoordinateDDLRequest{
		TargetTable: metadata.TargetTable{Schema: info.DownSchema, Table: info.DownTable},
		SourceTable: metadata.SourceTable{Source: info.Source, Schema: info.UpSchema, Table: info.UpTable},
		Tables:      tables,
		DDLs:        info.DDLs,
		Type:        metadata.CreateTable,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := o.messageAgent.SendRequest(ctx, o.jobID, dmproto.CoordinateDDL, req)
	return 0, err
}

func (o *OptimistEngine) RemoveTable(info optimism.Info) (int64, error) {
	req := &dmproto.CoordinateDDLRequest{
		TargetTable: metadata.TargetTable{Schema: info.DownSchema, Table: info.DownTable},
		SourceTable: metadata.SourceTable{Source: info.Source, Schema: info.UpSchema, Table: info.UpTable},
		DDLs:        info.DDLs,
		Type:        metadata.DropTable,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := o.messageAgent.SendRequest(ctx, o.jobID, dmproto.CoordinateDDL, req)
	return 0, err
}

func (o *OptimistEngine) GetOperation(ctx context.Context, info optimism.Info, rev int64) (optimism.Operation, error) {
	op := o.PendingOperation()
	if op == nil {
		return optimism.Operation{}, errors.New("no pending operation")
	}
	return *op, nil
}

func (o *OptimistEngine) DoneOperation(op optimism.Operation) error {
	o.mu.Lock()
	o.pendingInfo = nil
	o.pendingOp = nil
	o.mu.Unlock()
	return nil
}

func (o *OptimistEngine) PendingInfo() *optimism.Info {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.pendingInfo == nil {
		return nil
	}
	info := *o.pendingInfo
	return &info
}

func (o *OptimistEngine) PendingOperation() *optimism.Operation {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.pendingOp == nil {
		return nil
	}
	op := *o.pendingOp
	return &op
}

func (o *OptimistEngine) GetRedirectOperation(ctx context.Context, info optimism.Info, rev int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.logger.Info("waiting redirect operation", zap.Stringer("info", info))
	o.waitingRedirectOps[metadata.SourceTable{Source: info.Source, Schema: info.UpSchema, Table: info.UpTable}] = struct{}{}
}

func (o *OptimistEngine) PendingRedirectOperation() (*optimism.Operation, string) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for targetTableID, op := range o.pendingRedirectOps {
		return op, targetTableID
	}
	return nil, ""
}

func (o *OptimistEngine) DoneRedirectOperation(targetTableID string) {
	o.mu.Lock()
	delete(o.pendingRedirectOps, targetTableID)
	o.mu.Unlock()
}

func (o *OptimistEngine) RedirectDDL(req *dmproto.RedirectDDLRequest) error {
	switch req.ConflictStage {
	case optimism.ConflictNone, optimism.ConflictResolved:
	default:
		return errors.Errorf("invalid conflict stage %s", req.ConflictStage)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.waitingRedirectOps[req.SourceTable]; !ok {
		o.logger.Info("ignore redirect ddl request", zap.Any("source_table", req.SourceTable))
		return nil
	}
	delete(o.waitingRedirectOps, req.SourceTable)
	o.pendingRedirectOps[utils.GenTableID(&filter.Table{Schema: req.TargetTable.Schema, Name: req.TargetTable.Table})] = &optimism.Operation{
		ID:            utils.GenDDLLockID(o.task, req.TargetTable.Schema, req.TargetTable.Table),
		Task:          o.task,
		Source:        o.source,
		UpSchema:      req.SourceTable.Schema,
		UpTable:       req.SourceTable.Table,
		ConflictStage: req.ConflictStage,
	}
	return nil
}
