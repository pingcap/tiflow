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

package schema

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/log"
	dmterror "github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Tracker is used to track schema locally.
type Tracker struct {
	// The Tracker uses tidb DDL library to track table structure changes.
	// where there was basically no parallel operation at the beginning.
	// However, since the validator is introduced and heavily dependent on the Tracker, we need to make sure
	// the synchronization between the reading from the validator and the modification from the syncer (e.g.
	// when the checkpoint is being rolled back, we have to make sure the validator can still vision the original tables)
	// From this point, we add an extra layer of the synchronization for the following operations:
	// 1. GetTableInfo: the validator reads table infos.
	// 2. Init: when the syncer restarts, it may re-initialize the Tracker while the validator may read the Tracker at the same time.
	// 3. Close: Being similar as above, the validator can read the Tracker while the syncer is closing the Tracker.
	sync.RWMutex
	lowerCaseTableNames int
	se                  sessionctx.Context
	upstreamTracker     schematracker.SchemaTracker
	downstreamTracker   *downstreamTracker
	logger              log.Logger
	closed              atomic.Bool
}

// downstreamTracker tracks downstream schema.
type downstreamTracker struct {
	sync.RWMutex
	se             sessionctx.Context
	downstreamConn *dbconn.DBConn                  // downstream connection
	stmtParser     *parser.Parser                  // statement parser
	tableInfos     map[string]*DownstreamTableInfo // downstream table infos
}

// DownstreamTableInfo contains tableinfo and index cache.
type DownstreamTableInfo struct {
	TableInfo   *model.TableInfo // tableInfo which comes from parse create statement syntaxtree
	WhereHandle *sqlmodel.WhereHandle
}

type executorContext struct {
	sessionctx.Context
}

var _ sqlexec.RestrictedSQLExecutor = executorContext{}

func (se executorContext) ParseWithParams(context.Context, string, ...interface{}) (ast.StmtNode, error) {
	return nil, nil
}

func (se executorContext) ExecRestrictedStmt(context.Context, ast.StmtNode, ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
}

func (se executorContext) ExecRestrictedSQL(context.Context, []sqlexec.OptionFuncAlias, string, ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
}

// NewTracker simply returns an empty Tracker,
// which should be followed by an initialization before used.
func NewTracker() *Tracker {
	return &Tracker{}
}

// Init initializes the Tracker. `sessionCfg` will be set as tracker's session variables if specified, or retrieve
// some variable from downstream using `downstreamConn`.
// NOTE **sessionCfg is a reference to caller**.
func (tr *Tracker) Init(
	ctx context.Context,
	task string,
	lowerCaseTableNames int,
	downstreamConn *dbconn.DBConn,
	logger log.Logger,
) error {
	if tr == nil {
		return nil
	}
	var err error

	rollbackHolder := fr.NewRollbackHolder("schema-tracker")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	logger = logger.WithFields(zap.String("component", "schema-tracker"), zap.String("task", task))

	upTracker := schematracker.NewSchemaTracker(lowerCaseTableNames)
	dsSession := mock.NewContext()
	dsSession.GetSessionVars().StrictSQLMode = false
	downTracker := &downstreamTracker{
		downstreamConn: downstreamConn,
		se:             dsSession,
		tableInfos:     make(map[string]*DownstreamTableInfo),
	}
	// TODO: need to use upstream timezone to correctly check literal is in [1970, 2038]
	se := executorContext{Context: mock.NewContext()}
	tr.Lock()
	defer tr.Unlock()
	tr.lowerCaseTableNames = lowerCaseTableNames
	tr.se = se
	tr.upstreamTracker = upTracker
	tr.downstreamTracker = downTracker
	tr.logger = logger
	tr.closed.Store(false)
	return nil
}

// NewTestTracker creates an empty Tracker and initializes it subsequently.
// It is useful for test.
func NewTestTracker(
	ctx context.Context,
	task string,
	downstreamConn *dbconn.DBConn,
	logger log.Logger,
) (*Tracker, error) {
	tr := NewTracker()
	err := tr.Init(ctx, task, int(conn.LCTableNamesSensitive), downstreamConn, logger)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// Exec runs an SQL (DDL) statement.
func (tr *Tracker) Exec(ctx context.Context, db string, stmt ast.StmtNode) (errRet error) {
	defer func() {
		if r := recover(); r != nil {
			errRet = fmt.Errorf("tracker panicked: %v", r)
		}
	}()
	visitor := currentDBSetter{
		currentDB: db,
	}
	stmt.Accept(&visitor)

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		return tr.upstreamTracker.CreateSchema(tr.se, v)
	case *ast.AlterDatabaseStmt:
		return tr.upstreamTracker.AlterSchema(tr.se, v)
	case *ast.DropDatabaseStmt:
		return tr.upstreamTracker.DropSchema(tr.se, v)
	case *ast.CreateTableStmt:
		return tr.upstreamTracker.CreateTable(tr.se, v)
	case *ast.AlterTableStmt:
		return tr.upstreamTracker.AlterTable(ctx, tr.se, v)
	case *ast.RenameTableStmt:
		return tr.upstreamTracker.RenameTable(tr.se, v)
	case *ast.DropTableStmt:
		return tr.upstreamTracker.DropTable(tr.se, v)
	case *ast.CreateIndexStmt:
		return tr.upstreamTracker.CreateIndex(tr.se, v)
	case *ast.DropIndexStmt:
		return tr.upstreamTracker.DropIndex(tr.se, v)
	case *ast.TruncateTableStmt:
		ident := ast.Ident{Schema: v.Table.Schema, Name: v.Table.Name}
		return tr.upstreamTracker.TruncateTable(tr.se, ident)
	default:
		tr.logger.DPanic("unexpected statement type", zap.String("type", fmt.Sprintf("%T", v)))
	}
	return nil
}

// GetTableInfo returns the schema associated with the table.
func (tr *Tracker) GetTableInfo(table *filter.Table) (*model.TableInfo, error) {
	tr.RLock()
	defer tr.RUnlock()
	if tr.closed.Load() {
		return nil, dmterror.ErrSchemaTrackerIsClosed.New("fail to get table info")
	}
	return tr.upstreamTracker.TableByName(model.NewCIStr(table.Schema), model.NewCIStr(table.Name))
}

// GetCreateTable returns the `CREATE TABLE` statement of the table.
func (tr *Tracker) GetCreateTable(ctx context.Context, table *filter.Table) (string, error) {
	tableInfo, err := tr.upstreamTracker.TableByName(model.NewCIStr(table.Schema), model.NewCIStr(table.Name))
	if err != nil {
		return "", err
	}
	result := bytes.NewBuffer(make([]byte, 0, 512))
	err = executor.ConstructResultOfShowCreateTable(tr.se, tableInfo, autoid.Allocators{}, result)
	if err != nil {
		return "", err
	}
	return conn.CreateTableSQLToOneRow(result.String()), nil
}

// AllSchemas returns all schemas visible to the tracker (excluding system tables).
func (tr *Tracker) AllSchemas() []string {
	return tr.upstreamTracker.AllSchemaNames()
}

// ListSchemaTables lists all tables in the schema.
func (tr *Tracker) ListSchemaTables(schema string) ([]string, error) {
	ret, err := tr.upstreamTracker.AllTableNamesOfSchema(model.NewCIStr(schema))
	if err != nil {
		return nil, dmterror.ErrSchemaTrackerUnSchemaNotExist.Generate(schema)
	}
	return ret, nil
}

// GetSingleColumnIndices returns indices of input column if input column only has single-column indices
// returns nil if input column has no indices, or has multi-column indices.
// TODO: move out of this package!
func (tr *Tracker) GetSingleColumnIndices(db, tbl, col string) ([]*model.IndexInfo, error) {
	col = strings.ToLower(col)
	t, err := tr.upstreamTracker.TableByName(model.NewCIStr(db), model.NewCIStr(tbl))
	if err != nil {
		return nil, err
	}

	var idxInfos []*model.IndexInfo
	for _, idx := range t.Indices {
		for _, col2 := range idx.Columns {
			// found an index covers input column
			if col2.Name.L == col {
				if len(idx.Columns) == 1 {
					idxInfos = append(idxInfos, idx)
				} else {
					// temporary use errors.New, won't propagate further
					return nil, errors.New("found multi-column index")
				}
			}
		}
	}
	return idxInfos, nil
}

// IsTableNotExists checks if err means the database or table does not exist.
func IsTableNotExists(err error) bool {
	return infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)
}

// Reset drops all tables inserted into this tracker.
func (tr *Tracker) Reset() {
	// TODO: lock?
	tr.upstreamTracker = schematracker.NewSchemaTracker(tr.lowerCaseTableNames)
}

// Close closes a tracker.
func (tr *Tracker) Close() {
	if tr == nil {
		return
	}
	// prevent SchemaTracker being closed when
	// other components are getting/setting table info
	tr.Lock()
	defer tr.Unlock()
	tr.closed.Store(true)
}

// DropTable drops a table from this tracker.
func (tr *Tracker) DropTable(table *filter.Table) error {
	return tr.upstreamTracker.DeleteTable(model.NewCIStr(table.Schema), model.NewCIStr(table.Name))
}

// CreateSchemaIfNotExists creates a SCHEMA of the given name if it did not exist.
func (tr *Tracker) CreateSchemaIfNotExists(db string) error {
	dbName := model.NewCIStr(db)
	if tr.upstreamTracker.SchemaByName(dbName) != nil {
		return nil
	}
	stmt := &ast.CreateDatabaseStmt{
		Name:        dbName,
		IfNotExists: true,
	}
	return tr.upstreamTracker.CreateSchema(tr.se, stmt)
}

// cloneTableInfo creates a clone of the TableInfo.
func cloneTableInfo(ti *model.TableInfo) *model.TableInfo {
	ret := ti.Clone()
	ret.Lock = nil
	// FIXME pingcap/parser's Clone() doesn't clone Partition yet
	if ret.Partition != nil {
		pi := *ret.Partition
		pi.Definitions = append([]model.PartitionDefinition(nil), ret.Partition.Definitions...)
		ret.Partition = &pi
	}
	return ret
}

// CreateTableIfNotExists creates a TABLE of the given name if it did not exist.
func (tr *Tracker) CreateTableIfNotExists(table *filter.Table, ti *model.TableInfo) error {
	schemaName := model.NewCIStr(table.Schema)
	tableName := model.NewCIStr(table.Name)
	ti = cloneTableInfo(ti)
	ti.Name = tableName
	return tr.upstreamTracker.CreateTableWithInfo(tr.se, schemaName, ti, ddl.OnExistIgnore)
}

// SplitBatchCreateTableAndHandle will split the batch if it exceeds the kv entry size limit.
func (tr *Tracker) SplitBatchCreateTableAndHandle(schema model.CIStr, info []*model.TableInfo, l int, r int) error {
	var err error
	if err = tr.upstreamTracker.BatchCreateTableWithInfo(tr.se, schema, info[l:r], ddl.OnExistIgnore); kv.ErrEntryTooLarge.Equal(err) {
		if r-l == 1 {
			return err
		}
		err = tr.SplitBatchCreateTableAndHandle(schema, info, l, (l+r)/2)
		if err != nil {
			return err
		}
		err = tr.SplitBatchCreateTableAndHandle(schema, info, (l+r)/2, r)
		if err != nil {
			return err
		}
		return nil
	}
	return err
}

// BatchCreateTableIfNotExist will batch creating tables per schema. If the schema does not exist, it will create it.
// The argument is { database name -> { table name -> TableInfo } }.
func (tr *Tracker) BatchCreateTableIfNotExist(tablesToCreate map[string]map[string]*model.TableInfo) error {
	for schema, tableNameInfo := range tablesToCreate {
		if err := tr.CreateSchemaIfNotExists(schema); err != nil {
			return err
		}

		var cloneTis []*model.TableInfo
		for table, ti := range tableNameInfo {
			cloneTi := cloneTableInfo(ti)        // clone TableInfo w.r.t the warning of the CreateTable function
			cloneTi.Name = model.NewCIStr(table) // TableInfo has no `TableName`
			cloneTis = append(cloneTis, cloneTi)
		}
		schemaName := model.NewCIStr(schema)
		if err := tr.SplitBatchCreateTableAndHandle(schemaName, cloneTis, 0, len(cloneTis)); err != nil {
			return err
		}
	}
	return nil
}

// GetDownStreamTableInfo gets downstream table info.
// note. this function will init downstreamTrack's table info.
func (tr *Tracker) GetDownStreamTableInfo(tctx *tcontext.Context, tableID string, originTI *model.TableInfo) (*DownstreamTableInfo, error) {
	return tr.downstreamTracker.getOrInit(tctx, tableID, originTI)
}

// RemoveDownstreamSchema just remove schema or table in downstreamTrack.
func (tr *Tracker) RemoveDownstreamSchema(tctx *tcontext.Context, targetTables []*filter.Table) {
	if len(targetTables) == 0 {
		return
	}

	for _, targetTable := range targetTables {
		tr.downstreamTracker.remove(tctx, targetTable)
	}
}

func (dt *downstreamTracker) getOrInit(tctx *tcontext.Context, tableID string, originTI *model.TableInfo) (*DownstreamTableInfo, error) {
	dt.RLock()
	dti, ok := dt.tableInfos[tableID]
	dt.RUnlock()
	if ok {
		return dti, nil
	}

	// cache miss, get from downstream
	dt.Lock()
	defer dt.Unlock()
	dti, ok = dt.tableInfos[tableID]
	if !ok {
		tctx.Logger.Info("Downstream schema tracker init. ", zap.String("tableID", tableID))
		downstreamTI, err := dt.getTableInfoByCreateStmt(tctx, tableID)
		if err != nil {
			tctx.Logger.Error("Init dowstream schema info error. ", zap.String("tableID", tableID), zap.Error(err))
			return nil, err
		}

		dti = &DownstreamTableInfo{
			TableInfo:   downstreamTI,
			WhereHandle: sqlmodel.GetWhereHandle(originTI, downstreamTI),
		}
		dt.tableInfos[tableID] = dti
	}
	return dti, nil
}

func (dt *downstreamTracker) remove(tctx *tcontext.Context, targetTable *filter.Table) {
	dt.Lock()
	defer dt.Unlock()

	tableID := utils.GenTableID(targetTable)
	if _, ok := dt.tableInfos[tableID]; !ok {
		// handle just have schema
		if targetTable.Schema != "" && targetTable.Name == "" {
			for k := range dt.tableInfos {
				if strings.HasPrefix(k, tableID+".") {
					delete(dt.tableInfos, k)
					tctx.Logger.Info("Remove downstream schema tracker", zap.String("tableID", k))
				}
			}
		}
	} else {
		delete(dt.tableInfos, tableID)
		tctx.Logger.Info("Remove downstream schema tracker", zap.String("tableID", tableID))
	}
}

// getTableInfoByCreateStmt get downstream tableInfo by "SHOW CREATE TABLE" stmt.
func (dt *downstreamTracker) getTableInfoByCreateStmt(tctx *tcontext.Context, tableID string) (*model.TableInfo, error) {
	if dt.stmtParser == nil {
		err := dt.initDownStreamSQLModeAndParser(tctx)
		if err != nil {
			return nil, err
		}
	}
	createStr, err := dbconn.GetTableCreateSQL(tctx, dt.downstreamConn, tableID)
	if err != nil {
		return nil, dmterror.ErrSchemaTrackerCannotFetchDownstreamCreateTableStmt.Delegate(err, tableID)
	}

	tctx.Logger.Info("Show create table info", zap.String("tableID", tableID), zap.String("create string", createStr))
	// parse create table stmt.
	stmtNode, err := dt.stmtParser.ParseOneStmt(createStr, "", "")
	if err != nil {
		return nil, dmterror.ErrSchemaTrackerInvalidCreateTableStmt.Delegate(err, createStr)
	}

	// suppress ErrTooLongKey
	strictSQLModeBackup := dt.se.GetSessionVars().StrictSQLMode
	// support drop PK
	enableClusteredIndexBackup := dt.se.GetSessionVars().EnableClusteredIndex
	dt.se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	defer func() {
		dt.se.GetSessionVars().StrictSQLMode = strictSQLModeBackup
		dt.se.GetSessionVars().EnableClusteredIndex = enableClusteredIndexBackup
	}()

	ti, err := ddl.BuildTableInfoWithStmt(dt.se, stmtNode.(*ast.CreateTableStmt), mysql.DefaultCharset, "", nil)
	if err != nil {
		return nil, dmterror.ErrSchemaTrackerCannotMockDownstreamTable.Delegate(err, createStr)
	}
	ti.State = model.StatePublic
	return ti, nil
}

// initDownStreamTrackerParser init downstream tracker parser by default sql_mode.
func (dt *downstreamTracker) initDownStreamSQLModeAndParser(tctx *tcontext.Context) error {
	setSQLMode := fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)
	_, err := dt.downstreamConn.ExecuteSQL(tctx, nil, []string{setSQLMode})
	if err != nil {
		return dmterror.ErrSchemaTrackerCannotSetDownstreamSQLMode.Delegate(err, mysql.DefaultSQLMode)
	}
	stmtParser, err := conn.GetParserFromSQLModeStr(mysql.DefaultSQLMode)
	if err != nil {
		return dmterror.ErrSchemaTrackerCannotInitDownstreamParser.Delegate(err, mysql.DefaultSQLMode)
	}
	dt.stmtParser = stmtParser
	return nil
}
