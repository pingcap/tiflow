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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	tidbConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/schematracker"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	unistoreConfig "github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/log"
	dmterror "github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

const (
	// TiDBClusteredIndex is the variable name for clustered index.
	TiDBClusteredIndex = "tidb_enable_clustered_index"
)

var (
	// don't read clustered index variable from downstream because it may changed during syncing
	// we always using OFF tidb_enable_clustered_index unless user set it in config.
	downstreamVars    = []string{"sql_mode", "tidb_skip_utf8_check"}
	defaultGlobalVars = map[string]string{
		TiDBClusteredIndex: "OFF",
	}
)

// TODO: use it in next PR.
var _ = schematracker.NewSchemaTracker(0)

func init() {
	unistoreConfig.DefaultConf.Engine.VlogFileSize = int64(kv.TxnEntrySizeLimit)
	unistoreConfig.DefaultConf.Engine.L1Size = 128 * units.MiB
}

// Tracker is used to track schema locally.
type Tracker struct {
	// The Tracker is an embedded tidb in essence, where there was basically no parallel operation at the beginning.
	// However, since the validator is introduced and heavily dependent on the Tracker, we need to make sure
	// the synchronization between the reading from the validator and the modification from the syncer (e.g.
	// when the checkpoint is being rolled back, we have to make sure the validator can still vision the original tables)
	// From this point, we add an extra layer of the synchronization for the following operations:
	// 1. GetTableInfo: the validator reads table infos.
	// 2. Init: when the syncer restarts, it may re-initialize the Tracker while the validator may read the Tracker at the same time.
	// 3. Close: Being similar as above, the validator can read the Tracker while the syncer is closing the Tracker.
	sync.RWMutex
	storePath string
	store     kv.Storage
	dom       *domain.Domain
	se        session.Session
	dsTracker *downstreamTracker
	closed    atomic.Bool
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
	sessionCfg map[string]string,
	downstreamConn *dbconn.DBConn,
	logger log.Logger,
) error {
	if tr == nil {
		return nil
	}
	var (
		err       error
		storePath string
		store     kv.Storage
		dom       *domain.Domain
		se        session.Session
	)

	rollbackHolder := fr.NewRollbackHolder("schema-tracker")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	// NOTE: tidb uses a **global** config so can't isolate tracker's config from each other. If that isolation is needed,
	// we might SetGlobalConfig before every call to tracker, or use some patch like https://github.com/bouk/monkey
	tidbConfig.UpdateGlobal(func(conf *tidbConfig.Config) {
		// bypass wait time of https://github.com/pingcap/tidb/pull/20550
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		// explicitly disable new-collation for better compatibility as tidb only support a subset of all mysql collations.
		conf.NewCollationsEnabledOnFirstBootstrap = false
		conf.Performance.RunAutoAnalyze = false
	})

	if len(sessionCfg) == 0 {
		sessionCfg = make(map[string]string)
	}

	logger = logger.WithFields(zap.String("component", "schema-tracker"), zap.String("task", task))
	tctx := tcontext.NewContext(ctx, logger)
	// get variables if user doesn't specify
	// all cfg in downstreamVars should be lowercase
	for _, k := range downstreamVars {
		if _, ok := sessionCfg[k]; !ok {
			var ignoredColumn interface{}
			rows, err2 := downstreamConn.QuerySQL(tctx, nil, fmt.Sprintf("SHOW VARIABLES LIKE '%s'", k))
			if err2 != nil {
				return err2
			}
			if rows.Next() {
				var value string
				if err3 := rows.Scan(&ignoredColumn, &value); err3 != nil {
					return err3
				}
				sessionCfg[k] = value
			}
			// nolint:sqlclosecheck
			if err2 = rows.Close(); err2 != nil {
				return err2
			}
			if err2 = rows.Err(); err2 != nil {
				return err2
			}
		}
	}

	storePath, err = newTmpFolderForTracker(task)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "DeleteStorePath", Fn: func() {
		_ = os.RemoveAll(storePath)
	}})

	store, err = mockstore.NewMockStore(
		mockstore.WithStoreType(mockstore.EmbedUnistore),
		mockstore.WithPath(storePath))
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "CloseStore", Fn: func() {
		_ = store.Close()
	}})

	// avoid data race and of course no use in DM
	session.DisableStats4Test()

	dom, err = session.BootstrapSession(store)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "CloseDomain", Fn: dom.Close})

	se, err = session.CreateSession(store)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "CloseSession", Fn: se.Close})

	globalVarsToSet := make(map[string]string, len(defaultGlobalVars))
	for k, v := range defaultGlobalVars {
		// user's config has highest priority
		if _, ok := sessionCfg[k]; !ok {
			globalVarsToSet[k] = v
		}
	}

	for k, v := range sessionCfg {
		err = se.GetSessionVars().SetSystemVarWithRelaxedValidation(k, v)
		if err != nil {
			// when user set some unsupported variable, we just ignore it
			if terror.ErrorEqual(err, variable.ErrUnknownSystemVar) {
				log.L().Warn("can not set this variable", zap.Error(err))
				continue
			}
			return err
		}
	}
	for k, v := range globalVarsToSet {
		err = se.GetSessionVars().SetSystemVarWithRelaxedValidation(k, v)
		if err != nil {
			return err
		}
	}
	// skip DDL test https://github.com/pingcap/tidb/pull/33079
	se.SetValue(sessionctx.QueryString, "skip")

	// TiDB will unconditionally create an empty "test" schema.
	// This interferes with MySQL/MariaDB upstream which such schema does not
	// exist by default. So we need to drop it first.
	err = dropDatabase(dom, se, "test")
	if err != nil {
		return err
	}

	dsSession := mock.NewContext()
	dsSession.GetSessionVars().StrictSQLMode = false
	// init downstreamTracker
	dsTracker := &downstreamTracker{
		downstreamConn: downstreamConn,
		se:             dsSession,
		tableInfos:     make(map[string]*DownstreamTableInfo),
	}
	tr.Lock()
	defer tr.Unlock()
	tr.storePath = storePath
	tr.store = store
	tr.dom = dom
	tr.se = se
	tr.dsTracker = dsTracker
	tr.closed.Store(false)
	return nil
}

// NewTestTracker creates an empty Tracker and initializes it subsequently.
// It is useful for test.
func NewTestTracker(
	ctx context.Context,
	task string,
	sessionCfg map[string]string,
	downstreamConn *dbconn.DBConn,
	logger log.Logger,
) (*Tracker, error) {
	tr := NewTracker()
	err := tr.Init(ctx, task, sessionCfg, downstreamConn, logger)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func newTmpFolderForTracker(task string) (string, error) {
	return os.MkdirTemp("./", url.PathEscape(task)+"-tracker")
}

// Exec runs an SQL (DDL) statement.
func (tr *Tracker) Exec(ctx context.Context, db string, sql string) error {
	tr.se.GetSessionVars().CurrentDB = db
	_, err := tr.se.Execute(ctx, sql)
	return err
}

// GetTableInfo returns the schema associated with the table.
func (tr *Tracker) GetTableInfo(table *filter.Table) (*model.TableInfo, error) {
	tr.RLock()
	defer tr.RUnlock()
	if tr.closed.Load() {
		return nil, dmterror.ErrSchemaTrackerIsClosed.New("fail to get table info")
	}
	dbName := model.NewCIStr(table.Schema)
	tableName := model.NewCIStr(table.Name)
	t, err := tr.dom.InfoSchema().TableByName(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return t.Meta(), nil
}

// GetCreateTable returns the `CREATE TABLE` statement of the table.
func (tr *Tracker) GetCreateTable(ctx context.Context, table *filter.Table) (string, error) {
	// use `SHOW CREATE TABLE` now, another method maybe `executor.ConstructResultOfShowCreateTable`.
	rs, err := tr.se.Execute(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table.String()))
	if err != nil {
		return "", err
	} else if len(rs) != 1 {
		return "", nil // this should not happen.
	}
	// nolint:errcheck
	defer rs[0].Close()

	req := rs[0].NewChunk(nil)
	err = rs[0].Next(ctx, req)
	if err != nil {
		return "", err
	}
	if req.NumRows() == 0 {
		return "", nil // this should not happen.
	}

	row := req.GetRow(0)
	str := row.GetString(1) // the first column is the table name.
	return utils.CreateTableSQLToOneRow(str), nil
}

// AllSchemas returns all schemas visible to the tracker (excluding system tables).
func (tr *Tracker) AllSchemas() []*model.DBInfo {
	allSchemas := tr.dom.InfoSchema().AllSchemas()
	filteredSchemas := make([]*model.DBInfo, 0, len(allSchemas)-3)
	for _, db := range allSchemas {
		if !filter.IsSystemSchema(db.Name.L) {
			filteredSchemas = append(filteredSchemas, db)
		}
	}
	return filteredSchemas
}

// ListSchemaTables lists all tables in the schema.
func (tr *Tracker) ListSchemaTables(schema string) ([]string, error) {
	allSchemas := tr.AllSchemas()
	for _, db := range allSchemas {
		if db.Name.String() == schema {
			tables := make([]string, len(db.Tables))
			for i, t := range db.Tables {
				tables[i] = t.Name.String()
			}
			return tables, nil
		}
	}
	return nil, dmterror.ErrSchemaTrackerUnSchemaNotExist.Generate(schema)
}

// GetSingleColumnIndices returns indices of input column if input column only has single-column indices
// returns nil if input column has no indices, or has multi-column indices.
func (tr *Tracker) GetSingleColumnIndices(db, tbl, col string) ([]*model.IndexInfo, error) {
	col = strings.ToLower(col)
	t, err := tr.dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(tbl))
	if err != nil {
		return nil, err
	}

	var idxInfos []*model.IndexInfo
	for _, idx := range t.Indices() {
		m := idx.Meta()
		for _, col2 := range m.Columns {
			// found an index covers input column
			if col2.Name.L == col {
				if len(m.Columns) == 1 {
					idxInfos = append(idxInfos, m)
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
func (tr *Tracker) Reset() error {
	tr.se.SetValue(sessionctx.QueryString, "skip")
	allDBs := tr.dom.InfoSchema().AllSchemaNames()
	for _, db := range allDBs {
		dbName := model.NewCIStr(db)
		if filter.IsSystemSchema(dbName.L) {
			continue
		}
		if err := dropDatabase(tr.dom, tr.se, dbName.L); err != nil {
			return err
		}
	}
	return nil
}

func dropDatabase(dom *domain.Domain, se session.Session, db string) error {
	stmt := &ast.DropDatabaseStmt{
		Name:     model.NewCIStr(db),
		IfExists: true,
	}
	return dom.DDL().DropSchema(se, stmt)
}

// Close close a tracker.
func (tr *Tracker) Close() error {
	if tr == nil {
		return nil
	}
	// prevent SchemaTracker being closed when
	// other components are getting/setting table info
	tr.Lock()
	defer tr.Unlock()
	if !tr.closed.CAS(false, true) {
		return nil
	}
	// Build of the Tracker and the initialization is divided.
	// these fields can possibly be nil if the Tracker is closed before the initialization.
	if tr.se != nil {
		tr.se.Close()
	}
	if tr.dom != nil {
		tr.dom.Close()
	}
	if tr.store != nil {
		if err := tr.store.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(tr.storePath)
}

// DropTable drops a table from this tracker.
func (tr *Tracker) DropTable(table *filter.Table) error {
	tr.se.SetValue(sessionctx.QueryString, "skip")
	stmt := &ast.DropTableStmt{
		Tables: []*ast.TableName{
			{
				Schema: model.NewCIStr(table.Schema),
				Name:   model.NewCIStr(table.Name),
			},
		},
		IfExists: true,
	}
	return tr.dom.DDL().DropTable(tr.se, stmt)
}

// DropIndex drops an index from this tracker.
func (tr *Tracker) DropIndex(table *filter.Table, index string) error {
	tr.se.SetValue(sessionctx.QueryString, "skip")
	stmt := &ast.DropIndexStmt{
		Table: &ast.TableName{
			Schema: model.NewCIStr(table.Schema),
			Name:   model.NewCIStr(table.Name),
		},
		IndexName: index,
		IfExists:  true,
	}
	return tr.dom.DDL().DropIndex(tr.se, stmt)
}

// CreateSchemaIfNotExists creates a SCHEMA of the given name if it did not exist.
func (tr *Tracker) CreateSchemaIfNotExists(db string) error {
	tr.se.SetValue(sessionctx.QueryString, "skip")
	dbName := model.NewCIStr(db)
	if tr.dom.InfoSchema().SchemaExists(dbName) {
		return nil
	}
	stmt := &ast.CreateDatabaseStmt{
		Name:        dbName,
		IfNotExists: true,
	}
	return tr.dom.DDL().CreateSchema(tr.se, stmt)
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
	tr.se.SetValue(sessionctx.QueryString, "skip")
	schemaName := model.NewCIStr(table.Schema)
	tableName := model.NewCIStr(table.Name)
	ti = cloneTableInfo(ti)
	ti.Name = tableName
	return tr.dom.DDL().CreateTableWithInfo(tr.se, schemaName, ti, ddl.OnExistIgnore)
}

// SplitBatchCreateTableAndHandle will split the batch if it exceeds the kv entry size limit.
func (tr *Tracker) SplitBatchCreateTableAndHandle(schema model.CIStr, info []*model.TableInfo, l int, r int) error {
	var err error
	if err = tr.dom.DDL().BatchCreateTableWithInfo(tr.se, schema, info[l:r], ddl.OnExistIgnore); kv.ErrEntryTooLarge.Equal(err) {
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
	tr.se.SetValue(sessionctx.QueryString, "skip")
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

// GetSystemVar gets a variable from schema tracker.
func (tr *Tracker) GetSystemVar(name string) (string, bool) {
	return tr.se.GetSessionVars().GetSystemVar(name)
}

// GetDownStreamTableInfo gets downstream table info.
// note. this function will init downstreamTrack's table info.
func (tr *Tracker) GetDownStreamTableInfo(tctx *tcontext.Context, tableID string, originTI *model.TableInfo) (*DownstreamTableInfo, error) {
	return tr.dsTracker.getOrInit(tctx, tableID, originTI)
}

// RemoveDownstreamSchema just remove schema or table in downstreamTrack.
func (tr *Tracker) RemoveDownstreamSchema(tctx *tcontext.Context, targetTables []*filter.Table) {
	if len(targetTables) == 0 {
		return
	}

	for _, targetTable := range targetTables {
		tr.dsTracker.remove(tctx, targetTable)
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
	stmtParser, err := utils.GetParserFromSQLModeStr(mysql.DefaultSQLMode)
	if err != nil {
		return dmterror.ErrSchemaTrackerCannotInitDownstreamParser.Delegate(err, mysql.DefaultSQLMode)
	}
	dt.stmtParser = stmtParser
	return nil
}
