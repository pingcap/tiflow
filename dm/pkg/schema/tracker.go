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
	"strings"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	"github.com/pingcap/ticdc/dm/pkg/log"
	dterror "github.com/pingcap/ticdc/dm/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/utils"
	"github.com/pingcap/ticdc/dm/syncer/dbconn"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tidbConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

const (
	// TiDBClusteredIndex is the variable name for clustered index.
	TiDBClusteredIndex = "tidb_enable_clustered_index"
	// downstream mock table id, consists of serial numbers of letters.
	mockTableID = 121402101900011104
)

var (
	// don't read clustered index variable from downstream because it may changed during syncing
	// we always using OFF tidb_enable_clustered_index unless user set it in config.
	downstreamVars    = []string{"sql_mode", "tidb_skip_utf8_check"}
	defaultGlobalVars = map[string]string{
		TiDBClusteredIndex: "OFF",
	}
)

// Tracker is used to track schema locally.
type Tracker struct {
	store     kv.Storage
	dom       *domain.Domain
	se        session.Session
	dsTracker *downstreamTracker // downstream tracker tableid -> createTableStmt
}

// downstreamTracker tracks downstream schema.
type downstreamTracker struct {
	downstreamConn *dbconn.DBConn                  // downstream connection
	stmtParser     *parser.Parser                  // statement parser
	tableInfos     map[string]*downstreamTableInfo // downstream table infos
}

// downstreamTableInfo contains tableinfo and index cache.
type downstreamTableInfo struct {
	tableInfo        *model.TableInfo   // tableInfo which comes from parse create statement syntaxtree
	indexCache       *model.IndexInfo   // index cache include pk/uk(not null)
	availableUKCache []*model.IndexInfo // index cache include uks(data not null)
}

// NewTracker creates a new tracker. `sessionCfg` will be set as tracker's session variables if specified, or retrieve
// some variable from downstream using `downstreamConn`.
// NOTE **sessionCfg is a reference to caller**.
func NewTracker(ctx context.Context, task string, sessionCfg map[string]string, downstreamConn *dbconn.DBConn) (*Tracker, error) {
	// NOTE: tidb uses a **global** config so can't isolate tracker's config from each other. If that isolation is needed,
	// we might SetGlobalConfig before every call to tracker, or use some patch like https://github.com/bouk/monkey
	tidbConfig.UpdateGlobal(func(conf *tidbConfig.Config) {
		// bypass wait time of https://github.com/pingcap/tidb/pull/20550
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	if len(sessionCfg) == 0 {
		sessionCfg = make(map[string]string)
	}

	tctx := tcontext.NewContext(ctx, log.With(zap.String("component", "schema-tracker"), zap.String("task", task)))
	// get variables if user doesn't specify
	// all cfg in downstreamVars should be lower case
	for _, k := range downstreamVars {
		if _, ok := sessionCfg[k]; !ok {
			var ignoredColumn interface{}
			rows, err2 := downstreamConn.QuerySQL(tctx, fmt.Sprintf("SHOW VARIABLES LIKE '%s'", k))
			if err2 != nil {
				return nil, err2
			}
			if rows.Next() {
				var value string
				if err3 := rows.Scan(&ignoredColumn, &value); err3 != nil {
					return nil, err3
				}
				sessionCfg[k] = value
			}
			// nolint:sqlclosecheck
			if err2 = rows.Close(); err2 != nil {
				return nil, err2
			}
			if err2 = rows.Err(); err2 != nil {
				return nil, err2
			}
		}
	}

	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	if err != nil {
		return nil, err
	}

	// avoid data race and of course no use in DM
	domain.RunAutoAnalyze = false
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, err
	}

	se, err := session.CreateSession(store)
	if err != nil {
		return nil, err
	}

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
			return nil, err
		}
	}
	for k, v := range globalVarsToSet {
		err = se.GetSessionVars().SetSystemVarWithRelaxedValidation(k, v)
		if err != nil {
			return nil, err
		}
	}

	// TiDB will unconditionally create an empty "test" schema.
	// This interferes with MySQL/MariaDB upstream which such schema does not
	// exist by default. So we need to drop it first.
	err = dom.DDL().DropSchema(se, model.NewCIStr("test"))
	if err != nil {
		return nil, err
	}

	// init downstreamTracker
	dsTracker := &downstreamTracker{
		downstreamConn: downstreamConn,
		tableInfos:     make(map[string]*downstreamTableInfo),
	}

	return &Tracker{
		store:     store,
		dom:       dom,
		se:        se,
		dsTracker: dsTracker,
	}, nil
}

// Exec runs an SQL (DDL) statement.
func (tr *Tracker) Exec(ctx context.Context, db string, sql string) error {
	tr.se.GetSessionVars().CurrentDB = db
	_, err := tr.se.Execute(ctx, sql)
	return err
}

// GetTableInfo returns the schema associated with the table.
func (tr *Tracker) GetTableInfo(table *filter.Table) (*model.TableInfo, error) {
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

	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	if err != nil {
		return "", err
	}
	if req.NumRows() == 0 {
		return "", nil // this should not happen.
	}

	row := req.GetRow(0)
	str := row.GetString(1) // the first column is the table name.
	// returned as single line.
	str = strings.ReplaceAll(str, "\n", "")
	str = strings.ReplaceAll(str, "  ", " ")
	return str, nil
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
	allDBs := tr.dom.InfoSchema().AllSchemaNames()
	ddl := tr.dom.DDL()
	for _, db := range allDBs {
		dbName := model.NewCIStr(db)
		if filter.IsSystemSchema(dbName.L) {
			continue
		}
		if err := ddl.DropSchema(tr.se, dbName); err != nil {
			return err
		}
	}
	return nil
}

// Close close a tracker.
func (tr *Tracker) Close() error {
	tr.se.Close()
	tr.dom.Close()
	return tr.store.Close()
}

// DropTable drops a table from this tracker.
func (tr *Tracker) DropTable(table *filter.Table) error {
	tableIdent := ast.Ident{
		Schema: model.NewCIStr(table.Schema),
		Name:   model.NewCIStr(table.Name),
	}
	return tr.dom.DDL().DropTable(tr.se, tableIdent)
}

// DropIndex drops an index from this tracker.
func (tr *Tracker) DropIndex(table *filter.Table, index string) error {
	tableIdent := ast.Ident{
		Schema: model.NewCIStr(table.Schema),
		Name:   model.NewCIStr(table.Name),
	}
	return tr.dom.DDL().DropIndex(tr.se, tableIdent, model.NewCIStr(index), true)
}

// CreateSchemaIfNotExists creates a SCHEMA of the given name if it did not exist.
func (tr *Tracker) CreateSchemaIfNotExists(db string) error {
	dbName := model.NewCIStr(db)
	if tr.dom.InfoSchema().SchemaExists(dbName) {
		return nil
	}
	return tr.dom.DDL().CreateSchema(tr.se, dbName, nil, nil, nil)
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
	return tr.dom.DDL().CreateTableWithInfo(tr.se, schemaName, ti, ddl.OnExistIgnore, false)
}

// GetSystemVar gets a variable from schema tracker.
func (tr *Tracker) GetSystemVar(name string) (string, bool) {
	return tr.se.GetSessionVars().GetSystemVar(name)
}

// GetDownStreamIndexInfo gets downstream PK/UK(not null) Index.
// note. this function will init downstreamTrack's table info.
func (tr *Tracker) GetDownStreamIndexInfo(tctx *tcontext.Context, tableID string, originTi *model.TableInfo) (*model.IndexInfo, error) {
	dti, ok := tr.dsTracker.tableInfos[tableID]
	if !ok {
		log.L().Info("Downstream schema tracker init. ", zap.String("tableID", tableID))
		ti, err := tr.getTableInfoByCreateStmt(tctx, tableID)
		if err != nil {
			log.L().Error("Init dowstream schema info error. ", zap.String("tableID", tableID), zap.Error(err))
			return nil, err
		}

		dti = getDownStreamTi(ti, originTi)
		tr.dsTracker.tableInfos[tableID] = dti
	}
	return dti.indexCache, nil
}

// GetAvailableDownStreamUKIndexInfo gets available downstream UK whose data is not null.
// note. this function will not init downstreamTrack.
func (tr *Tracker) GetAvailableDownStreamUKIndexInfo(tableID string, data []interface{}) *model.IndexInfo {
	dti, ok := tr.dsTracker.tableInfos[tableID]

	if !ok || len(dti.availableUKCache) == 0 {
		return nil
	}
	// func for check data is not null
	fn := func(i int) bool {
		return data[i] != nil
	}

	for i, uk := range dti.availableUKCache {
		// check uk's column data is not null
		if isSpecifiedIndexColumn(uk, fn) {
			if i != 0 {
				// exchange available uk to the first of the array to reduce judgements for next row
				dti.availableUKCache[0], dti.availableUKCache[i] = dti.availableUKCache[i], dti.availableUKCache[0]
			}
			return uk
		}
	}
	return nil
}

// RemoveDownstreamSchema just remove schema or table in downstreamTrack.
func (tr *Tracker) RemoveDownstreamSchema(targetTables []*filter.Table) {
	if len(targetTables) == 0 {
		return
	}

	for _, targetTable := range targetTables {
		tableID := utils.GenTableID(targetTable)
		_, ok := tr.dsTracker.tableInfos[tableID]
		if !ok {
			// handle just have schema
			if targetTable.Schema != "" && targetTable.Name == "" {
				for k := range tr.dsTracker.tableInfos {
					if strings.HasPrefix(k, tableID+".") {
						delete(tr.dsTracker.tableInfos, k)
						log.L().Info("Remove downstream schema tracker", zap.String("tableID", tableID))
					}
				}
			}
		} else {
			delete(tr.dsTracker.tableInfos, tableID)
			log.L().Info("Remove downstream schema tracker", zap.String("tableID", tableID))
		}
	}
}

// getTableInfoByCreateStmt get downstream tableInfo by "SHOW CREATE TABLE" stmt.
func (tr *Tracker) getTableInfoByCreateStmt(tctx *tcontext.Context, tableID string) (*model.TableInfo, error) {
	if tr.dsTracker.stmtParser == nil {
		err := tr.initDownStreamSQLModeAndParser(tctx)
		if err != nil {
			return nil, err
		}
	}

	querySQL := fmt.Sprintf("SHOW CREATE TABLE %s", tableID)
	rows, err := tr.dsTracker.downstreamConn.QuerySQL(tctx, querySQL)
	if err != nil {
		return nil, dterror.ErrSchemaTrackerCannotFetchDownstreamCreateTableStmt.Delegate(err, tableID)
	}
	defer rows.Close()
	var tableName, createStr string
	if rows.Next() {
		if err = rows.Scan(&tableName, &createStr); err != nil {
			return nil, dterror.DBErrorAdapt(rows.Err(), dterror.ErrDBDriverError)
		}
	}

	log.L().Info("Show create table info", zap.String("tableID", tableID), zap.String("create string", createStr))
	// parse create table stmt.
	stmtNode, err := tr.dsTracker.stmtParser.ParseOneStmt(createStr, "", "")
	if err != nil {
		return nil, dterror.ErrSchemaTrackerInvalidCreateTableStmt.Delegate(err, createStr)
	}

	ti, err := ddl.MockTableInfo(mock.NewContext(), stmtNode.(*ast.CreateTableStmt), mockTableID)
	if err != nil {
		return nil, dterror.ErrSchemaTrackerCannotMockDownstreamTable.Delegate(err, createStr)
	}
	return ti, nil
}

// initDownStreamTrackerParser init downstream tracker parser by default sql_mode.
func (tr *Tracker) initDownStreamSQLModeAndParser(tctx *tcontext.Context) error {
	setSQLMode := fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)
	_, err := tr.dsTracker.downstreamConn.ExecuteSQL(tctx, []string{setSQLMode})
	if err != nil {
		return dterror.ErrSchemaTrackerCannotSetDownstreamSQLMode.Delegate(err, mysql.DefaultSQLMode)
	}
	stmtParser, err := utils.GetParserFromSQLModeStr(mysql.DefaultSQLMode)
	if err != nil {
		return dterror.ErrSchemaTrackerCannotInitDownstreamParser.Delegate(err, mysql.DefaultSQLMode)
	}
	tr.dsTracker.stmtParser = stmtParser
	return nil
}

// getDownStreamTi constructs downstreamTable index cache by tableinfo.
func getDownStreamTi(ti *model.TableInfo, originTi *model.TableInfo) *downstreamTableInfo {
	var (
		indexCache       *model.IndexInfo
		availableUKCache = make([]*model.IndexInfo, 0, len(ti.Indices))
		hasPk            = false
	)

	// func for check not null constraint
	fn := func(i int) bool {
		return mysql.HasNotNullFlag(ti.Columns[i].Flag)
	}

	for _, idx := range ti.Indices {
		indexRedict := redirectIndexKeys(idx, originTi)
		if indexRedict == nil {
			continue
		}
		if idx.Primary {
			indexCache = indexRedict
			hasPk = true
		} else if idx.Unique {
			// second check not null unique key
			if !hasPk && isSpecifiedIndexColumn(idx, fn) {
				indexCache = indexRedict
			} else {
				availableUKCache = append(availableUKCache, indexRedict)
			}
		}
	}

	// handle pk exceptional case.
	// e.g. "create table t(a int primary key, b int)".
	if !hasPk {
		exPk := redirectIndexKeys(handlePkExCase(ti), originTi)
		if exPk != nil {
			indexCache = exPk
		}
	}

	return &downstreamTableInfo{
		tableInfo:        ti,
		indexCache:       indexCache,
		availableUKCache: availableUKCache,
	}
}

// redirectIndexKeys redirect index's columns offset in origin tableinfo.
func redirectIndexKeys(index *model.IndexInfo, originTi *model.TableInfo) *model.IndexInfo {
	if index == nil || originTi == nil {
		return nil
	}

	columns := make([]*model.IndexColumn, 0, len(index.Columns))
	for _, key := range index.Columns {
		if originColumn := model.FindColumnInfo(originTi.Columns, key.Name.O); originColumn != nil {
			column := &model.IndexColumn{
				Name:   key.Name,
				Offset: originColumn.Offset,
				Length: key.Length,
			}
			columns = append(columns, column)
		}
	}
	if len(columns) == len(index.Columns) {
		return &model.IndexInfo{
			Table:   index.Table,
			Unique:  index.Unique,
			Primary: index.Primary,
			State:   index.State,
			Tp:      index.Tp,
			Columns: columns,
		}
	}
	return nil
}

// handlePkExCase is handle pk exceptional case.
// e.g. "create table t(a int primary key, b int)".
func handlePkExCase(ti *model.TableInfo) *model.IndexInfo {
	if pk := ti.GetPkColInfo(); pk != nil {
		return &model.IndexInfo{
			Table:   ti.Name,
			Unique:  true,
			Primary: true,
			State:   model.StatePublic,
			Tp:      model.IndexTypeBtree,
			Columns: []*model.IndexColumn{{
				Name:   pk.Name,
				Offset: pk.Offset,
				Length: types.UnspecifiedLength,
			}},
		}
	}
	return nil
}

// isSpecifiedIndexColumn checks all of index's columns are matching 'fn'.
func isSpecifiedIndexColumn(index *model.IndexInfo, fn func(i int) bool) bool {
	for _, col := range index.Columns {
		if !fn(col.Offset) {
			return false
		}
	}
	return true
}
