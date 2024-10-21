// Copyright 2021 PingCAP, Inc.
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

package source

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	tableFilter "github.com/pingcap/tidb/pkg/util/table-filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// DMLType is the type of DML
type DMLType int32

const (
	// Insert means insert
	Insert DMLType = iota + 1
	// Delete means delete
	Delete
	// Replace means replace
	Replace
)

const (
	shieldDBName      = "_no__exists__db_"
	shieldTableName   = "_no__exists__table_"
	getSyncPointQuery = "SELECT primary_ts, secondary_ts FROM tidb_cdc.syncpoint_v1 ORDER BY primary_ts DESC LIMIT 1"
)

// ChecksumInfo stores checksum and count
type ChecksumInfo struct {
	Checksum uint64
	Count    int64
	Err      error
	Cost     time.Duration
}

// RowDataIterator represents the row data in source.
type RowDataIterator interface {
	// Next seeks the next row data, it used when compared rows.
	Next() (map[string]*dbutil.ColumnData, error)
	// Close release the resource.
	Close()
}

// TableAnalyzer represents the method in different source.
// each source has its own analyze function.
type TableAnalyzer interface {
	// AnalyzeSplitter picks the proper splitter.ChunkIterator according to table and source.
	AnalyzeSplitter(context.Context, *common.TableDiff, *splitter.RangeInfo) (splitter.ChunkIterator, error)
}

// Source is the interface for table
type Source interface {
	// GetTableAnalyzer pick the proper analyzer for different source.
	// the implement of this function is different in mysql/tidb.
	GetTableAnalyzer() TableAnalyzer

	// GetRangeIterator generates the range iterator with the checkpoint(*splitter.RangeInfo) and analyzer.
	// this is the mainly iterator across the whole sync diff.
	// One source has one range iterator to produce the range to channel.
	// there are many workers consume the range from the channel to compare.
	GetRangeIterator(context.Context, *splitter.RangeInfo, TableAnalyzer, int) (RangeIterator, error)

	// GetCountAndMD5 gets the md5 result and the count from given range.
	GetCountAndMD5(context.Context, *splitter.RangeInfo) *ChecksumInfo

	// GetCountForLackTable gets the count for tables that don't exist upstream or downstream.
	GetCountForLackTable(context.Context, *splitter.RangeInfo) int64

	// GetRowsIterator gets the row data iterator from given range.
	GetRowsIterator(context.Context, *splitter.RangeInfo) (RowDataIterator, error)

	// GenerateFixSQL generates the fix sql with given type.
	GenerateFixSQL(DMLType, map[string]*dbutil.ColumnData, map[string]*dbutil.ColumnData, int) string

	// GetTables represents the tableDiffs.
	GetTables() []*common.TableDiff

	// GetSourceStructInfo get the source table info from a given target table
	GetSourceStructInfo(context.Context, int) ([]*model.TableInfo, error)

	// GetDB represents the db connection.
	GetDB() *sql.DB

	// GetSnapshot represents the snapshot of source.
	// only TiDB source has the snapshot.
	// TODO refine the interface.
	GetSnapshot() string

	// Close ...
	Close()
}

// NewSources returns a new source
func NewSources(ctx context.Context, cfg *config.Config) (downstream Source, upstream Source, err error) {
	// init db connection for upstream / downstream.
	err = initDBConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tablesToBeCheck, err := initTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	tableDiffs := make([]*common.TableDiff, 0, len(tablesToBeCheck))
	for _, tableConfig := range tablesToBeCheck {
		newInfo, needUnifiedTimeZone := utils.ResetColumns(tableConfig.TargetTableInfo, tableConfig.IgnoreColumns)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: tableConfig.Schema,
			Table:  tableConfig.Table,
			Info:   newInfo,
			// TODO: field `IgnoreColumns` can be deleted.
			IgnoreColumns:       tableConfig.IgnoreColumns,
			Fields:              strings.Join(tableConfig.Fields, ","),
			Range:               tableConfig.Range,
			NeedUnifiedTimeZone: needUnifiedTimeZone,
			Collation:           tableConfig.Collation,
			ChunkSize:           tableConfig.ChunkSize,
		})

		// When the router set case-sensitive false,
		// that add rule match itself will make table case unsensitive.
		for _, d := range cfg.Task.SourceInstances {
			if _, ok := d.RouteTargetSet[dbutil.TableName(tableConfig.Schema, tableConfig.Table)]; ok {
				// There is a user rule routing to `tableConfig.Schema`.`tableConfig.Table`
				rules := d.Router.Match(tableConfig.Schema, tableConfig.Table)

				if len(rules) == 0 {
					// There is no self match in these user rules.
					// Need to shield the table for this source.
					if d.Router.AddRule(&router.TableRule{
						SchemaPattern: tableConfig.Schema,
						TablePattern:  tableConfig.Table,
						TargetSchema:  shieldDBName,
						TargetTable:   shieldTableName,
					}) != nil {
						return nil, nil, errors.Errorf("add shield rule failed [schema =  %s] [table = %s]", tableConfig.Schema, tableConfig.Table)
					}
				}
			} else if _, ok := d.RouteTargetSet[dbutil.TableName(tableConfig.Schema, "")]; ok {
				// There is a user rule routing to `tableConfig.Schema`
				rules := d.Router.Match(tableConfig.Schema, tableConfig.Table)

				if len(rules) == 0 {
					// There is no self match in these user rules.
					// Need to shield the table for this source.
					if d.Router.AddRule(&router.TableRule{
						SchemaPattern: tableConfig.Schema,
						TablePattern:  tableConfig.Table,
						TargetSchema:  shieldDBName,
						TargetTable:   shieldTableName,
					}) != nil {
						return nil, nil, errors.Errorf("add shield rule failed [schema =  %s] [table = %s]", tableConfig.Schema, tableConfig.Table)
					}
				}
			} else {
				// Add the default rule to match upper/lower case
				if d.Router.AddRule(&router.TableRule{
					SchemaPattern: tableConfig.Schema,
					TablePattern:  tableConfig.Table,
					TargetSchema:  tableConfig.Schema,
					TargetTable:   tableConfig.Table,
				}) != nil {
					return nil, nil, errors.Errorf("add rule failed [schema = %s] [table = %s]", tableConfig.Schema, tableConfig.Table)
				}
			}
		}
	}

	// Sort TableDiff is important!
	// because we compare table one by one.
	sort.Slice(tableDiffs, func(i, j int) bool {
		ti := utils.UniqueID(tableDiffs[i].Schema, tableDiffs[i].Table)
		tj := utils.UniqueID(tableDiffs[j].Schema, tableDiffs[j].Table)
		return strings.Compare(ti, tj) > 0
	})

	// If `bucket size` is much larger than `chunk size`,
	// we need to split the bucket into some chunks, which wastes much time.
	// So we use WorkPool to split buckets in parallel.
	// Besides, bucketSpliters of each table use shared WorkPool
	bucketSpliterPool := utils.NewWorkerPool(uint(cfg.CheckThreadCount), "bucketIter")
	// for mysql_shard, it needs `cfg.CheckThreadCount` + `cfg.SplitThreadCount` at most, because it cannot use bucket.
	mysqlConnCount := cfg.CheckThreadCount + cfg.SplitThreadCount
	upstream, err = buildSourceFromCfg(ctx, tableDiffs, mysqlConnCount, bucketSpliterPool, cfg.SkipNonExistingTable, cfg.Task.TargetCheckTables, cfg.Task.SourceInstances...)
	if err != nil {
		return nil, nil, errors.Annotate(err, "from upstream")
	}
	if len(upstream.GetTables()) == 0 {
		return nil, nil, errors.Errorf("no table need to be compared")
	}
	downstream, err = buildSourceFromCfg(ctx, upstream.GetTables(), mysqlConnCount, bucketSpliterPool, cfg.SkipNonExistingTable, cfg.Task.TargetCheckTables, cfg.Task.TargetInstance)
	if err != nil {
		return nil, nil, errors.Annotate(err, "from downstream")
	}
	return downstream, upstream, nil
}

func buildSourceFromCfg(
	ctx context.Context,
	tableDiffs []*common.TableDiff, connCount int,
	bucketSpliterPool *utils.WorkerPool,
	skipNonExistingTable bool,
	f tableFilter.Filter, dbs ...*config.DataSource,
) (Source, error) {
	if len(dbs) < 1 {
		return nil, errors.Errorf("no db config detected")
	}
	ok, err := dbutil.IsTiDB(ctx, dbs[0].Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "connect to db failed")
	}

	if ok {
		if len(dbs) == 1 {
			return NewTiDBSource(ctx, tableDiffs, dbs[0], bucketSpliterPool, f, skipNonExistingTable)
		}

		log.Fatal("Don't support check table in multiple tidb instance, please specify one tidb instance.")
	}
	return NewMySQLSources(ctx, tableDiffs, dbs, connCount, f, skipNonExistingTable)
}

func getAutoSnapshotPosition(cfg *mysql.Config) (string, string, error) {
	tmpConn, err := common.ConnectMySQL(cfg, 2)
	if err != nil {
		return "", "", errors.Annotatef(err, "connecting to auto-position tidb_snapshot failed")
	}
	defer tmpConn.Close()
	var primaryTs, secondaryTs string
	err = tmpConn.QueryRow(getSyncPointQuery).Scan(&primaryTs, &secondaryTs)
	if err != nil {
		return "", "", errors.Annotatef(err, "fetching auto-position tidb_snapshot failed")
	}
	return primaryTs, secondaryTs, nil
}

func initDBConn(_ context.Context, cfg *config.Config) error {
	// Fill in tidb_snapshot if it is set to AUTO
	// This is only supported when set to auto on both target/source.
	if cfg.Task.TargetInstance.IsAutoSnapshot() {
		if len(cfg.Task.SourceInstances) > 1 {
			return errors.Errorf("'auto' snapshot only supports one tidb source")
		}
		if !cfg.Task.SourceInstances[0].IsAutoSnapshot() {
			return errors.Errorf("'auto' snapshot should be set on both target and source")
		}
		primaryTs, secondaryTs, err := getAutoSnapshotPosition(cfg.Task.TargetInstance.ToDriverConfig())
		if err != nil {
			return err
		}
		cfg.Task.TargetInstance.SetSnapshot(secondaryTs)
		cfg.Task.SourceInstances[0].SetSnapshot(primaryTs)
	}
	// we had `cfg.SplitThreadCount` producers and `cfg.CheckThreadCount` consumer to use db connections maybe and `cfg.CheckThreadCount` splitter to split buckets.
	// so the connection count need to be cfg.SplitThreadCount + cfg.CheckThreadCount + cfg.CheckThreadCount.
	targetConn, err := common.ConnectMySQL(cfg.Task.TargetInstance.ToDriverConfig(), cfg.SplitThreadCount+2*cfg.CheckThreadCount)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.Task.TargetInstance.Conn = targetConn

	for _, source := range cfg.Task.SourceInstances {
		// If it is still set to AUTO it means it was not set on the target.
		// We require it to be set to AUTO on both.
		if source.IsAutoSnapshot() {
			return errors.Errorf("'auto' snapshot should be set on both target and source")
		}
		// connect source db with target db time_zone
		conn, err := common.ConnectMySQL(source.ToDriverConfig(), cfg.SplitThreadCount+2*cfg.CheckThreadCount)
		if err != nil {
			return errors.Trace(err)
		}
		source.Conn = conn
	}
	return nil
}

func initTables(ctx context.Context, cfg *config.Config) (cfgTables []*config.TableConfig, err error) {
	downStreamConn := cfg.Task.TargetInstance.Conn
	TargetTablesList := make([]*common.TableSource, 0)
	targetSchemas, err := dbutil.GetSchemas(ctx, downStreamConn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from target source")
	}

	for _, schema := range targetSchemas {
		if filter.IsSystemSchema(schema) {
			continue
		}
		allTables, err := dbutil.GetTables(ctx, downStreamConn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from target source %s", schema)
		}
		for _, t := range allTables {
			TargetTablesList = append(TargetTablesList, &common.TableSource{
				OriginSchema: schema,
				OriginTable:  t,
			})
		}
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	// cfg.Tables.Schema => cfg.Tables.Tables => target/source Schema.Table
	cfgTables = make([]*config.TableConfig, 0, len(TargetTablesList))
	version := utils.TryToGetVersion(ctx, downStreamConn)
	for _, tables := range TargetTablesList {
		if cfg.Task.TargetCheckTables.MatchTable(tables.OriginSchema, tables.OriginTable) {
			log.Debug("match target table", zap.String("table", dbutil.TableName(tables.OriginSchema, tables.OriginTable)))

			tableInfo, err := utils.GetTableInfoWithVersion(ctx, downStreamConn, tables.OriginSchema, tables.OriginTable, version)
			if err != nil {
				return nil, errors.Errorf("get table %s.%s's information error %s", tables.OriginSchema, tables.OriginTable, errors.ErrorStack(err))
			}
			// Initialize all the tables that matches the `target-check-tables`[config.toml] and appears in downstream.
			cfgTables = append(cfgTables, &config.TableConfig{
				Schema:          tables.OriginSchema,
				Table:           tables.OriginTable,
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
			})
		}
	}

	// Reset fields of some tables of `cfgTables` according to `table-configs`[config.toml].
	// The table in `table-configs`[config.toml] should exist in both `target-check-tables`[config.toml] and tables from downstream.
	for i, table := range cfg.Task.TargetTableConfigs {
		// parse every config to find target table.
		cfgFilter, err := tableFilter.Parse(table.TargetTables)
		if err != nil {
			return nil, errors.Errorf("unable to parse target table for the %dth config", i)
		}
		// iterate all target tables to make sure
		// 1. one table only match at most one config.
		// 2. config can miss table.
		for _, cfgTable := range cfgTables {
			if cfgFilter.MatchTable(cfgTable.Schema, cfgTable.Table) {
				if cfgTable.HasMatched {
					return nil, errors.Errorf("different config matched to same target table %s.%s", cfgTable.Schema, cfgTable.Table)
				}
				if table.Range != "" {
					cfgTable.Range = table.Range
				}
				cfgTable.IgnoreColumns = table.IgnoreColumns
				cfgTable.Fields = table.Fields
				cfgTable.Collation = table.Collation
				cfgTable.ChunkSize = table.ChunkSize
				cfgTable.HasMatched = true
			}
		}
	}
	return cfgTables, nil
}

// RangeIterator generate next chunk for the whole tables lazily.
type RangeIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next(context.Context) (*splitter.RangeInfo, error)

	Close()
}

func checkTableMatched(tableDiffs []*common.TableDiff, targetMap map[string]struct{}, sourceMap map[string]struct{}, skipNonExistingTable bool) ([]*common.TableDiff, error) {
	tableIndexMap := getIndexMapForTable(tableDiffs)
	// check target exists but source not found
	for tableDiff := range targetMap {
		// target table have all passed in tableFilter
		if _, ok := sourceMap[tableDiff]; !ok {
			if !skipNonExistingTable {
				return tableDiffs, errors.Errorf("the source has no table to be compared. target-table is `%s`", tableDiff)
			}
			index := tableIndexMap[tableDiff]
			if tableDiffs[index].TableLack == 0 {
				tableDiffs[index].TableLack = common.UpstreamTableLackFlag
				log.Info("the source has no table to be compared", zap.String("target-table", tableDiff))
			}
		}
	}
	// check source exists but target not found
	for tableDiff := range sourceMap {
		// need check source table have passd in tableFilter here
		if _, ok := targetMap[tableDiff]; !ok {
			if !skipNonExistingTable {
				return tableDiffs, errors.Errorf("the target has no table to be compared. source-table is `%s`", tableDiff)
			}
			slice := strings.Split(strings.Replace(tableDiff, "`", "", -1), ".")
			tableDiffs = append(tableDiffs, &common.TableDiff{
				Schema:    slice[0],
				Table:     slice[1],
				TableLack: common.DownstreamTableLackFlag,
			})
			log.Info("the target has no table to be compared", zap.String("source-table", tableDiff))
		}
	}
	log.Info("table match check finished")
	return tableDiffs, nil
}

func getIndexMapForTable(tableDiffs []*common.TableDiff) map[string]int {
	tableIndexMap := make(map[string]int)
	for i := 0; i < len(tableDiffs); i++ {
		tableUniqueID := utils.UniqueID(tableDiffs[i].Schema, tableDiffs[i].Table)
		tableIndexMap[tableUniqueID] = i
	}
	return tableIndexMap
}
