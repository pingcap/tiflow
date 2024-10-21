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
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	tableFilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// MySQLTableAnalyzer is used to analyze MySQL table
type MySQLTableAnalyzer struct {
	sourceTableMap map[string][]*common.TableShardSource
}

// AnalyzeSplitter return an iterator for current table
func (a *MySQLTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	matchedSources := getMatchedSourcesForTable(a.sourceTableMap, table)

	// It's useful we are not able to pick shard merge source as workSource to generate ChunksIterator.
	if len(matchedSources) > 1 {
		log.Fatal("unreachable, shard merge table cannot generate splitter for now.")
	}
	// Shallow Copy
	originTable := *table
	originTable.Schema = matchedSources[0].OriginSchema
	originTable.Table = matchedSources[0].OriginTable
	progressID := dbutil.TableName(table.Schema, table.Table)
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, progressID, &originTable, matchedSources[0].DBConn, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

// MySQLSources represent one table in MySQL
type MySQLSources struct {
	tableDiffs []*common.TableDiff

	sourceTablesMap map[string][]*common.TableShardSource
}

func getMatchedSourcesForTable(sourceTablesMap map[string][]*common.TableShardSource, table *common.TableDiff) []*common.TableShardSource {
	if sourceTablesMap == nil {
		log.Fatal("unreachable, source tables map shouldn't be nil.")
	}
	matchSources, ok := sourceTablesMap[utils.UniqueID(table.Schema, table.Table)]
	if !ok && common.AllTableExist(table.TableLack) {
		log.Fatal("unreachable, no match source tables in mysql shard source.")
	}
	return matchSources
}

// GetTableAnalyzer get analyzer for current table
func (s *MySQLSources) GetTableAnalyzer() TableAnalyzer {
	return &MySQLTableAnalyzer{
		s.sourceTablesMap,
	}
}

// GetRangeIterator get range iterator
func (s *MySQLSources) GetRangeIterator(ctx context.Context, r *splitter.RangeInfo, analyzer TableAnalyzer, splitThreadCount int) (RangeIterator, error) {
	return NewChunksIterator(ctx, analyzer, s.tableDiffs, r, splitThreadCount)
}

// Close close the current table
func (s *MySQLSources) Close() {
	for _, t := range s.sourceTablesMap {
		for _, db := range t {
			db.DBConn.Close()
		}
	}
}

// GetCountAndMD5 return count and checksum
func (s *MySQLSources) GetCountAndMD5(ctx context.Context, tableRange *splitter.RangeInfo) *ChecksumInfo {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()

	matchSources := getMatchedSourcesForTable(s.sourceTablesMap, table)
	infoCh := make(chan *ChecksumInfo, len(s.sourceTablesMap))

	for _, ms := range matchSources {
		go func(ms *common.TableShardSource) {
			count, checksum, err := utils.GetCountAndMD5Checksum(ctx, ms.DBConn, ms.OriginSchema, ms.OriginTable, table.Info, chunk.Where, chunk.Args)
			infoCh <- &ChecksumInfo{
				Checksum: checksum,
				Count:    count,
				Err:      err,
			}
		}(ms)
	}
	defer close(infoCh)

	var (
		err           error
		totalCount    int64
		totalChecksum uint64
	)

	for range matchSources {
		info := <-infoCh
		// catch the first error
		if err == nil && info.Err != nil {
			err = info.Err
		}
		totalCount += info.Count
		totalChecksum ^= info.Checksum
	}

	cost := time.Since(beginTime)
	return &ChecksumInfo{
		Checksum: totalChecksum,
		Count:    totalCount,
		Err:      err,
		Cost:     cost,
	}
}

// GetCountForLackTable return count for lack table
func (s *MySQLSources) GetCountForLackTable(ctx context.Context, tableRange *splitter.RangeInfo) int64 {
	table := s.tableDiffs[tableRange.GetTableIndex()]
	var totalCount int64

	matchSources := getMatchedSourcesForTable(s.sourceTablesMap, table)
	for _, ms := range matchSources {
		count, _ := dbutil.GetRowCount(ctx, ms.DBConn, ms.OriginSchema, ms.OriginTable, "", nil)
		totalCount += count
	}
	return totalCount
}

// GetTables return all tables
func (s *MySQLSources) GetTables() []*common.TableDiff {
	return s.tableDiffs
}

// GenerateFixSQL generate SQL
func (s *MySQLSources) GenerateFixSQL(t DMLType, upstreamData, downstreamData map[string]*dbutil.ColumnData, tableIndex int) string {
	switch t {
	case Insert:
		return utils.GenerateReplaceDML(upstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	case Delete:
		return utils.GenerateDeleteDML(downstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	case Replace:
		return utils.GenerateReplaceDMLWithAnnotation(upstreamData, downstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	default:
		log.Fatal("Don't support this type", zap.Any("dml type", t))
	}
	return ""
}

// GetRowsIterator get iterator for current table
func (s *MySQLSources) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()

	sourceRows := make(map[int]*sql.Rows)

	table := s.tableDiffs[tableRange.GetTableIndex()]
	// for tables that do not exist upstream or downstream
	if !common.AllTableExist(table.TableLack) {
		return nil, nil
	}
	matchSources := getMatchedSourcesForTable(s.sourceTablesMap, table)

	var rowsQuery string
	var orderKeyCols []*model.ColumnInfo
	for i, ms := range matchSources {
		rowsQuery, orderKeyCols = utils.GetTableRowsQueryFormat(ms.OriginSchema, ms.OriginTable, table.Info, table.Collation)
		query := fmt.Sprintf(rowsQuery, chunk.Where)
		rows, err := ms.DBConn.QueryContext(ctx, query, chunk.Args...)
		defer func() {
			if rows != nil {
				_ = rows.Err()
			}
		}()
		if err != nil {
			return nil, errors.Trace(err)
		}
		sourceRows[i] = rows
	}

	sourceRowDatas := &common.RowDatas{
		Rows:         make([]common.RowData, 0, len(sourceRows)),
		OrderKeyCols: orderKeyCols,
	}
	heap.Init(sourceRowDatas)
	// first push one row from all the sources into heap
	for source, sourceRow := range sourceRows {
		rowData, err := getRowData(sourceRow)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData != nil {
			heap.Push(sourceRowDatas, common.RowData{
				Data:   rowData,
				Source: source,
			})
		} else {
			if sourceRow.Err() != nil {
				return nil, sourceRow.Err()
			}
		}
	}

	return &MultiSourceRowsIterator{
		sourceRows:     sourceRows,
		sourceRowDatas: sourceRowDatas,
	}, nil
}

// GetDB get the current DB
func (s *MySQLSources) GetDB() *sql.DB {
	// return any of them is ok
	for _, st := range s.sourceTablesMap {
		for _, db := range st {
			return db.DBConn
		}
	}
	log.Warn("the source has no DB connection.")
	return nil
}

// GetSnapshot get the current snapshot
func (s *MySQLSources) GetSnapshot() string {
	log.Fatal("unreachable!, mysql doesn't have the snapshot")
	return ""
}

// GetSourceStructInfo get the current table info
func (s *MySQLSources) GetSourceStructInfo(ctx context.Context, tableIndex int) ([]*model.TableInfo, error) {
	tableDiff := s.GetTables()[tableIndex]
	// for tables that do not exist upstream or downstream
	if !common.AllTableExist(tableDiff.TableLack) {
		return nil, nil
	}
	tableSources := getMatchedSourcesForTable(s.sourceTablesMap, tableDiff)
	sourceTableInfos := make([]*model.TableInfo, len(tableSources))
	for i, tableSource := range tableSources {
		sourceSchema, sourceTable := tableSource.OriginSchema, tableSource.OriginTable
		sourceTableInfo, err := utils.GetTableInfo(ctx, tableSource.DBConn, sourceSchema, sourceTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sourceTableInfo, _ = utils.ResetColumns(sourceTableInfo, tableDiff.IgnoreColumns)
		sourceTableInfos[i] = sourceTableInfo
	}
	return sourceTableInfos, nil
}

// MultiSourceRowsIterator is used to iterate rows from multi source
type MultiSourceRowsIterator struct {
	sourceRows     map[int]*sql.Rows
	sourceRowDatas *common.RowDatas
}

func getRowData(rows *sql.Rows) (rowData map[string]*dbutil.ColumnData, err error) {
	for rows.Next() {
		rowData, err = dbutil.ScanRow(rows)
		return
	}
	return
}

// Next return the next row
func (ms *MultiSourceRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	// Before running getSourceRow, heap save one row from all the sources,
	// otherwise this source has read to the end. Each row should be the smallest in each source.
	// Once there is one row popped, we need to immediately push one row, which is from the same source, into the heap.
	// all the sources had read to the end, no data to return
	if len(ms.sourceRowDatas.Rows) == 0 {
		return nil, nil
	}
	rowData := heap.Pop(ms.sourceRowDatas).(common.RowData)
	newRowData, err := getRowData(ms.sourceRows[rowData.Source])
	if err != nil {
		return nil, err
	}
	if newRowData != nil {
		heap.Push(ms.sourceRowDatas, common.RowData{
			Data:   newRowData,
			Source: rowData.Source,
		})
	} else {
		if ms.sourceRows[rowData.Source].Err() != nil {
			return nil, ms.sourceRows[rowData.Source].Err()
		}
	}
	return rowData.Data, nil
}

// Close return all sources
func (ms *MultiSourceRowsIterator) Close() {
	for _, s := range ms.sourceRows {
		s.Close()
	}
}

// NewMySQLSources return sources for MySQL tables
func NewMySQLSources(ctx context.Context, tableDiffs []*common.TableDiff, ds []*config.DataSource, threadCount int, f tableFilter.Filter, skipNonExistingTable bool) (Source, error) {
	sourceTablesMap := make(map[string][]*common.TableShardSource)
	// we should get the real table name
	// and real table row query from sourceDB.
	targetUniqueTableMap := make(map[string]struct{})
	for _, tableDiff := range tableDiffs {
		targetUniqueTableMap[utils.UniqueID(tableDiff.Schema, tableDiff.Table)] = struct{}{}
	}

	// only used for check
	sourceTablesAfterRoute := make(map[string]struct{})

	for i, sourceDB := range ds {
		sourceSchemas, err := dbutil.GetSchemas(ctx, sourceDB.Conn)
		if err != nil {
			return nil, errors.Annotatef(err, "get schemas from %d source", i)
		}

		// use this map to record max Connection for this source.
		maxSourceRouteTableCount := make(map[string]int)
		for _, schema := range sourceSchemas {
			// Skip system schema.
			if filter.IsSystemSchema(schema) {
				continue
			}
			allTables, err := dbutil.GetTables(ctx, sourceDB.Conn, schema)
			if err != nil {
				return nil, errors.Annotatef(err, "get tables from %d source %s", i, schema)
			}
			for _, table := range allTables {
				targetSchema, targetTable := schema, table
				if sourceDB.Router != nil {
					targetSchema, targetTable, err = sourceDB.Router.Route(schema, table)
					if err != nil {
						return nil, errors.Errorf("get route result for %d source %s.%s failed, error %v", i, schema, table, err)
					}
				}
				uniqueID := utils.UniqueID(targetSchema, targetTable)
				isMatched := f.MatchTable(targetSchema, targetTable)
				if isMatched {
					// if match the filter, we should respect it and check target has this table later.
					sourceTablesAfterRoute[uniqueID] = struct{}{}
				}
				if _, ok := targetUniqueTableMap[uniqueID]; !ok && !(isMatched && skipNonExistingTable) {
					continue
				}
				maxSourceRouteTableCount[uniqueID]++
				if _, ok := sourceTablesMap[uniqueID]; !ok {
					sourceTablesMap[uniqueID] = make([]*common.TableShardSource, 0)
				}
				sourceTablesMap[uniqueID] = append(sourceTablesMap[uniqueID], &common.TableShardSource{
					TableSource: common.TableSource{
						OriginSchema: schema,
						OriginTable:  table,
					},
					DBConn: sourceDB.Conn,
				})
			}
		}
		maxConn := 0
		for _, c := range maxSourceRouteTableCount {
			if c > maxConn {
				maxConn = c
			}
		}
		log.Info("will increase connection configurations for DB of instance",
			zap.Int("connection limit", maxConn*threadCount+1))
		// Set this conn to max
		sourceDB.Conn.SetMaxOpenConns(maxConn*threadCount + 1)
		sourceDB.Conn.SetMaxIdleConns(maxConn*threadCount + 1)

	}

	tableDiffs, err := checkTableMatched(tableDiffs, targetUniqueTableMap, sourceTablesAfterRoute, skipNonExistingTable)
	if err != nil {
		return nil, errors.Annotatef(err, "please make sure the filter is correct.")
	}

	mss := &MySQLSources{
		tableDiffs:      tableDiffs,
		sourceTablesMap: sourceTablesMap,
	}
	return mss, nil
}
