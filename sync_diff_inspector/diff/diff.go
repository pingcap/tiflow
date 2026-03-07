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

package diff

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// Check file exists or not
func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

// GetSnapshot get the snapshot
func GetSnapshot(latestSnap []string, snap string, db *sql.DB) string {
	if len(latestSnap) != 1 {
		return snap
	}

	latestSnapshotVal, err := utils.ParseSnapshotToTSO(db, latestSnap[0])
	if err != nil || latestSnapshotVal == 0 {
		return snap
	}

	snapshotVal, err := utils.ParseSnapshotToTSO(db, snap)
	if err != nil {
		return latestSnap[0]
	}

	// compare the snapshot and choose the small one to lock
	if latestSnapshotVal < snapshotVal {
		return latestSnap[0]
	}
	return snap
}

const (
	// checkpointFile represents the checkpoints' file name which used for save and loads chunks
	checkpointFile = "sync_diff_checkpoints.pb"
)

// ChunkDML SQL struct for each chunk
type ChunkDML struct {
	node      *checkpoints.Node
	sqls      []string
	rowAdd    int
	rowDelete int
}

// Diff contains two sql DB, used for comparing.
type Diff struct {
	// we may have multiple sources in dm sharding sync.
	upstream   source.Source
	downstream source.Source

	// workSource is one of upstream/downstream by some policy in #pickSource.
	workSource source.Source

	checkThreadCount int
	splitThreadCount int
	exportFixSQL     bool
	sqlWg            sync.WaitGroup
	checkpointWg     sync.WaitGroup

	FixSQLDir     string
	CheckpointDir string

	sqlCh      chan *ChunkDML
	cp         *checkpoints.Checkpoint
	startRange *splitter.RangeInfo
	report     *report.Report
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *config.Config) (diff *Diff, err error) {
	diff = &Diff{
		checkThreadCount: cfg.CheckThreadCount,
		splitThreadCount: cfg.SplitThreadCount,
		exportFixSQL:     cfg.ExportFixSQL,
		sqlCh:            make(chan *ChunkDML, splitter.DefaultChannelBuffer),
		cp:               new(checkpoints.Checkpoint),
		report:           report.NewReport(&cfg.Task),
	}
	if err = diff.init(ctx, cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

// PrintSummary print the summary and return true if report is passed
func (df *Diff) PrintSummary(ctx context.Context) bool {
	// Stop updating progress bar so that summary won't be flushed.
	progress.Close()
	df.report.CalculateTotalSize(ctx, df.downstream.GetDB())
	err := df.report.CommitSummary()
	if err != nil {
		log.Fatal("failed to commit report", zap.Error(err))
	}
	df.report.Print(os.Stdout)
	return df.report.Result == report.Pass
}

// Close the current struct
func (df *Diff) Close() {
	if df.upstream != nil {
		df.upstream.Close()
	}
	if df.downstream != nil {
		df.downstream.Close()
	}

	failpoint.Inject("wait-for-checkpoint", func() {
		log.Info("failpoint wait-for-checkpoint injected, skip delete checkpoint file.")
		failpoint.Return()
	})

	if err := os.Remove(filepath.Join(df.CheckpointDir, checkpointFile)); err != nil && !os.IsNotExist(err) {
		log.Fatal("fail to remove the checkpoint file", zap.String("error", err.Error()))
	}
}

func (df *Diff) init(ctx context.Context, cfg *config.Config) (err error) {
	// TODO adjust config
	setTiDBCfg()

	df.downstream, df.upstream, err = source.NewSources(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	df.workSource = df.pickSource(ctx)
	df.FixSQLDir = cfg.Task.FixDir
	df.CheckpointDir = cfg.Task.CheckpointDir

	sourceConfigs, targetConfig, err := getConfigsForReport(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	df.report.Init(df.downstream.GetTables(), sourceConfigs, targetConfig)
	if err := df.initCheckpoint(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (df *Diff) initCheckpoint() error {
	df.cp.Init()

	finishTableNums := 0
	path := filepath.Join(df.CheckpointDir, checkpointFile)
	if fileExists(path) {
		node, reportInfo, err := df.cp.LoadChunk(path)
		if err != nil {
			return errors.Annotate(err, "the checkpoint load process failed")
		}

		// this need not be synchronized, because at the moment, the is only one thread access the section
		log.Info("load checkpoint",
			zap.Any("chunk index", node.GetID()),
			zap.Reflect("chunk", node),
			zap.String("state", node.GetState()))
		df.cp.InitCurrentSavedID(node)

		if node != nil {
			// remove the sql file that ID bigger than node.
			// cause we will generate these sql again.
			err = df.removeSQLFiles(node.GetID())
			if err != nil {
				return errors.Trace(err)
			}
			df.startRange = splitter.FromNode(node)
			df.report.LoadReport(reportInfo)
			finishTableNums = df.startRange.GetTableIndex()
			if df.startRange.ChunkRange.Type == chunk.Empty {
				// chunk_iter will skip this table directly
				finishTableNums++
			}
		}
	} else {
		log.Info("not found checkpoint file, start from beginning")
		id := &chunk.CID{TableIndex: -1, BucketIndexLeft: -1, BucketIndexRight: -1, ChunkIndex: -1, ChunkCnt: 0}
		err := df.removeSQLFiles(id)
		if err != nil {
			return errors.Trace(err)
		}
	}
	progress.Init(len(df.workSource.GetTables()), finishTableNums)
	return nil
}

func encodeConfig(config *report.Config) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func getConfigsForReport(cfg *config.Config) ([][]byte, []byte, error) {
	sourceConfigs := make([]*report.Config, len(cfg.Task.SourceInstances))
	for i := 0; i < len(cfg.Task.SourceInstances); i++ {
		instance := cfg.Task.SourceInstances[i]

		sourceConfigs[i] = &report.Config{
			Host:     instance.Host,
			Port:     instance.Port,
			User:     instance.User,
			Snapshot: instance.Snapshot,
			SQLMode:  instance.SQLMode,
		}
	}
	instance := cfg.Task.TargetInstance
	targetConfig := &report.Config{
		Host:     instance.Host,
		Port:     instance.Port,
		User:     instance.User,
		Snapshot: instance.Snapshot,
		SQLMode:  instance.SQLMode,
	}
	sourceBytes := make([][]byte, len(sourceConfigs))
	var err error
	for i := range sourceBytes {
		sourceBytes[i], err = encodeConfig(sourceConfigs[i])
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	targetBytes, err := encodeConfig(targetConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return sourceBytes, targetBytes, nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunksIterator(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer chunksIter.Close()
	pool := utils.NewWorkerPool(uint(df.checkThreadCount), "consumer")
	stopCh := make(chan struct{})

	df.checkpointWg.Add(1)
	go df.handleCheckpoints(ctx, stopCh)
	df.sqlWg.Add(1)
	go df.writeSQLs(ctx)

	defer func() {
		pool.WaitFinished()
		log.Debug("all consume tasks finished")
		// close the sql channel
		close(df.sqlCh)
		df.sqlWg.Wait()
		stopCh <- struct{}{}
		df.checkpointWg.Wait()
	}()

	for {
		c, err := chunksIter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if c == nil {
			// finish read the tables
			break
		}
		log.Info("global consume chunk info", zap.Any("chunk index", c.ChunkRange.Index), zap.Any("chunk bound", c.ChunkRange.Bounds))
		pool.Apply(func() {
			isEqual := df.consume(ctx, c)
			if !isEqual {
				progress.FailTable(c.ProgressID)
			}
			progress.Inc(c.ProgressID)
		})
	}

	return nil
}

// StructEqual compare tables from downstream
func (df *Diff) StructEqual(ctx context.Context) error {
	tables := df.downstream.GetTables()
	tableIndex := 0
	if df.startRange != nil {
		tableIndex = df.startRange.ChunkRange.Index.TableIndex
	}
	for ; tableIndex < len(tables); tableIndex++ {
		isEqual, isSkip, isAllTableExist := false, true, tables[tableIndex].TableLack
		if common.AllTableExist(isAllTableExist) {
			var err error
			isEqual, isSkip, err = df.compareStruct(ctx, tableIndex)
			if err != nil {
				return errors.Trace(err)
			}
		}
		progress.RegisterTable(dbutil.TableName(tables[tableIndex].Schema, tables[tableIndex].Table), !isEqual, isSkip, isAllTableExist)
		df.report.SetTableStructCheckResult(tables[tableIndex].Schema, tables[tableIndex].Table, isEqual, isSkip, isAllTableExist)
	}
	return nil
}

func (df *Diff) compareStruct(ctx context.Context, tableIndex int) (isEqual bool, isSkip bool, err error) {
	sourceTableInfos, err := df.upstream.GetSourceStructInfo(ctx, tableIndex)
	if err != nil {
		return false, true, errors.Trace(err)
	}
	table := df.downstream.GetTables()[tableIndex]
	isEqual, isSkip = utils.CompareStruct(sourceTableInfos, table.Info)
	table.IgnoreDataCheck = isSkip
	return isEqual, isSkip, nil
}

func (df *Diff) startGCKeeperForTiDB(ctx context.Context, db *sql.DB, snap string) {
	pdCli, _ := utils.GetPDClientForGC(ctx, db)
	if pdCli != nil {
		// Get latest snapshot
		latestSnap, err := utils.GetSnapshot(ctx, db)
		if err != nil {
			log.Info("failed to get snapshot, user should guarantee the GC stopped during diff progress.")
			return
		}

		snap = GetSnapshot(latestSnap, snap, db)

		err = utils.StartGCSavepointUpdateService(ctx, pdCli, db, snap)
		if err != nil {
			log.Info("failed to keep snapshot, user should guarantee the GC stopped during diff progress.")
		} else {
			log.Info("start update service to keep GC stopped automatically")
		}
	}
}

// pickSource pick one proper source to do some work. e.g. generate chunks
func (df *Diff) pickSource(ctx context.Context) source.Source {
	workSource := df.downstream
	if ok, _ := dbutil.IsTiDB(ctx, df.upstream.GetDB()); ok {
		log.Info("The upstream is TiDB. pick it as work source candidate")
		df.startGCKeeperForTiDB(ctx, df.upstream.GetDB(), df.upstream.GetSnapshot())
		workSource = df.upstream
	}
	if ok, _ := dbutil.IsTiDB(ctx, df.downstream.GetDB()); ok {
		log.Info("The downstream is TiDB. pick it as work source first")
		df.startGCKeeperForTiDB(ctx, df.downstream.GetDB(), df.downstream.GetSnapshot())
		workSource = df.downstream
	}
	return workSource
}

func (df *Diff) generateChunksIterator(ctx context.Context) (source.RangeIterator, error) {
	return df.workSource.GetRangeIterator(ctx, df.startRange, df.workSource.GetTableAnalyzer(), df.splitThreadCount)
}

func (df *Diff) handleCheckpoints(ctx context.Context, stopCh chan struct{}) {
	// a background goroutine which will insert the verified chunk,
	// and periodically save checkpoint
	log.Info("start handleCheckpoint goroutine")
	defer func() {
		log.Info("close handleCheckpoint goroutine")
		df.checkpointWg.Done()
	}()
	flush := func() {
		chunk := df.cp.GetChunkSnapshot()
		if chunk != nil {
			tableDiff := df.downstream.GetTables()[chunk.GetTableIndex()]
			schema, table := tableDiff.Schema, tableDiff.Table
			r, err := df.report.GetSnapshot(chunk.GetID(), schema, table)
			if err != nil {
				log.Warn("fail to save the report", zap.Error(err))
			}
			_, err = df.cp.SaveChunk(ctx, filepath.Join(df.CheckpointDir, checkpointFile), chunk, r)
			if err != nil {
				log.Warn("fail to save the chunk", zap.Error(err))
				// maybe we should panic, because SaveChunk method should not failed.
			}
		}
	}
	defer flush()
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop do checkpoint by context done")
			return
		case <-stopCh:
			log.Info("Stop do checkpoint")
			return
		case <-time.After(10 * time.Second):
			flush()
		}
	}
}

func (df *Diff) consume(ctx context.Context, rangeInfo *splitter.RangeInfo) bool {
	dml := &ChunkDML{
		node: rangeInfo.ToNode(),
	}
	defer func() { df.sqlCh <- dml }()
	tableDiff := df.downstream.GetTables()[rangeInfo.GetTableIndex()]
	schema, table := tableDiff.Schema, tableDiff.Table
	id := rangeInfo.ChunkRange.Index
	if rangeInfo.ChunkRange.Type == chunk.Empty {
		dml.node.State = checkpoints.IgnoreState
		// for tables that don't exist upstream or downstream
		if !common.AllTableExist(tableDiff.TableLack) {
			upCount := df.upstream.GetCountForLackTable(ctx, rangeInfo)
			downCount := df.downstream.GetCountForLackTable(ctx, rangeInfo)
			df.report.SetTableDataCheckResult(schema, table, false, int(upCount), int(downCount), upCount, downCount, id)
			return false
		}
		return true
	}

	var state string = checkpoints.SuccessState

	isEqual, upCount, downCount, err := df.compareChecksumAndGetCount(ctx, rangeInfo)
	if err != nil {
		// If an error occurs during the checksum phase, skip the data compare phase.
		state = checkpoints.FailedState
		df.report.SetTableMeetError(schema, table, err)
	} else if !isEqual {
		state = checkpoints.FailedState
		// if the chunk's checksum differ, try to do binary check
		info := rangeInfo
		if upCount > splitter.SplitThreshold {
			log.Debug("count greater than threshold, start do bingenerate", zap.Any("chunk id", rangeInfo.ChunkRange.Index), zap.Int64("upstream chunk size", upCount))
			info, err = df.BinGenerate(ctx, df.workSource, rangeInfo, upCount)
			if err != nil {
				log.Error("fail to do binary search.", zap.Error(err))
				df.report.SetTableMeetError(schema, table, err)
				// reuse rangeInfo to compare data
				info = rangeInfo
			} else {
				log.Debug("bin generate finished", zap.Reflect("chunk", info.ChunkRange), zap.Any("chunk id", info.ChunkRange.Index))
			}
		}
		isDataEqual, err := df.compareRows(ctx, info, dml)
		if err != nil {
			df.report.SetTableMeetError(schema, table, err)
		}
		isEqual = isDataEqual
	}
	dml.node.State = state
	df.report.SetTableDataCheckResult(schema, table, isEqual, dml.rowAdd, dml.rowDelete, upCount, downCount, id)
	return isEqual
}

// BinGenerate ...
func (df *Diff) BinGenerate(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
	tableDiff := targetSource.GetTables()[tableRange.GetTableIndex()]
	indices := dbutil.FindAllIndex(tableDiff.Info)
	// if no index, do not split
	if len(indices) == 0 {
		log.Warn("cannot found an index to split and disable the BinGenerate",
			zap.String("table", dbutil.TableName(tableDiff.Schema, tableDiff.Table)))
		return tableRange, nil
	}
	var index *model.IndexInfo
	// using the index
	for _, i := range indices {
		if tableRange.IndexID == i.ID {
			index = i
			break
		}
	}
	if index == nil {
		log.Warn("have indices but cannot found a proper index to split and disable the BinGenerate",
			zap.String("table", dbutil.TableName(tableDiff.Schema, tableDiff.Table)))
		return tableRange, nil
	}
	// TODO use selectivity from utils.GetBetterIndex
	// only support PK/UK
	if !(index.Primary || index.Unique) {
		log.Warn("BinGenerate only support PK/UK")
		return tableRange, nil
	}

	log.Debug("index for BinGenerate", zap.String("index", index.Name.O))
	indexColumns := utils.GetColumnsFromIndex(index, tableDiff.Info)
	if len(indexColumns) == 0 {
		log.Warn("fail to get columns of the selected index, directly return the origin chunk")
		return tableRange, nil
	}

	return df.binSearch(ctx, targetSource, tableRange, count, tableDiff, indexColumns)
}

func (df *Diff) binSearch(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64, tableDiff *common.TableDiff, indexColumns []*model.ColumnInfo) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
	var (
		isEqual1, isEqual2 bool
		count1, count2     int64
	)
	tableRange1 := tableRange.Copy()
	tableRange2 := tableRange.Copy()

	chunkLimits, args := tableRange.ChunkRange.ToString(tableDiff.Collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, tableDiff.Range)
	midValues, err := utils.GetApproximateMidBySize(ctx, targetSource.GetDB(), tableDiff.Schema, tableDiff.Table, indexColumns, limitRange, args, count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if midValues == nil {
		// TODO Since the count is from upstream,
		// the midValues may be empty when downstream has much less rows in this chunk.
		return tableRange, nil
	}
	log.Debug("mid values", zap.Reflect("mid values", midValues), zap.Reflect("indices", indexColumns), zap.Reflect("bounds", tableRange.ChunkRange.Bounds))
	log.Debug("table ranges", zap.Reflect("original range", tableRange))
	for i := range indexColumns {
		log.Debug("update tableRange", zap.String("field", indexColumns[i].Name.O), zap.String("value", midValues[indexColumns[i].Name.O]))
		tableRange1.Update(indexColumns[i].Name.O, "", midValues[indexColumns[i].Name.O], false, true, tableDiff.Collation, tableDiff.Range)
		tableRange2.Update(indexColumns[i].Name.O, midValues[indexColumns[i].Name.O], "", true, false, tableDiff.Collation, tableDiff.Range)
	}
	log.Debug("table ranges", zap.Reflect("tableRange 1", tableRange1), zap.Reflect("tableRange 2", tableRange2))
	isEqual1, count1, _, err = df.compareChecksumAndGetCount(ctx, tableRange1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	isEqual2, count2, _, err = df.compareChecksumAndGetCount(ctx, tableRange2)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if count1+count2 != count {
		log.Fatal("the count is not correct",
			zap.Int64("count1", count1),
			zap.Int64("count2", count2),
			zap.Int64("count", count))
	}
	log.Info("chunk split successfully",
		zap.Any("chunk id", tableRange.ChunkRange.Index),
		zap.Int64("count1", count1),
		zap.Int64("count2", count2))

	// If there is a count zero, we think the range is very small.
	if (!isEqual1 && !isEqual2) || (count1 == 0 || count2 == 0) {
		return tableRange, nil
	} else if !isEqual1 {
		c, err := df.binSearch(ctx, targetSource, tableRange1, count1, tableDiff, indexColumns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else if !isEqual2 {
		c, err := df.binSearch(ctx, targetSource, tableRange2, count2, tableDiff, indexColumns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	}

	// TODO: handle the error to foreground
	log.Fatal("the isEqual1 and isEqual2 cannot be both true")
	return nil, nil
}

func (df *Diff) compareChecksumAndGetCount(ctx context.Context, tableRange *splitter.RangeInfo) (bool, int64, int64, error) {
	var wg sync.WaitGroup
	var upstreamInfo, downstreamInfo *source.ChecksumInfo
	wg.Add(1)
	go func() {
		defer wg.Done()
		upstreamInfo = df.upstream.GetCountAndMD5(ctx, tableRange)
	}()
	downstreamInfo = df.downstream.GetCountAndMD5(ctx, tableRange)
	wg.Wait()

	if upstreamInfo.Err != nil {
		log.Warn("failed to compare upstream checksum")
		return false, -1, -1, errors.Trace(upstreamInfo.Err)
	}
	if downstreamInfo.Err != nil {
		log.Warn("failed to compare downstream checksum")
		return false, -1, -1, errors.Trace(downstreamInfo.Err)

	}

	if upstreamInfo.Count == downstreamInfo.Count && upstreamInfo.Checksum == downstreamInfo.Checksum {
		return true, upstreamInfo.Count, downstreamInfo.Count, nil
	}
	log.Debug("checksum doesn't match, need to compare rows",
		zap.Any("chunk id", tableRange.ChunkRange.Index),
		zap.String("table", df.workSource.GetTables()[tableRange.GetTableIndex()].Table),
		zap.Int64("upstream chunk size", upstreamInfo.Count),
		zap.Int64("downstream chunk size", downstreamInfo.Count),
		zap.Uint64("upstream checksum", upstreamInfo.Checksum),
		zap.Uint64("downstream checksum", downstreamInfo.Checksum))
	return false, upstreamInfo.Count, downstreamInfo.Count, nil
}

func (df *Diff) compareRows(ctx context.Context, rangeInfo *splitter.RangeInfo, dml *ChunkDML) (bool, error) {
	rowsAdd, rowsDelete := 0, 0
	upstreamRowsIterator, err := df.upstream.GetRowsIterator(ctx, rangeInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err := df.downstream.GetRowsIterator(ctx, rangeInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData
	equal := true

	tableInfo := df.workSource.GetTables()[rangeInfo.GetTableIndex()].Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastUpstreamData == nil {
			// don't have source data, so all the targetRows's data is redundant, should be deleted
			for lastDownstreamData != nil {
				rowsDelete++

				if df.exportFixSQL {
					sql := df.downstream.GenerateFixSQL(
						source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex(),
					)
					log.Debug("[delete]", zap.String("sql", sql))

					dml.sqls = append(dml.sqls, sql)
				}

				equal = false
				lastDownstreamData, err = downstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				rowsAdd++
				if df.exportFixSQL {
					sql := df.downstream.GenerateFixSQL(source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
					log.Debug("[insert]", zap.String("sql", sql))

					dml.sqls = append(dml.sqls, sql)
				}
				equal = false

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
		if err != nil {
			return false, errors.Trace(err)
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		equal = false
		sql := ""

		switch cmp {
		case 1:
			// delete
			rowsDelete++
			if df.exportFixSQL {
				sql = df.downstream.GenerateFixSQL(
					source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex(),
				)
				log.Debug("[delete]", zap.String("sql", sql))
			}
			lastDownstreamData = nil
		case -1:
			// insert
			rowsAdd++
			if df.exportFixSQL {
				sql = df.downstream.GenerateFixSQL(
					source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex(),
				)
				log.Debug("[insert]", zap.String("sql", sql))
			}
			lastUpstreamData = nil
		case 0:
			// update
			rowsAdd++
			rowsDelete++
			if df.exportFixSQL {
				sql = df.downstream.GenerateFixSQL(
					source.Replace, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex(),
				)
				log.Debug("[update]", zap.String("sql", sql))
			}
			lastUpstreamData = nil
			lastDownstreamData = nil
		}

		dml.sqls = append(dml.sqls, sql)
	}
	dml.rowAdd = rowsAdd
	dml.rowDelete = rowsDelete

	log.Debug("compareRows",
		zap.Bool("equal", equal),
		zap.Int("rowsAdd", rowsAdd),
		zap.Int("rowsDelete", rowsDelete),
		zap.Any("chunk id", rangeInfo.ChunkRange.Index),
		zap.String("table", df.workSource.GetTables()[rangeInfo.GetTableIndex()].Table))
	return equal, nil
}

// WriteSQLs write sqls to file
func (df *Diff) writeSQLs(ctx context.Context) {
	log.Info("start writeSQLs goroutine")
	defer func() {
		log.Info("close writeSQLs goroutine")
		df.sqlWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case dml, ok := <-df.sqlCh:
			if !ok && dml == nil {
				log.Info("write sql channel closed")
				return
			}
			if len(dml.sqls) > 0 {
				tableDiff := df.downstream.GetTables()[dml.node.GetTableIndex()]
				fileName := fmt.Sprintf("%s:%s:%s.sql", tableDiff.Schema, tableDiff.Table, utils.GetSQLFileName(dml.node.GetID()))
				fixSQLPath := filepath.Join(df.FixSQLDir, fileName)
				if fileExists(fixSQLPath) {
					// unreachable
					log.Fatal("write sql failed: repeat sql happen", zap.Strings("sql", dml.sqls))
				}
				fixSQLFile, err := os.Create(fixSQLPath)
				if err != nil {
					log.Fatal("write sql failed: cannot create file", zap.Strings("sql", dml.sqls), zap.Error(err))
				}
				// write chunk meta
				chunkRange := dml.node.ChunkRange
				fixSQLFile.WriteString(fmt.Sprintf("-- table: %s.%s\n-- %s\n", tableDiff.Schema, tableDiff.Table, chunkRange.ToMeta()))
				if tableDiff.NeedUnifiedTimeZone {
					fixSQLFile.WriteString(fmt.Sprintf("set @@session.time_zone = \"%s\";\n", config.UnifiedTimeZone))
				}
				for _, sql := range dml.sqls {
					_, err = fixSQLFile.WriteString(fmt.Sprintf("%s\n", sql))
					if err != nil {
						log.Fatal("write sql failed", zap.String("sql", sql), zap.Error(err))
					}
				}
				fixSQLFile.Close()
			}
			log.Debug("insert node", zap.Any("chunk index", dml.node.GetID()))
			df.cp.Insert(dml.node)
		}
	}
}

func (df *Diff) removeSQLFiles(checkPointID *chunk.CID) error {
	ts := time.Now().Format("2006-01-02T15:04:05Z07:00")
	dirName := fmt.Sprintf(".trash-%s", ts)
	folderPath := filepath.Join(df.FixSQLDir, dirName)

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		err = os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			return errors.Trace(err)
		}
		defer os.RemoveAll(folderPath)
	}

	err := filepath.Walk(df.FixSQLDir, func(path string, f fs.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		name := f.Name()
		// in mac osx, the path parameter is absolute path; in linux, the path is relative path to execution base dir,
		// so use Rel to convert to relative path to l.base
		relPath, _ := filepath.Rel(df.FixSQLDir, path)
		oldPath := filepath.Join(df.FixSQLDir, relPath)
		newPath := filepath.Join(folderPath, relPath)
		if strings.Contains(oldPath, ".trash") {
			return nil
		}

		if strings.HasSuffix(name, ".sql") {
			fileIDStr := strings.TrimRight(name, ".sql")
			fileIDSubstrs := strings.SplitN(fileIDStr, ":", 3)
			if len(fileIDSubstrs) != 3 {
				return nil
			}
			tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, err := utils.GetCIDFromSQLFileName(fileIDSubstrs[2])
			if err != nil {
				return errors.Trace(err)
			}
			fileID := &chunk.CID{
				TableIndex: tableIndex, BucketIndexLeft: bucketIndexLeft, BucketIndexRight: bucketIndexRight, ChunkIndex: chunkIndex, ChunkCnt: 0,
			}
			if err != nil {
				return errors.Trace(err)
			}
			if fileID.Compare(checkPointID) > 0 {
				// move to trash
				err = os.Rename(oldPath, newPath)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = tidbconfig.DefMaxOfMaxIndexLength
	tidbconfig.StoreGlobalConfig(tidbCfg)

	log.Debug("set tidb cfg")
}
