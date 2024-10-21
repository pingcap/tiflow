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

package report

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

const (
	// Pass means all data and struct of tables are equal
	Pass = "pass"
	// Fail means not all data or struct of tables are equal
	Fail = "fail"
	// Error means we meet an error
	Error = "error"
)

// Config stores the config information for the user
type Config struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Snapshot string `toml:"snapshot,omitempty"`
	SQLMode  string `toml:"sql-mode,omitempty"`
}

// TableResult saves the check result for every table.
type TableResult struct {
	Schema      string                  `json:"schema"`
	Table       string                  `json:"table"`
	StructEqual bool                    `json:"struct-equal"`
	DataSkip    bool                    `json:"data-skip"`
	DataEqual   bool                    `json:"data-equal"`
	MeetError   error                   `json:"-"`
	ChunkMap    map[string]*ChunkResult `json:"chunk-result"` // `ChunkMap` stores the `ChunkResult` of each chunk of the table
	UpCount     int64                   `json:"up-count"`     // `UpCount` is the number of rows in the table from upstream
	DownCount   int64                   `json:"down-count"`   // `DownCount` is the number of rows in the table from downstream
	TableLack   int                     `json:"table-lack"`
}

// ChunkResult save the necessarily information to provide summary information
type ChunkResult struct {
	RowsAdd    int `json:"rows-add"`    // `RowsAdd` is the number of rows needed to add
	RowsDelete int `json:"rows-delete"` // `RowsDelete` is the number of rows needed to delete
}

// Report saves the check results.
type Report struct {
	sync.RWMutex
	Result       string                             `json:"-"`             // Result is pass or fail
	PassNum      int32                              `json:"-"`             // The pass number of tables
	FailedNum    int32                              `json:"-"`             // The failed number of tables
	SkippedNum   int32                              `json:"-"`             // The skipped number of tables
	TableResults map[string]map[string]*TableResult `json:"table-results"` // TableResult saved the map of  `schema` => `table` => `tableResult`
	StartTime    time.Time                          `json:"start-time"`
	Duration     time.Duration                      `json:"time-duration"`
	TotalSize    int64                              `json:"-"` // Total size of the checked tables
	SourceConfig [][]byte                           `json:"-"`
	TargetConfig []byte                             `json:"-"`

	task *config.TaskConfig `json:"-"`
}

// LoadReport loads the report from the checkpoint
func (r *Report) LoadReport(reportInfo *Report) {
	r.StartTime = time.Now()
	r.Duration = reportInfo.Duration
	r.TotalSize = reportInfo.TotalSize
	for schema, tableMap := range reportInfo.TableResults {
		if _, ok := r.TableResults[schema]; !ok {
			r.TableResults[schema] = make(map[string]*TableResult)
		}
		for table, result := range tableMap {
			r.TableResults[schema][table] = result
		}
	}
}

func (r *Report) getSortedTables() [][]string {
	equalTables := make([][]string, 0)
	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				equalRow := make([]string, 0, 3)
				equalRow = append(equalRow, dbutil.TableName(schema, table))
				equalRow = append(equalRow, strconv.FormatInt(result.UpCount, 10))
				equalRow = append(equalRow, strconv.FormatInt(result.DownCount, 10))
				equalTables = append(equalTables, equalRow)
			}
		}
	}
	sort.Slice(equalTables, func(i, j int) bool { return equalTables[i][0] < equalTables[j][0] })
	return equalTables
}

func (r *Report) getDiffRows() [][]string {
	diffRows := make([][]string, 0)
	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				continue
			}
			diffRow := make([]string, 0)
			diffRow = append(diffRow, dbutil.TableName(schema, table))
			if !common.AllTableExist(result.TableLack) {
				diffRow = append(diffRow, "skipped")
			} else {
				diffRow = append(diffRow, "succeed")
			}
			if !result.StructEqual {
				diffRow = append(diffRow, "false")
			} else {
				diffRow = append(diffRow, "true")
			}
			rowsAdd, rowsDelete := 0, 0
			for _, chunkResult := range result.ChunkMap {
				rowsAdd += chunkResult.RowsAdd
				rowsDelete += chunkResult.RowsDelete
			}
			diffRow = append(diffRow, fmt.Sprintf("+%d/-%d", rowsAdd, rowsDelete), strconv.FormatInt(result.UpCount, 10), strconv.FormatInt(result.DownCount, 10))
			diffRows = append(diffRows, diffRow)
		}
	}
	return diffRows
}

// CalculateTotalSize calculate the total size of all the checked tables
// Notice, user should run the analyze table first, when some of tables' size are zero.
func (r *Report) CalculateTotalSize(ctx context.Context, db *sql.DB) {
	for schema, tableMap := range r.TableResults {
		for table := range tableMap {
			size, err := utils.GetTableSize(ctx, db, schema, table)
			if size == 0 || err != nil {
				log.Warn("fail to get the correct size of table, if you want to get the correct size, please analyze the corresponding tables", zap.String("table", dbutil.TableName(schema, table)), zap.Error(err))
			} else {
				r.TotalSize += size
			}
		}
	}
}

// CommitSummary commit summary info
func (r *Report) CommitSummary() error {
	passNum, failedNum, skippedNum := int32(0), int32(0), int32(0)
	for _, tableMap := range r.TableResults {
		for _, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				passNum++
			} else if !common.AllTableExist(result.TableLack) {
				skippedNum++
			} else {
				failedNum++
			}
		}
	}
	r.PassNum = passNum
	r.FailedNum = failedNum
	r.SkippedNum = skippedNum
	summaryPath := filepath.Join(r.task.OutputDir, "summary.txt")
	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		return errors.Trace(err)
	}
	defer summaryFile.Close()
	summaryFile.WriteString("Summary\n\n\n\n")
	summaryFile.WriteString("Source Database\n\n\n\n")
	for i := 0; i < len(r.SourceConfig); i++ {
		summaryFile.Write(r.SourceConfig[i])
		summaryFile.WriteString("\n")
	}
	summaryFile.WriteString("Target Databases\n\n\n\n")
	summaryFile.Write(r.TargetConfig)
	summaryFile.WriteString("\n")

	summaryFile.WriteString("Comparison Result\n\n\n\n")
	summaryFile.WriteString("The table structure and data in following tables are equivalent\n\n")
	equalTables := r.getSortedTables()
	if len(equalTables) > 0 {
		tableString := &strings.Builder{}
		table := tablewriter.NewWriter(tableString)
		table.SetHeader([]string{"Table", "UpCount", "DownCount"})
		for _, v := range equalTables {
			table.Append(v)
		}
		table.Render()
		summaryFile.WriteString(tableString.String())
		summaryFile.WriteString("\n\n")
	}
	if r.Result == Fail || r.SkippedNum != 0 {
		summaryFile.WriteString("The following tables contains inconsistent data\n\n")
		tableString := &strings.Builder{}
		table := tablewriter.NewWriter(tableString)
		table.SetHeader([]string{"Table", "Result", "Structure equality", "Data diff rows", "UpCount", "DownCount"})
		diffRows := r.getDiffRows()
		for _, v := range diffRows {
			table.Append(v)
		}
		table.Render()
		summaryFile.WriteString(tableString.String())
	}
	duration := r.Duration + time.Since(r.StartTime)
	summaryFile.WriteString(fmt.Sprintf("\nTime Cost: %s\n", duration))
	summaryFile.WriteString(fmt.Sprintf("Average Speed: %fMB/s\n", float64(r.TotalSize)/(1024.0*1024.0*duration.Seconds())))
	return nil
}

// Print print the current report
func (r *Report) Print(w io.Writer) error {
	var summary strings.Builder
	if r.Result == Pass && r.SkippedNum == 0 {
		summary.WriteString(fmt.Sprintf("A total of %d table have been compared and all are equal.\n", r.FailedNum+r.PassNum+r.SkippedNum))
		summary.WriteString(fmt.Sprintf("You can view the comparison details through '%s/%s'\n", r.task.OutputDir, config.LogFileName))
	} else if r.Result == Fail || r.SkippedNum != 0 {
		for schema, tableMap := range r.TableResults {
			for table, result := range tableMap {
				if !result.StructEqual {
					if result.DataSkip {
						switch result.TableLack {
						case common.UpstreamTableLackFlag:
							summary.WriteString(fmt.Sprintf("The data of %s does not exist in upstream database\n", dbutil.TableName(schema, table)))
						case common.DownstreamTableLackFlag:
							summary.WriteString(fmt.Sprintf("The data of %s does not exist in downstream database\n", dbutil.TableName(schema, table)))
						default:
							summary.WriteString(fmt.Sprintf("The structure of %s is not equal, and data-check is skipped\n", dbutil.TableName(schema, table)))
						}
					} else {
						summary.WriteString(fmt.Sprintf("The structure of %s is not equal\n", dbutil.TableName(schema, table)))
					}
				}
				if !result.DataEqual && common.AllTableExist(result.TableLack) {
					summary.WriteString(fmt.Sprintf("The data of %s is not equal\n", dbutil.TableName(schema, table)))
				}
			}
		}
		summary.WriteString("\n")
		summary.WriteString("The rest of tables are all equal.\n")
		summary.WriteString("\n")
		summary.WriteString(fmt.Sprintf("A total of %d tables have been compared, %d tables finished, %d tables failed, %d tables skipped.\n", r.FailedNum+r.PassNum+r.SkippedNum, r.PassNum, r.FailedNum, r.SkippedNum))
		summary.WriteString(fmt.Sprintf("The patch file has been generated in \n\t'%s/'\n", r.task.FixDir))
		summary.WriteString(fmt.Sprintf("You can view the comparison details through '%s/%s'\n", r.task.OutputDir, config.LogFileName))
	} else {
		summary.WriteString("Error in comparison process:\n")
		for schema, tableMap := range r.TableResults {
			for table, result := range tableMap {
				if result.MeetError != nil {
					summary.WriteString(fmt.Sprintf("%s error occurred in %s\n", result.MeetError.Error(), dbutil.TableName(schema, table)))
				}
			}
		}
		summary.WriteString(fmt.Sprintf("You can view the comparison details through '%s/%s'\n", r.task.OutputDir, config.LogFileName))
	}
	fmt.Fprint(w, summary.String())
	return nil
}

// NewReport returns a new Report.
func NewReport(task *config.TaskConfig) *Report {
	return &Report{
		TableResults: make(map[string]map[string]*TableResult),
		Result:       Pass,
		task:         task,
	}
}

// Init initialize the report
func (r *Report) Init(tableDiffs []*common.TableDiff, sourceConfig [][]byte, targetConfig []byte) {
	r.StartTime = time.Now()
	r.SourceConfig = sourceConfig
	r.TargetConfig = targetConfig
	for _, tableDiff := range tableDiffs {
		schema, table := tableDiff.Schema, tableDiff.Table
		if _, ok := r.TableResults[schema]; !ok {
			r.TableResults[schema] = make(map[string]*TableResult)
		}
		r.TableResults[schema][table] = &TableResult{
			Schema:      schema,
			Table:       table,
			StructEqual: true,
			DataEqual:   true,
			MeetError:   nil,
			ChunkMap:    make(map[string]*ChunkResult),
		}
	}
}

// SetTableStructCheckResult sets the struct check result for table.
func (r *Report) SetTableStructCheckResult(schema, table string, equal bool, skip bool, exist int) {
	r.Lock()
	defer r.Unlock()
	tableResult := r.TableResults[schema][table]
	tableResult.StructEqual = equal
	tableResult.DataSkip = skip
	tableResult.TableLack = exist
	if !equal && common.AllTableExist(tableResult.TableLack) && r.Result != Error {
		r.Result = Fail
	}
}

// SetTableDataCheckResult sets the data check result for table.
func (r *Report) SetTableDataCheckResult(schema, table string, equal bool, rowsAdd, rowsDelete int, upCount, downCount int64, id *chunk.CID) {
	r.Lock()
	defer r.Unlock()
	result := r.TableResults[schema][table]
	result.UpCount += upCount
	result.DownCount += downCount
	if !equal {
		result.DataEqual = equal
		if _, ok := result.ChunkMap[id.ToString()]; !ok {
			result.ChunkMap[id.ToString()] = &ChunkResult{
				RowsAdd:    0,
				RowsDelete: 0,
			}
		}
		result.ChunkMap[id.ToString()].RowsAdd += rowsAdd
		result.ChunkMap[id.ToString()].RowsDelete += rowsDelete
		if r.Result != Error && common.AllTableExist(result.TableLack) {
			r.Result = Fail
		}
	}
	if !equal && common.AllTableExist(result.TableLack) && r.Result != Error {
		r.Result = Fail
	}
}

// SetTableMeetError sets meet error when check the table.
func (r *Report) SetTableMeetError(schema, table string, err error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.TableResults[schema]; !ok {
		r.TableResults[schema] = make(map[string]*TableResult)
		r.TableResults[schema][table] = &TableResult{
			MeetError: err,
		}
		return
	}

	r.TableResults[schema][table].MeetError = err
	r.Result = Error
}

// GetSnapshot get the snapshot of the current state of the report, then we can restart the
// sync-diff and get the correct report state.
func (r *Report) GetSnapshot(chunkID *chunk.CID, schema, table string) (*Report, error) {
	r.RLock()
	defer r.RUnlock()
	targetID := utils.UniqueID(schema, table)
	reserveMap := make(map[string]map[string]*TableResult)
	for schema, tableMap := range r.TableResults {
		reserveMap[schema] = make(map[string]*TableResult)
		for table, result := range tableMap {
			reportID := utils.UniqueID(schema, table)
			if reportID >= targetID {
				chunkRes := make(map[string]*ChunkResult)
				reserveMap[schema][table] = &TableResult{
					Schema:      result.Schema,
					Table:       result.Table,
					StructEqual: result.StructEqual,
					DataEqual:   result.DataEqual,
					MeetError:   result.MeetError,
				}
				for id, chunkResult := range result.ChunkMap {
					sid := new(chunk.CID)
					err := sid.FromString(id)
					if err != nil {
						return nil, errors.Trace(err)
					}
					if sid.Compare(chunkID) <= 0 {
						chunkRes[id] = chunkResult
					}
				}
				reserveMap[schema][table].ChunkMap = chunkRes
			}
		}
	}

	result := r.Result
	totalSize := r.TotalSize
	duration := time.Since(r.StartTime)
	task := r.task
	return &Report{
		PassNum:      0,
		FailedNum:    0,
		Result:       result,
		TableResults: reserveMap,
		StartTime:    r.StartTime,
		Duration:     duration,
		TotalSize:    totalSize,

		task: task,
	}, nil
}
