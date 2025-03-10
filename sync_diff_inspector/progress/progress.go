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

package progress

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
)

type atomicWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (aw *atomicWriter) Set(writer io.Writer) {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.writer = writer
}

func (aw *atomicWriter) Write(s string, args ...any) {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	fmt.Fprintf(aw.writer, s, args...)
}

func (aw *atomicWriter) Get() io.Writer {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	return aw.writer
}

type tableProgressPrinter struct {
	tableList     *list.List
	tableFailList *list.List
	tableMap      map[string]*list.Element
	output        atomicWriter
	lines         int

	progressTableNums int
	finishTableNums   int
	tableNums         int

	progress int
	total    int

	optCh    chan operator
	finishCh chan struct{}
}

type tableState int

const (
	tableStateRegister                    tableState = 0x1
	tableStatePrestart                    tableState = 0x2
	tableStateComparing                   tableState = 0x4
	tableStateFinish                      tableState = 0x8
	tableStateResultOK                    tableState = 0x00
	tableStateResultFailStructureDone     tableState = 0x10
	tableStateResultFailStructureContinue tableState = 0x20
	tableStateResultFailStructurePass     tableState = 0x40
	tableStateResultDifferent             tableState = 0x80
	tableStateHead                        tableState = 0xff
	tableStateResultMask                  tableState = 0xff0
	tableStateNotExistUpstream            tableState = 0x100
	tableStateNotExistDownstream          tableState = 0x200
)

// TableProgress store the progress of one table
type TableProgress struct {
	name            string
	progress        int
	total           int
	state           tableState
	totalStopUpdate bool
}

type progressOpt int

const (
	progressOptInc progressOpt = iota
	progressOptUpdate
	progressOptRegister
	progressOptStart
	progressOptFail
	progressOptClose
	progressOptError
)

type operator struct {
	optType         progressOpt
	name            string
	total           int
	state           tableState
	totalStopUpdate bool
}

func newTableProgressPrinter(tableNums int, finishTableNums int) *tableProgressPrinter {
	tpp := &tableProgressPrinter{
		tableList:     list.New(),
		tableFailList: list.New(),
		tableMap:      make(map[string]*list.Element),
		lines:         0,

		progressTableNums: 0,
		finishTableNums:   finishTableNums,
		tableNums:         tableNums,

		progress: 0,
		total:    0,

		optCh:    make(chan operator, 16),
		finishCh: make(chan struct{}),
	}
	tpp.init()
	go tpp.serve()
	tpp.output.Write("A total of %d tables need to be compared\n\n\n", tableNums)
	return tpp
}

func (tpp *tableProgressPrinter) SetOutput(output io.Writer) {
	tpp.output.Set(output)
}

func (tpp *tableProgressPrinter) Inc(name string) {
	tpp.optCh <- operator{
		optType: progressOptInc,
		name:    name,
	}
}

func (tpp *tableProgressPrinter) UpdateTotal(name string, total int, stopUpdate bool) {
	tpp.optCh <- operator{
		optType:         progressOptUpdate,
		name:            name,
		total:           total,
		totalStopUpdate: stopUpdate,
	}
}

func (tpp *tableProgressPrinter) RegisterTable(name string, isFailed bool, isDone bool, isExist int) {
	var state tableState
	if isFailed {
		if isDone {
			switch isExist {
			case common.UpstreamTableLackFlag:
				state = tableStateNotExistUpstream | tableStateRegister
			case common.DownstreamTableLackFlag:
				state = tableStateNotExistDownstream | tableStateRegister
			default:
				state = tableStateResultFailStructureDone | tableStateRegister
			}
		} else {
			state = tableStateResultFailStructureContinue | tableStateRegister
		}
	} else {
		state = tableStateRegister
	}
	tpp.optCh <- operator{
		optType: progressOptRegister,
		name:    name,
		state:   state,
	}
}

func (tpp *tableProgressPrinter) StartTable(name string, total int, stopUpdate bool) {
	tpp.optCh <- operator{
		optType:         progressOptStart,
		name:            name,
		total:           total,
		state:           tableStatePrestart,
		totalStopUpdate: stopUpdate,
	}
}

func (tpp *tableProgressPrinter) FailTable(name string) {
	tpp.optCh <- operator{
		optType: progressOptFail,
		name:    name,
		state:   tableStateResultDifferent,
	}
}

func (tpp *tableProgressPrinter) Close() {
	tpp.optCh <- operator{
		optType: progressOptClose,
	}
	<-tpp.finishCh
}

func (tpp *tableProgressPrinter) PrintSummary() {
	var cleanStr, fixStr string
	cleanStr = "\x1b[1A\x1b[J"
	fixStr = "\nSummary:\n\n"
	if tpp.tableFailList.Len() == 0 {
		fixStr = fmt.Sprintf(
			"%sA total of %d tables have been compared and all are equal.\nYou can view the comparison details through './output_dir/sync_diff_inspector.log'\n",
			fixStr,
			tpp.tableNums,
		)
	} else {
		SkippedNum := 0
		for p := tpp.tableFailList.Front(); p != nil; p = p.Next() {
			tp := p.Value.(*TableProgress)
			if tp.state&(tableStateResultFailStructureDone|tableStateResultFailStructureContinue) != 0 {
				fixStr = fmt.Sprintf("%sThe structure of %s is not equal.\n", fixStr, tp.name)
			}
			if tp.state&(tableStateResultDifferent) != 0 {
				fixStr = fmt.Sprintf("%sThe data of %s is not equal.\n", fixStr, tp.name)
			}
			if tp.state&(tableStateNotExistDownstream) != 0 {
				fixStr = fmt.Sprintf("%sThe data of %s does not exist in downstream database.\n", fixStr, tp.name)
				SkippedNum++
			}
			if tp.state&(tableStateNotExistUpstream) != 0 {
				fixStr = fmt.Sprintf("%sThe data of %s does not exist in upstream database.\n", fixStr, tp.name)
				SkippedNum++
			}
		}
		fixStr = fmt.Sprintf(
			"%s\nThe rest of the tables are all equal.\nA total of %d tables have been compared, %d tables finished, %d tables failed, %d tables skipped.\nThe patch file has been generated to './output_dir/patch.sql'\nYou can view the comparison details through './output_dir/sync_diff_inspector.log'\n",
			fixStr, tpp.tableNums, tpp.tableNums-tpp.tableFailList.Len(), tpp.tableFailList.Len()-SkippedNum, SkippedNum,
		)
	}

	tpp.output.Write("%s%s\n", cleanStr, fixStr)
}

func (tpp *tableProgressPrinter) Error(err error) {
	tpp.optCh <- operator{
		optType: progressOptError,
	}
	<-tpp.finishCh
	var cleanStr, fixStr string
	cleanStr = "\x1b[1A\x1b[J"
	fixStr = fmt.Sprintf("\nError in comparison process:\n%v\n\nYou can view the comparison details through './output_dir/sync_diff_inspector.log'\n", err)
	tpp.output.Write("%s%s", cleanStr, fixStr)
}

func (tpp *tableProgressPrinter) init() {
	tpp.tableList.PushBack(&TableProgress{
		state: tableStateHead,
	})

	tpp.output.Set(os.Stdout)
}

func (tpp *tableProgressPrinter) serve() {
	tick := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-tick.C:
			tpp.flush(false)
		case opt := <-tpp.optCh:
			switch opt.optType {
			case progressOptClose:
				tpp.flush(false)
				tpp.finishCh <- struct{}{}
				return
			case progressOptError:
				tpp.finishCh <- struct{}{}
				return
			case progressOptInc:
				if e, ok := tpp.tableMap[opt.name]; ok {
					tp := e.Value.(*TableProgress)
					tp.progress++
					tpp.progress++
					if tp.progress >= tp.total && tp.totalStopUpdate {
						tp.state = (tp.state & tableStateResultMask) | tableStateFinish
						tpp.progress -= tp.progress
						tpp.total -= tp.total
						delete(tpp.tableMap, opt.name)
						tpp.flush(true)
					}
				}
			case progressOptRegister:
				if _, ok := tpp.tableMap[opt.name]; !ok {
					e := tpp.tableList.PushBack(&TableProgress{
						name:            opt.name,
						progress:        0,
						total:           opt.total,
						state:           opt.state,
						totalStopUpdate: opt.totalStopUpdate,
					})
					tpp.tableMap[opt.name] = e
				}
			case progressOptStart:
				e, ok := tpp.tableMap[opt.name]
				if !ok {
					e = tpp.tableList.PushBack(&TableProgress{
						name:            opt.name,
						progress:        0,
						total:           opt.total,
						state:           opt.state | tableStateResultFailStructurePass,
						totalStopUpdate: opt.totalStopUpdate,
					})
					tpp.tableMap[opt.name] = e
				} else {
					tp := e.Value.(*TableProgress)
					tp.state ^= tableStateRegister | opt.state
					tp.progress = 0
					tp.total = opt.total
					tp.totalStopUpdate = opt.totalStopUpdate
				}
				if e.Value.(*TableProgress).state&tableStateResultFailStructureDone == 0 {
					tpp.total += opt.total
				} else {
					delete(tpp.tableMap, opt.name)
				}
				tpp.flush(true)
			case progressOptUpdate:
				if e, ok := tpp.tableMap[opt.name]; ok {
					tp := e.Value.(*TableProgress)
					tpp.total += opt.total
					tp.total += opt.total
					tp.totalStopUpdate = opt.totalStopUpdate
				}
			case progressOptFail:
				if e, ok := tpp.tableMap[opt.name]; ok {
					tp := e.Value.(*TableProgress)
					tp.state |= opt.state
					// continue to increment chunk
				}
			}
		}
	}
}

// flush info
func (tpp *tableProgressPrinter) flush(stateIsChanged bool) {
	/*
	 * A total of 15 tables need to be compared
	 *
	 * Comparing the table structure of `schema1.table1` ... equivalent
	 * Comparing the table data of `schema1.table1` ... equivalent
	 * Comparing the table structure of `schema2.table2` ... equivalent
	 * Comparing the table data of `schema2.table2` ...
	 * _____________________________________________________________________________
	 * Progress [===================>-----------------------------------------] 35%
	 *
	 */

	if stateIsChanged {
		var cleanStr, fixStr, dynStr string
		cleanStr = fmt.Sprintf("\x1b[%dA\x1b[J", tpp.lines)
		tpp.lines = 2
		/* PRESTART/COMPARING/FINISH OK/DIFFERENT */
		for p := tpp.tableList.Front(); p != nil; p = p.Next() {
			tp := p.Value.(*TableProgress)
			// There are 5 situations:
			// 1. structure is same and data is same
			// 2. structure is same and data is different
			// 3. structure is different and we won't compare data
			// 4. structure is different and data is same
			// 5. structure is different and data is different
			switch tp.state & 0xf {
			case tableStatePrestart:
				switch tp.state & tableStateResultMask {
				case tableStateResultOK:
					fixStr = fmt.Sprintf("%sComparing the table structure of %s ... equivalent\n", fixStr, tp.name)
					dynStr = fmt.Sprintf("%sComparing the table data of %s ...\n", dynStr, tp.name)
					tpp.lines++
					tpp.progressTableNums++
					tp.state = tableStateComparing
				case tableStateNotExistUpstream, tableStateNotExistDownstream:
					dynStr = fmt.Sprintf("%sComparing the table data of %s ...skipped\n", dynStr, tp.name)
					tpp.tableFailList.PushBack(tp)
					preNode := p.Prev()
					tpp.tableList.Remove(p)
					p = preNode
					tpp.finishTableNums++
				case tableStateResultFailStructureDone:
					fixStr = fmt.Sprintf("%sComparing the table structure of %s ... failure\n", fixStr, tp.name)
					tpp.tableFailList.PushBack(tp)
					// we have empty node as list head, so p is not nil
					preNode := p.Prev()
					tpp.tableList.Remove(p)
					p = preNode
					tpp.finishTableNums++
				case tableStateResultFailStructureContinue:
					fixStr = fmt.Sprintf("%sComparing the table structure of %s ... failure\n", fixStr, tp.name)
					dynStr = fmt.Sprintf("%sComparing the table data of %s ...\n", dynStr, tp.name)
					tpp.lines++
					tpp.progressTableNums++
					tp.state ^= tableStateComparing | tableStatePrestart
				case tableStateResultFailStructurePass:
					fixStr = fmt.Sprintf("%sComparing the table structure of %s ... skip\n", fixStr, tp.name)
					dynStr = fmt.Sprintf("%sComparing the table data of %s ...\n", dynStr, tp.name)
					tpp.lines++
					tpp.progressTableNums++
					tp.state ^= tableStateComparing | tableStatePrestart
				}
			case tableStateComparing:
				dynStr = fmt.Sprintf("%sComparing the table data of %s ...\n", dynStr, tp.name)
				tpp.lines++
			case tableStateFinish:
				if tp.state&tableStateResultDifferent == 0 {
					fixStr = fmt.Sprintf("%sComparing the table data of %s ... equivalent\n", fixStr, tp.name)
				} else {
					fixStr = fmt.Sprintf("%sComparing the table data of %s ... failure\n", fixStr, tp.name)
				}
				if tp.state&tableStateResultMask != 0 {
					tpp.tableFailList.PushBack(tp)
				}
				// we have empty node as list head, so p is not nil
				preNode := p.Prev()
				tpp.tableList.Remove(p)
				p = preNode
				tpp.progressTableNums--
				tpp.finishTableNums++
			}
		}

		dynStr = fmt.Sprintf("%s_____________________________________________________________________________\n", dynStr)
		tpp.output.Write("%s%s%s", cleanStr, fixStr, dynStr)
	} else {
		tpp.output.Write("\x1b[1A\x1b[J")
	}
	// show bar
	// 60 '='+'-'
	coe := float32(tpp.progressTableNums*tpp.progress)/float32(tpp.tableNums*(tpp.total+1)) + float32(tpp.finishTableNums)/float32(tpp.tableNums)
	numLeft := int(60 * coe)
	percent := int(100 * coe)
	tpp.output.Write("Progress [%s>%s] %d%% %d/%d\n", strings.Repeat("=", numLeft), strings.Repeat("-", 60-numLeft), percent, tpp.progress, tpp.total)
}

var progress *tableProgressPrinter = nil

// Init initialize the printer
func Init(tableNums, finishTableNums int) {
	progress = newTableProgressPrinter(tableNums, finishTableNums)
}

// Inc update the progress of one table
func Inc(name string) {
	if progress != nil {
		progress.Inc(name)
	}
}

// UpdateTotal the total for given table
func UpdateTotal(name string, total int, stopUpdate bool) {
	if progress != nil {
		progress.UpdateTotal(name, total, stopUpdate)
	}
}

// RegisterTable register a new table
func RegisterTable(name string, isFailed bool, isDone bool, isExist int) {
	if progress != nil {
		progress.RegisterTable(name, isFailed, isDone, isExist)
	}
}

// StartTable start a table
func StartTable(name string, total int, stopUpdate bool) {
	if progress != nil {
		progress.StartTable(name, total, stopUpdate)
	}
}

// FailTable stop a table
func FailTable(name string) {
	if progress != nil {
		progress.FailTable(name)
	}
}

// Close close the progress printer
func Close() {
	if progress != nil {
		progress.Close()
	}
}

// PrintSummary print the summary
func PrintSummary() {
	if progress != nil {
		progress.PrintSummary()
	}
}

// Error pass the error into progress printer
func Error(err error) {
	if progress != nil {
		progress.Error(err)
	}
}

// SetOutput set the output for progress printer
func SetOutput(output io.Writer) {
	if progress != nil {
		progress.SetOutput(output)
	}
}
