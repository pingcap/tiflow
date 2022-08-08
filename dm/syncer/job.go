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

package syncer

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/util/filter"

	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

type opType byte

const (
	null opType = iota
	dml
	ddl
	xid
	flush
	asyncFlush
	skip // used by Syncer.recordSkipSQLsLocation to record global location, but not execute SQL
	rotate
	conflict
	compact
	gc // used to clean up out dated causality keys
)

func (t opType) String() string {
	switch t {
	case dml:
		return "dml"
	case ddl:
		return "ddl"
	case xid:
		return "xid"
	case flush:
		return "flush"
	case asyncFlush:
		return "asyncFlush"
	case skip:
		return "skip"
	case rotate:
		return "rotate"
	case conflict:
		return "conflict"
	case compact:
		return "compact"
	case gc:
		return "gc"
	}

	return ""
}

// job is the least unit of work for DMLWorker/DDL worker. It's generated by syncer's inner logic or binlog events. Specifically, most
// binlog events will generate one job except for DML events which will generate multiple jobs, one job for each row
// change.
type job struct {
	tp opType
	// ddl in ShardOptimistic and ShardPessimistic will only affect one table at one time but for normal node
	// we don't have this limit. So we should update multi tables in normal mode.
	// sql example: drop table `s1`.`t1`, `s2`.`t2`.
	sourceTbls      map[string][]*filter.Table
	targetTable     *filter.Table
	dml             *sqlmodel.RowChange
	dmlQueueKey     string
	safeMode        bool
	retry           bool
	location        binlog.Location // location of last received (ROTATE / QUERY / XID) event, for global/table checkpoint
	startLocation   binlog.Location // start location of the sql in binlog, for handle_error
	currentLocation binlog.Location // end location of the sql in binlog, for user to skip sql manually by changing checkpoint
	ddls            []string
	originSQL       string // show origin sql when error, only DDL now

	eventHeader *replication.EventHeader
	jobAddTime  time.Time       // job commit time
	flushSeq    int64           // sequence number for sync and async flush job
	flushWg     *sync.WaitGroup // wait group for sync, async and conflict job
	timestamp   uint32
	timezone    string
}

func (j *job) clone() *job {
	newJob := &job{}
	*newJob = *j
	return newJob
}

func (j *job) String() string {
	// only output some important information, maybe useful in execution.
	var dmlStr string
	if j.dml != nil {
		dmlStr = j.dml.String()
	}
	return fmt.Sprintf("tp: %s, flushSeq: %d, dml: [%s], safemode: %v, ddls: %s, last_location: %s, start_location: %s, current_location: %s", j.tp, j.flushSeq, dmlStr, j.safeMode, j.ddls, j.location, j.startLocation, j.currentLocation)
}

func newDMLJob(rowChange *sqlmodel.RowChange, ec *eventContext) *job {
	sourceTable := rowChange.GetSourceTable()
	targetTable := rowChange.GetTargetTable()
	// TODO: remove sourceTbls and targetTable for dml Job
	return &job{
		tp: dml,
		sourceTbls: map[string][]*filter.Table{
			sourceTable.Schema: {
				&filter.Table{Schema: sourceTable.Schema, Name: sourceTable.Table},
			},
		},
		targetTable: &filter.Table{Schema: targetTable.Schema, Name: targetTable.Table},
		dml:         rowChange,
		retry:       true,
		safeMode:    ec.safeMode,

		location:        *ec.lastLocation,
		startLocation:   *ec.startLocation,
		currentLocation: *ec.currentLocation,
		eventHeader:     ec.header,
		jobAddTime:      time.Now(),
	}
}

// newDDL job is used to create a new ddl job
// when cfg.ShardMode == ShardOptimistic || ShardPessimistic, len(qec.sourceTbls) == 0.
// when cfg.ShardMode == "", len(sourceTbls) != 0, we use sourceTbls to record ddl affected tables.
func newDDLJob(qec *queryEventContext) *job {
	j := &job{
		tp:          ddl,
		targetTable: &filter.Table{},
		ddls:        qec.needHandleDDLs,
		originSQL:   qec.originSQL,

		location:        *qec.lastLocation,
		startLocation:   *qec.startLocation,
		currentLocation: *qec.currentLocation,
		eventHeader:     qec.header,
		jobAddTime:      time.Now(),
	}

	ddlInfo := qec.shardingDDLInfo
	if len(qec.sourceTbls) != 0 {
		j.sourceTbls = make(map[string][]*filter.Table, len(qec.sourceTbls))
		for schema, tbMap := range qec.sourceTbls {
			if len(tbMap) > 0 {
				j.sourceTbls[schema] = make([]*filter.Table, 0, len(tbMap))
			}
			for name := range tbMap {
				j.sourceTbls[schema] = append(j.sourceTbls[schema], &filter.Table{Schema: schema, Name: name})
			}
		}
	} else if ddlInfo != nil && ddlInfo.sourceTables != nil && ddlInfo.targetTables != nil {
		j.sourceTbls = map[string][]*filter.Table{ddlInfo.sourceTables[0].Schema: {ddlInfo.sourceTables[0]}}
		j.targetTable = ddlInfo.targetTables[0]
	}

	j.timestamp = qec.timestamp
	j.timezone = qec.timezone

	return j
}

func newSkipJob(ec *eventContext) *job {
	return &job{
		tp:          skip,
		location:    *ec.lastLocation,
		eventHeader: ec.header,
		jobAddTime:  time.Now(),
	}
}

func newXIDJob(location, startLocation, currentLocation binlog.Location) *job {
	return &job{
		tp:              xid,
		location:        location,
		startLocation:   startLocation,
		currentLocation: currentLocation,
		jobAddTime:      time.Now(),
	}
}

func newFlushJob(workerCount int, seq int64) *job {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)

	return &job{
		tp:          flush,
		targetTable: &filter.Table{},
		jobAddTime:  time.Now(),
		flushWg:     wg,
		flushSeq:    seq,
	}
}

func newAsyncFlushJob(workerCount int, seq int64) *job {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)

	return &job{
		tp:          asyncFlush,
		targetTable: &filter.Table{},
		jobAddTime:  time.Now(),
		flushWg:     wg,
		flushSeq:    seq,
	}
}

func newGCJob(flushJobSeq int64) *job {
	return &job{
		tp:       gc,
		flushSeq: flushJobSeq,
	}
}

func newConflictJob(workerCount int) *job {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)

	return &job{
		tp:          conflict,
		targetTable: &filter.Table{},
		jobAddTime:  time.Now(),
		flushWg:     wg,
	}
}

// newCompactJob is only used for MetricsProxies.
func newCompactJob(targetTable *filter.Table) *job {
	return &job{
		tp:          compact,
		targetTable: targetTable,
	}
}

// put queues into bucket to monitor them.
func queueBucketName(queueID int) string {
	return fmt.Sprintf("q_%d", queueID%defaultBucketCount)
}

func dmlWorkerJobIdx(queueID int) int {
	return queueID + workerJobTSArrayInitSize
}
