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
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
)

type opType byte

const (
	null opType = iota
	insert
	update
	del
	ddl
	xid
	flush
	skip // used by Syncer.recordSkipSQLsLocation to record global location, but not execute SQL
	rotate
	conflict
	gc // used to clean up out dated causality keys
)

func (t opType) String() string {
	switch t {
	case insert:
		return "insert"
	case update:
		return "update"
	case del:
		return "delete"
	case ddl:
		return "ddl"
	case xid:
		return "xid"
	case flush:
		return "flush"
	case skip:
		return "skip"
	case rotate:
		return "rotate"
	case conflict:
		return "conflict"
	case gc:
		return "gc"
	}

	return ""
}

type job struct {
	tp opType
	// ddl in ShardOptimistic and ShardPessimistic will only affect one table at one time but for normal node
	// we don't have this limit. So we should update multi tables in normal mode.
	// sql example: drop table `s1`.`t1`, `s2`.`t2`.
	sourceTbls      map[string][]*filter.Table
	targetTable     *filter.Table
	dml             *DML
	retry           bool
	location        binlog.Location // location of last received (ROTATE / QUERY / XID) event, for global/table checkpoint
	startLocation   binlog.Location // start location of the sql in binlog, for handle_error
	currentLocation binlog.Location // end location of the sql in binlog, for user to skip sql manually by changing checkpoint
	ddls            []string
	originSQL       string // show origin sql when error, only DDL now

	eventHeader *replication.EventHeader
	jobAddTime  time.Time       // job commit time
	seq         int64           // sequence number for this job
	sync        bool            // whether the flush job is sync or async
	wg          *sync.WaitGroup // wait group for flush job
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
	return fmt.Sprintf("tp: %s, dml: %s, ddls: %s, last_location: %s, start_location: %s, current_location: %s", j.tp, dmlStr, j.ddls, j.location, j.startLocation, j.currentLocation)
}

func newDMLJob(tp opType, sourceTable, targetTable *filter.Table, dml *DML, ec *eventContext) *job {
	return &job{
		tp:          tp,
		sourceTbls:  map[string][]*filter.Table{sourceTable.Schema: {sourceTable}},
		targetTable: targetTable,
		dml:         dml,
		retry:       true,

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

func newFlushJob() *job {
	return &job{
		tp:          flush,
		targetTable: &filter.Table{},
		jobAddTime:  time.Now(),
		wg:          &sync.WaitGroup{},
	}
}

func newConflictJob() *job {
	return &job{
		tp:          conflict,
		targetTable: &filter.Table{},
		jobAddTime:  time.Now(),
	}
}

// put queues into bucket to monitor them.
func queueBucketName(queueID int) string {
	return fmt.Sprintf("q_%d", queueID%defaultBucketCount)
}

func dmlWorkerJobIdx(queueID int) int {
	return queueID + workerJobTSArrayInitSize
}
