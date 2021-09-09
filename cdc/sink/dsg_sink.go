// Copyright 2020 PingCAP, Inc.
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

package sink

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// newDsgTestSink creates a block hole sink
func newDsgTestSink(ctx context.Context, opts map[string]string) *dsgSocketSink {
	return &dsgSocketSink{
		statistics: NewStatistics(ctx, "dsgsocket", opts),
	}
}

type dsgSocketSink struct {
	statistics      *Statistics
	checkpointTs    uint64
	accumulated     uint64
	lastAccumulated uint64
}



func (b *dsgSocketSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {

	//var columnName = ""
	var msg = ""


	for _, row := range rows {
		msg = ""
		log.Debug("dsgSocketSink: EmitRowChangedEvents", zap.Any("row", row))

		schema := row.Table.Schema
		table := row.Table.Table

		log.Debug("schema::"+schema+"::table:"+table)

		for _, column := range row.Columns {
			columnName := column.Name
			columnValue := model.ColumnValueString(column.Value)
			//fmt.Println(value)

			//fmt.Fprintf(conn, value+"\n")
			msg += "columnName:"+columnName+":::columnValue:" +columnValue
		}

		conn, err := net.Dial("tcp", ":2300")
		if err != nil {
			log.Fatal("",zap.Any("err", err))
		}
		defer conn.Close()
		fmt.Fprintf(conn, msg+"\n")


		res, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Fatal("",zap.Any("err", err))
		}

		fmt.Println(string(res))
	}



	rowsCount := len(rows)
	atomic.AddUint64(&b.accumulated, uint64(rowsCount))
	b.statistics.AddRowsCount(rowsCount)
	return nil
}

func (b *dsgSocketSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	log.Debug("dsgSocketSink: FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs))
	err := b.statistics.RecordBatchExecution(func() (int, error) {
		// TODO: add some random replication latency
		accumulated := atomic.LoadUint64(&b.accumulated)
		batchSize := accumulated - b.lastAccumulated
		b.lastAccumulated = accumulated
		return int(batchSize), nil
	})
	b.statistics.PrintStatus(ctx)
	atomic.StoreUint64(&b.checkpointTs, resolvedTs)
	return resolvedTs, err
}

/*func analysisRowsAndSend(b *dsgSocketSink, ctx context.Context, singleTableTxn *model.SingleTableTxn) error {

	var eventTypeValue int32

	rowsMap, err := analysisRows(singleTableTxn)
	if err != nil {
		return errors.Trace(err)
	}
	for dmlType, rows := range rowsMap {
		if dmlType == "I" {
			eventTypeValue = 2
			if rows != nil {
				err := send(b, ctx, singleTableTxn, rows, eventTypeValue)
				if err != nil {
					return errors.Trace(err)
				}
			}
		} else if dmlType == "U" {
			if rows != nil {
				eventTypeValue = 3
				err := send(b, ctx, singleTableTxn, rows, eventTypeValue)
				if err != nil {
					return errors.Trace(err)
				}
			}
		} else if dmlType == "D" {
			if rows != nil {
				eventTypeValue = 4
				err := send(b, ctx, singleTableTxn, rows, eventTypeValue)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
	return nil
}*/

func (b *dsgSocketSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("dsgSocketSink: Checkpoint Event", zap.Uint64("ts", ts))
	return nil
}

func (b *dsgSocketSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Debug("dsgSocketSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

// Initialize is no-op for blackhole
func (b *dsgSocketSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (b *dsgSocketSink) Close(ctx context.Context) error {
	return nil
}

func (b *dsgSocketSink) Barrier(ctx context.Context) error {
	return nil
}
