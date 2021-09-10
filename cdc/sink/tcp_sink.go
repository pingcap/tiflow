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
	//"bufio"
	"context"
	"fmt"
	"github.com/pingcap/ticdc/cdc/sink/socket"
	"github.com/pingcap/ticdc/cdc/sink/vo"
	"github.com/pingcap/ticdc/cdc/sink/publicUtils"
	//"net"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// newDsgTestSink creates a block hole sink
func newDsgSink(ctx context.Context, opts map[string]string) *dsgSink {
	return &dsgSink{
		statistics: NewStatistics(ctx, "rowsocket", opts),
	}
}

type dsgSink struct {
	statistics      *Statistics
	checkpointTs    uint64
	accumulated     uint64
	lastAccumulated uint64
}

type RowJson struct {
	SchemaName    string
	TableName     string
	Columns       []*Column
}

//*每个字段的数据结构*
type Column struct {
	ColNo   *int32  `protobuf:"varint,1,opt,name=colNo" json:"colNo,omitempty"`
	ColType byte `protobuf:"bytes,2,opt,name=colType" json:"colType,omitempty"`
	//*字段名称(忽略大小写)，在mysql中是没有的*
	ColName *string `protobuf:"bytes,3,opt,name=colName" json:"colName,omitempty"`
	//* 字段标识 *
	ColFlags *int32 `protobuf:"varint,4,opt,name=colFlags" json:"colFlags,omitempty"`
	ColValue             interface{}  `protobuf:"bytes,6,opt,name=colValue" json:"colValue,omitempty"`

}

func (b *dsgSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {


	for _, row := range rows {
		log.Debug("dsgSocketSink: EmitRowChangedEvents", zap.Any("row", row))
	}

	fmt.Println(">>>>>>>>>>>>>>>>>>>>======================================================================================>>>>>>>>>>>>>>>>>>>")

	/*//读取配置文件
	configMap := publicUtils.InitConfig("configuration.txt")
	//获取配置里host属性的value
	fmt.Println(configMap["addr"])
	//查看配置文件里所有键值对
	fmt.Println(configMap)*/

	var eventTypeValue int32
	var schemaName string
	var tableName string

	if len(rows) == 0 {
		return nil
	} else {
		log.Info("PreColumns: ", zap.Any("", rows[0].PreColumns))
		log.Info("Columns: ", zap.Any("", rows[0].Columns))
		if len(rows[0].PreColumns) == 0 {
			//insert
			eventTypeValue = 2
		} else if len(rows[0].Columns) == 0 {
			//delete
			eventTypeValue = 4
		} else {
			//update
			eventTypeValue = 3
		}

		schemaName = rows[0].Table.Schema
		tableName = rows[0].Table.Table

	}

	for _, row := range rows {
		log.Info("show::::::::::::::::::::::::::::: row", zap.Any("row", row))
		rowInfos := make([]*vo.RowInfos, 0);

		rowdata := new(vo.RowInfos)

		columnInfos  :=make([]*vo.ColumnVo,0);
		//rowdata := &vo.RowInfos{}
		//rowdata := make([]*vo.RowInfos, 0);
		if eventTypeValue == 2 {
			//insert
			columnInfos = getColumnInfos(0, row.Columns)
		} else if eventTypeValue == 4 {
			//delete
			columnInfos = getColumnInfos(0, row.PreColumns)
		} else if eventTypeValue == 3 {
			//update
			//after
			columnInfos = getColumnInfos(0, row.Columns)
			//before
			columnInfos = getColumnInfos(1, row.PreColumns)

		}
		rowdata.StartTimer = int64(row.StartTs)
		rowdata.CommitTimer = int64(row.CommitTs)
		rowdata.ColumnNo = int32(len(columnInfos))
		rowdata.SchemaName = schemaName
		rowdata.TableName = tableName
		rowdata.OperType = eventTypeValue
		rowdata.ColumnList = columnInfos

		rowInfos = append(rowInfos,rowdata)
		fmt.Println("show RowInfos ：：：：：：：：：：：：：：", rowInfos)

		log.Info("show RowInfos ：：：：：：：：：：：：：：", zap.Reflect("rowdata", rowInfos))

		//send
		socket.JddmClient("10.0.0.72",19850,rowInfos)

	    //sender(rowdata)

	}


	rowsCount := len(rows)
	atomic.AddUint64(&b.accumulated, uint64(rowsCount))
	b.statistics.AddRowsCount(rowsCount)
	return nil
}


func getColumnInfos(colFlag int, columns []*model.Column) []*vo.ColumnVo {
	//rowdata := &vo.RowInfos{}
	columnInfos :=make([]*vo.ColumnVo,0);

	for _, column := range columns {
		columnVo := new(vo.ColumnVo)

		columnVo.ColumnName = column.Name
		columnVo.ColumnValue = model.ColumnValueString(column.Value)
		columnVo.IsPkFlag = column.Flag.IsPrimaryKey()
		columnVo.ColumnType = column.Type
		fmt.Println("column.Value:::::",column.Value)
		fmt.Println("IsPrimaryKey:::::",column.Flag.IsPrimaryKey())
		columnInfos = append(columnInfos,columnVo)

	}
	return columnInfos
}

func (b *dsgSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
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

func (b *dsgSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("dsgSocketSink: Checkpoint Event", zap.Uint64("ts", ts))
	return nil
}

func (b *dsgSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	log.Debug("dsgSocketSink: DDL Event", zap.Any("ddl", ddl))



	ddldata := new(vo.DDLInfos)

	//tableInfo
	columnInfos  :=make([]*vo.ColVo,0);
	columnInfos = getTableColumnInfos(0, ddl.TableInfo)
    ddldata.TableInfoList = columnInfos

	//pretableInfo
	PreColumnInfos  :=make([]*vo.ColVo,0);
	PreColumnInfos = getPreTableColumnInfos(0, ddl.PreTableInfo)
	ddldata.PreTableInfoList = PreColumnInfos
    //
    ddldata.StartTimer = int64(ddl.StartTs)
	ddldata.CommitTimer = int64(ddl.CommitTs)

	ddldata.SchemaName = ddl.TableInfo.Schema
	ddldata.TableName = ddl.TableInfo.Table

	ddldata.DDLType = int32(ddl.Type)
	ddldata.TableColumnNo = int32(len(columnInfos))
	ddldata.PreTableColumnNo = int32(len(PreColumnInfos))
	ddldata.QuerySql = ddl.Query
	//ddlInfos =ddldata

	fmt.Println("show ddlInfos ：：：：：：：：：：：：：：", ddldata)
	log.Info("show ddlInfos ：：：：：：：：：：：：：：", zap.Reflect("ddlInfos", ddldata))
	//send
	socket.JddmDDLClient("10.0.0.72",19850,ddldata)

	return nil
}

func getTableColumnInfos(colFlag int, tableInfo *model.SimpleTableInfo) []*vo.ColVo {

	//rowdata := &vo.RowInfos{}

	columnInfos :=make([]*vo.ColVo,0);

	for _, column := range tableInfo.ColumnInfo {
		columnVo := new(vo.ColVo)
        columnVo.ColumnName = column.Name
		columnVo.ColumnType = int(column.Type)
		fmt.Println("column.Value:::::",columnVo.ColumnName)
		columnInfos = append(columnInfos,columnVo)

	}
	return columnInfos
}

func getPreTableColumnInfos(colFlag int, preTableInfo *model.SimpleTableInfo) []*vo.ColVo {

	//rowdata := &vo.RowInfos{}

	columnInfos :=make([]*vo.ColVo,0);

	for _, column := range preTableInfo.ColumnInfo {
		columnVo := new(vo.ColVo)
		columnVo.ColumnName = column.Name
		columnVo.ColumnType = int(column.Type)
		fmt.Println("column.Value:::::",columnVo.ColumnName)
		columnInfos = append(columnInfos,columnVo)

	}
	return columnInfos
}

// Initialize is no-op for blackhole
func (b *dsgSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (b *dsgSink) Close(ctx context.Context) error {
	return nil
}

func (b *dsgSink) Barrier(ctx context.Context) error {
	return nil
}

func getRowData(colFlag int32, columns []*model.Column, json *RowJson) *RowJson {

	rowdata := &RowJson{}
	for _, column := range columns {

		columnBuilder := &Column{}
		columnBuilder.ColName = &column.Name
		//columnBuilder.ColValue = &column.Value
		columnBuilder.ColValue = model.ColumnValueString(column.Value)
		//columnBuilder.ColValue = model.ColumnValueString(column.Value, column.Flag)
		//columnBuilder.ColType = &column.Type
		fmt.Println("column.Value:::::",column.Value)
		fmt.Println("IsPrimaryKey:::::",column.Flag.IsPrimaryKey())
		columnBuilder.ColFlags = &colFlag
		//columnBuilder.ColType = &column.Type
		rowdata.Columns = append(rowdata.Columns,columnBuilder)
	}
	return rowdata
}
