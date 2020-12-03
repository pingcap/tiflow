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
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/filter"
	tifilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/retry"
	dsgpb "github.com/pingcap/ticdc/proto/dsg"
	"google.golang.org/grpc"
	gbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

const (
	grpcInitialWindowSize     = 1 << 30 // The value for initial window size on a stream
	grpcInitialConnWindowSize = 1 << 30 // The value for initial window size on a connection
	grpcMaxCallRecvMsgSize    = 1 << 30 // The maximum message size the client can receive
)

// newBlackHoleSink creates a block hole sink
func newTicdcToOraclSink(ctx context.Context, sinkURI *url.URL, filter *tifilter.Filter, opts map[string]string) *ticdcToOraclSink {

	//address := "192.168.198.48:9099"
	conn, err := grpc.Dial(
		sinkURI.Host,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: gbackoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		fmt.Print(err)
		return nil
	}
	client := dsgpb.NewDSGTiCDCStreamingClient(conn)
	grpcCtx := context.Background()
	request, err := client.DSGTiCDCStreamingRequest(grpcCtx)
	if err != nil {
		fmt.Print(err)
		return nil
	}
	return &ticdcToOraclSink{
		statistics:    NewStatistics(ctx, "blackhole", opts),
		clientConn:    conn,
		clientRequest: request,
		filter:        filter,
		txnCache:      common.NewUnresolvedTxnCache(),
	}
}

type ticdcToOraclSink struct {
	statistics      *Statistics
	checkpointTs    uint64
	accumulated     uint64
	lastAccumulated uint64
	filter          *filter.Filter
	txnCache        *common.UnresolvedTxnCache
	clientConn      *grpc.ClientConn
	clientRequest   dsgpb.DSGTiCDCStreaming_DSGTiCDCStreamingRequestClient
}

var curCookies []*model.RowChangedEvent

func (b *ticdcToOraclSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {

	count := b.txnCache.Append(b.filter, rows...)
	b.statistics.AddRowsCount(count)

	var eventTypeValue int32
	var schemaName string
	var tableName string
	var batchID string
	//var colType string

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
		//事务号
		batchID = strconv.FormatUint(rows[0].StartTs, 20)
	}
	checkpointTs := atomic.LoadUint64(&b.checkpointTs)

	entryBuilder := &dsgpb.Entry{}
	headerBuilder := &dsgpb.Header{}
	rowdataListBuilder := &dsgpb.RowDataList{}

	for _, row := range rows {
		var rowdataBuilder dsgpb.RowData
		if eventTypeValue == 2 {
			//insert
			rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
		} else if eventTypeValue == 4 {
			//delete
			rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
		} else if eventTypeValue == 3 {
			//update
			//after
			rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
			//before
			rowdataBuilder = getRowDataByClomns(row.PreColumns, rowdataBuilder)
		}

		if row.CommitTs <= checkpointTs {
			log.Fatal("The CommitTs must be greater than the checkpointTs",
				zap.Uint64("CommitTs", row.CommitTs),
				zap.Uint64("checkpointTs", checkpointTs))
		}
		log.Info("BlockHoleSink: EmitRowChangedEvents", zap.Any("row", row))

		log.Info("show rowdataBuilder ", zap.Reflect("e", rowdataBuilder))
		rowdataListBuilder.RowDatas = append(rowdataListBuilder.RowDatas, &rowdataBuilder)
		log.Info("rowdataList size ", zap.Reflect("size :", len(rowdataListBuilder.RowDatas)))

	}

	headerBuilder.SchemaName = &schemaName
	headerBuilder.TableName = &tableName
	eventType := dsgpb.EventType(eventTypeValue)
	headerBuilder.EventType = &eventType
	entryBuilder.Header = headerBuilder

	entryBuilder.BatchID = &batchID
	var batchCountNo = int32(len(rows))
	entryBuilder.BatchCountNo = &batchCountNo

	var entry = dsgpb.EntryType(dsgpb.EntryType_ROWDATALIST)
	entryBuilder.EntryType = &entry
	log.Info("show rowdataListBuilder ", zap.Reflect("e", rowdataListBuilder))
	rowdataListBuilderBytes, err := rowdataListBuilder.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	entryBuilder.StoreValue = rowdataListBuilderBytes

	log.Info("show entryBuilder ", zap.Reflect("e", entryBuilder))
	err = clintSendDataWithRetry(b, entryBuilder)
	/*err = b.clientRequest.Send(entryBuilder)
	if err != nil {
		log.Warn("the connection to dsg server is broken")
		_, err2 := b.clientRequest.CloseAndRecv()
		if err2 != nil {
			log.Warn("error when close the connection", zap.Error(err2))
		}
		client := DSGEntryProtocol.NewDsgTicdcStreamingClient(b.clientConn)
		grpcCtx := context.Background()
		request, err := client.DsgTicdcStreamingRequest(grpcCtx)
		if err != nil {
			return errors.Trace(err)
		}
		b.clientRequest = request
		err = b.clientRequest.Send(entryBuilder)
		if err != nil {
			return errors.Trace(err)
		}
	}*/
	log.Info("send data success!")

	return nil
}

func (b *ticdcToOraclSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {

	var eventTypeValue int32
	var schemaName string
	var tableName string
	var batchID string

	atomic.StoreUint64(&b.checkpointTs, resolvedTs)
	resolvedTxnsMap := b.txnCache.Resolved(resolvedTs)

	if len(resolvedTxnsMap) != 0 {
		//循环多张表
		for _, singleTableTxns := range resolvedTxnsMap {

			//循环多个事务
			for _, singleTableTxn := range singleTableTxns {
				//todo
				rows := singleTableTxn.Rows
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
				//事务号
				batchID = strconv.FormatUint(rows[0].StartTs, 20)

				entryBuilder := &dsgpb.Entry{}
				headerBuilder := &dsgpb.Header{}
				rowdataListBuilder := &dsgpb.RowDataList{}

				for _, row := range rows {
					var rowdataBuilder dsgpb.RowData
					if eventTypeValue == 2 {
						//insert
						rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
					} else if eventTypeValue == 4 {
						//delete
						rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
					} else if eventTypeValue == 3 {
						//update
						//after
						rowdataBuilder = getRowDataByClomns(row.Columns, rowdataBuilder)
						//before
						rowdataBuilder = getRowDataByClomns(row.PreColumns, rowdataBuilder)
					}

					log.Info("BlockHoleSink: EmitRowChangedEvents", zap.Any("row", row))

					log.Info("show rowdataBuilder ", zap.Reflect("e", rowdataBuilder))
					rowdataListBuilder.RowDatas = append(rowdataListBuilder.RowDatas, &rowdataBuilder)
					log.Info("rowdataList size ", zap.Reflect("size :", len(rowdataListBuilder.RowDatas)))
				}

				headerBuilder.SchemaName = &schemaName
				headerBuilder.TableName = &tableName
				eventType := dsgpb.EventType(eventTypeValue)
				headerBuilder.EventType = &eventType
				entryBuilder.Header = headerBuilder

				entryBuilder.BatchID = &batchID
				var batchCountNo = int32(len(rows))
				entryBuilder.BatchCountNo = &batchCountNo

				var entry = dsgpb.EntryType(dsgpb.EntryType_ROWDATALIST)
				entryBuilder.EntryType = &entry
				log.Info("show rowdataListBuilder ", zap.Reflect("e", rowdataListBuilder))
				rowdataListBuilderBytes, err := rowdataListBuilder.Marshal()
				if err != nil {
					return resolvedTs, errors.Trace(err)
				}
				entryBuilder.StoreValue = rowdataListBuilderBytes

				log.Info("show entryBuilder ", zap.Reflect("e", entryBuilder))
				err = clintSendDataWithRetry(b, entryBuilder)
				log.Info("send data success!")

			}
		}
	}
	return resolvedTs, nil
}

func (b *ticdcToOraclSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("BlockHoleSink: Checkpoint Event", zap.Uint64("ts", ts))
	return nil
}

func (b *ticdcToOraclSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {

	log.Debug("BlockHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

// Initialize is no-op for blackhole
func (b *ticdcToOraclSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (b *ticdcToOraclSink) Close() error {
	return nil
}

func getRowDataByClomns(colums []*model.Column, rowdataBuilder dsgpb.RowData) dsgpb.RowData {

	var colType string

	for _, column := range colums {
		columnBuilder := &dsgpb.Column{}
		columnBuilder.ColName = &column.Name
		columnValue := model.ColumnValueString(column.Value)
		columnBuilder.ColValue = &columnValue
		if column.Type == 1 || column.Type == 2 || column.Type == 3 || column.Type == 4 || column.Type == 5 || column.Type == 8 || column.Type == 9 {
			colType = "integer"
		} else if column.Type == 15 || column.Type == 253 || column.Type == 245 || column.Type == 254 {
			colType = "string"
		} else if column.Type == 10 || column.Type == 14 {
			colType = "date"
		} else if column.Type == 7 || column.Type == 12 {
			colType = "datetime"
		} else if column.Type == 11 {
			colType = "time"
		} else if column.Type == 13 {
			colType = "year"
		}
		columnBuilder.ColType = &colType
		colFlag := int32(1)
		columnBuilder.ColFlags = &colFlag
		rowdataBuilder.Columns = append(rowdataBuilder.Columns, columnBuilder)
	}

	return rowdataBuilder
}

func clintSendDataWithRetry(b *ticdcToOraclSink, entryBuilder *dsgpb.Entry) error {
	return retry.Run(10*time.Millisecond, 10, func() error {
		return clintSendData(b, entryBuilder)
	})
}
func clintSendData(b *ticdcToOraclSink, entryBuilder *dsgpb.Entry) error {

	err := b.clientRequest.Send(entryBuilder)
	if err != nil {
		return errors.Trace(err)
		//log.Warn("the connection to dsg server is broken")
		////_, err2 := b.clientRequest.CloseAndRecv()
		////todo
		//err := b.clientRequest.CloseSend()
		//_, err2 := b.clientRequest.Recv()
		//if err2 != nil {
		//	log.Warn("error when close the connection", zap.Error(err2))
		//}
		//client := DSGEntryProtocol.NewDSGTiCDCStreamingClient(b.clientConn)
		//grpcCtx := context.Background()
		//request, err := client.DSGTiCDCStreamingRequest(grpcCtx)
		//if err != nil {
		//	return errors.Trace(err)
		//}
		//b.clientRequest = request
		//err = b.clientRequest.Send(entryBuilder)
		//if err != nil {
		//	return errors.Trace(err)
		//}
	}
	resp, err := b.clientRequest.Recv()
	if err != nil {
		return errors.Trace(err)
	}
	if *resp.ReplyType == dsgpb.ReplyType_ERROR {
		return errors.Errorf("failed to send data: %s", resp.ErrorMsg)
	}
	return nil
}
