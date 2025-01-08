// Copyright 2022 PingCAP, Inc.
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

package canal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

type tableKey struct {
	schema string
	table  string
}

type bufferedJSONDecoder struct {
	buf     *bytes.Buffer
	decoder *json.Decoder
}

func newBufferedJSONDecoder() *bufferedJSONDecoder {
	buf := new(bytes.Buffer)
	decoder := json.NewDecoder(buf)
	return &bufferedJSONDecoder{
		buf:     buf,
		decoder: decoder,
	}
}

// Write writes data to the buffer.
func (b *bufferedJSONDecoder) Write(data []byte) (n int, err error) {
	return b.buf.Write(data)
}

// Decode decodes the buffer into the original message.
func (b *bufferedJSONDecoder) Decode(v interface{}) error {
	return b.decoder.Decode(v)
}

// Len returns the length of the buffer.
func (b *bufferedJSONDecoder) Len() int {
	return b.buf.Len()
}

// Bytes returns the buffer content.
func (b *bufferedJSONDecoder) Bytes() []byte {
	return b.buf.Bytes()
}

// batchDecoder decodes the byte into the original message.
type batchDecoder struct {
	msg     canalJSONMessageInterface
	decoder *bufferedJSONDecoder

	config *common.Config

	storage storage.ExternalStorage

	upstreamTiDB *sql.DB
	bytesDecoder *encoding.Decoder

	tableInfoCache     map[tableKey]*model.TableInfo
	partitionInfoCache map[tableKey]*timodel.PartitionInfo

	tableIDAllocator *common.FakeTableIDAllocator
}

// NewBatchDecoder return a decoder for canal-json
func NewBatchDecoder(
	ctx context.Context, codecConfig *common.Config, db *sql.DB,
) (codec.RowEventDecoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if codecConfig.LargeMessageHandle.EnableClaimCheck() {
		storageURI := codecConfig.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if codecConfig.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &batchDecoder{
		config:             codecConfig,
		decoder:            newBufferedJSONDecoder(),
		storage:            externalStorage,
		upstreamTiDB:       db,
		bytesDecoder:       charmap.ISO8859_1.NewDecoder(),
		tableInfoCache:     make(map[tableKey]*model.TableInfo),
		partitionInfoCache: make(map[tableKey]*timodel.PartitionInfo),
		tableIDAllocator:   common.NewFakeTableIDAllocator(),
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *batchDecoder) AddKeyValue(_, value []byte) error {
	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Error(err))

		return errors.Trace(err)
	}
	if _, err = b.decoder.Write(value); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	if b.decoder.Len() == 0 {
		return model.MessageTypeUnknown, false, nil
	}

	var msg canalJSONMessageInterface = &JSONMessage{}
	if b.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}

	if err := b.decoder.Decode(msg); err != nil {
		log.Error("canal-json decoder decode failed",
			zap.Error(err), zap.ByteString("data", b.decoder.Bytes()))
		return model.MessageTypeUnknown, false, err
	}
	b.msg = msg
	return b.msg.messageType(), true, nil
}

func (b *batchDecoder) assembleClaimCheckRowChangedEvent(ctx context.Context, claimCheckLocation string) (*model.RowChangedEvent, error) {
	_, claimCheckFileName := filepath.Split(claimCheckLocation)
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		return nil, err
	}

	if !b.config.LargeMessageHandle.ClaimCheckRawValue {
		claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
		if err != nil {
			return nil, err
		}
		data = claimCheckM.Value
	}

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, data)
	if err != nil {
		return nil, err
	}
	message := &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(value, message)
	if err != nil {
		return nil, err
	}

	b.msg = message
	return b.NextRowChangedEvent()
}

func (b *batchDecoder) buildData(holder *common.ColumnsHolder) (map[string]interface{}, map[string]string, error) {
	columnsCount := holder.Length()
	data := make(map[string]interface{}, columnsCount)
	mysqlTypeMap := make(map[string]string, columnsCount)

	for i := 0; i < columnsCount; i++ {
		t := holder.Types[i]
		name := holder.Types[i].Name()
		mysqlType := strings.ToLower(t.DatabaseTypeName())

		var value string
		rawValue := holder.Values[i].([]uint8)
		if utils.IsBinaryMySQLType(mysqlType) {
			rawValue, err := b.bytesDecoder.Bytes(rawValue)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			value = string(rawValue)
		} else if strings.Contains(mysqlType, "bit") || strings.Contains(mysqlType, "set") {
			bitValue := common.MustBinaryLiteralToInt(rawValue)
			value = strconv.FormatUint(bitValue, 10)
		} else {
			value = string(rawValue)
		}
		mysqlTypeMap[name] = mysqlType
		data[name] = value
	}

	return data, mysqlTypeMap, nil
}

func (b *batchDecoder) assembleHandleKeyOnlyRowChangedEvent(
	ctx context.Context, message *canalJSONMessageWithTiDBExtension,
) (*model.RowChangedEvent, error) {
	var (
		commitTs  = message.Extensions.CommitTs
		schema    = message.Schema
		table     = message.Table
		eventType = message.EventType
	)
	conditions := make(map[string]interface{}, len(message.pkNameSet()))
	for name := range message.pkNameSet() {
		conditions[name] = message.getData()[name]
	}
	result := &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			Schema:  schema,
			Table:   table,
			PKNames: message.PKNames,

			EventType: eventType,
		},
		Extensions: &tidbExtension{
			CommitTs: commitTs,
		},
	}
	switch eventType {
	case "INSERT":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	case "UPDATE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}

		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		old, _, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.Old = []map[string]interface{}{old}
	case "DELETE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	}

	b.msg = result
	return b.NextRowChangedEvent()
}

func setColumnInfos(
	tableInfo *timodel.TableInfo,
	rawColumns map[string]interface{},
	mysqlType map[string]string,
	pkNames map[string]struct{},
) {
	mockColumnID := int64(100)
	for name := range rawColumns {
		columnInfo := new(timodel.ColumnInfo)
		columnInfo.ID = mockColumnID
		columnInfo.Name = pmodel.NewCIStr(name)
		if utils.IsBinaryMySQLType(mysqlType[name]) {
			columnInfo.AddFlag(mysql.BinaryFlag)
		}
		if _, isPK := pkNames[name]; isPK {
			columnInfo.AddFlag(mysql.PriKeyFlag)
		}
		tableInfo.Columns = append(tableInfo.Columns, columnInfo)
		mockColumnID++
	}
}

func setIndexes(
	tableInfo *timodel.TableInfo,
	pkNames map[string]struct{},
) {
	indexColumns := make([]*timodel.IndexColumn, 0, len(pkNames))
	for idx, col := range tableInfo.Columns {
		name := col.Name.O
		if _, ok := pkNames[name]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   pmodel.NewCIStr(name),
				Offset: idx,
			})
		}
	}
	indexInfo := &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
	}
	tableInfo.Indices = append(tableInfo.Indices, indexInfo)
}

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeRow {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found row changed event message")
	}

	message, withExtension := b.msg.(*canalJSONMessageWithTiDBExtension)
	if withExtension {
		ctx := context.Background()
		if message.Extensions.OnlyHandleKey {
			return b.assembleHandleKeyOnlyRowChangedEvent(ctx, message)
		}
		if message.Extensions.ClaimCheckLocation != "" {
			return b.assembleClaimCheckRowChangedEvent(ctx, message.Extensions.ClaimCheckLocation)
		}
	}

	result, err := b.canalJSONMessage2RowChange()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NextDDLEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeDDL {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found ddl event message")
	}

	result := canalJSONMessage2DDLEvent(b.msg)
	schema := *b.msg.getSchema()
	table := *b.msg.getTable()
	cacheKey := tableKey{
		schema: schema,
		table:  table,
	}
	// if receive a table level DDL, just remove the table info to trigger create a new one.
	if schema != "" && table != "" {
		delete(b.tableInfoCache, cacheKey)
		delete(b.partitionInfoCache, cacheKey)
	}

	stmt, err := parser.New().ParseOneStmt(result.Query, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v, ok := stmt.(*ast.CreateTableStmt); ok {
		tableInfo, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions := tableInfo.GetPartitionInfo()
		if partitions != nil {
			b.partitionInfoCache[cacheKey] = partitions
		}
	}

	return result, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeResolved {
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("MessageTypeResolved tidb extension not found")
	}
	return withExtensionEvent.Extensions.WatermarkTs, nil
}
