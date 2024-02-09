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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
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

// batchDecoder decodes the byte into the original message.
type batchDecoder struct {
	data []byte
	msg  canalJSONMessageInterface

	config *common.Config

	storage storage.ExternalStorage

	upstreamTiDB *sql.DB
	bytesDecoder *encoding.Decoder
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
		externalStorage, err = util.GetExternalStorageFromURI(ctx, storageURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if codecConfig.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &batchDecoder{
		config:       codecConfig,
		storage:      externalStorage,
		upstreamTiDB: db,
		bytesDecoder: charmap.ISO8859_1.NewDecoder(),
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
	b.data = value
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	if b.data == nil {
		return model.MessageTypeUnknown, false, nil
	}
	var (
		msg         canalJSONMessageInterface = &JSONMessage{}
		encodedData []byte
	)

	if b.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}

	if len(b.config.Terminator) > 0 {
		idx := bytes.IndexAny(b.data, b.config.Terminator)
		if idx >= 0 {
			encodedData = b.data[:idx]
			b.data = b.data[idx+len(b.config.Terminator):]
		} else {
			encodedData = b.data
			b.data = nil
		}
	} else {
		encodedData = b.data
		b.data = nil
	}

	if len(encodedData) == 0 {
		return model.MessageTypeUnknown, false, nil
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
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
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, err
	}

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, claimCheckM.Value)
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
			bitValue, err := common.BinaryLiteralToInt(rawValue)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
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

	handleKeyData := message.getData()
	pkNames := make([]string, 0, len(handleKeyData))
	for name := range handleKeyData {
		pkNames = append(pkNames, name)
	}

	result := &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			Schema:  schema,
			Table:   table,
			PKNames: pkNames,

			EventType: eventType,
		},
		Extensions: &tidbExtension{
			CommitTs: commitTs,
		},
	}

	switch eventType {
	case "INSERT":
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, handleKeyData)
		if err != nil {
			return nil, err
		}
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	case "UPDATE":
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, handleKeyData)
		if err != nil {
			return nil, err
		}
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}

		holder, err = common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, message.getOld())
		if err != nil {
			return nil, err
		}
		old, _, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.Old = []map[string]interface{}{old}
	case "DELETE":
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, handleKeyData)
		if err != nil {
			return nil, err
		}
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

	result, err := canalJSONMessage2RowChange(b.msg)
	if err != nil {
		return nil, err
	}
	b.msg = nil
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
	b.msg = nil
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
	b.msg = nil
	return withExtensionEvent.Extensions.WatermarkTs, nil
}
