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
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/goccy/go-json"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// batchDecoder decodes the byte into the original message.
type batchDecoder struct {
	data []byte
	msg  canalJSONMessageInterface

	config *common.Config

	storage storage.ExternalStorage

	upstreamTiDB *sql.DB
}

// NewBatchDecoder return a decoder for canal-json
func NewBatchDecoder(
	ctx context.Context, codecConfig *common.Config, db *sql.DB,
) (codec.RowEventDecoder, error) {
	var (
		storage storage.ExternalStorage
		err     error
	)
	if codecConfig.LargeMessageHandle.EnableClaimCheck() {
		storageURI := codecConfig.LargeMessageHandle.ClaimCheckStorageURI
		storage, err = util.GetExternalStorageFromURI(ctx, storageURI)
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
		storage:      storage,
		upstreamTiDB: db,
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *batchDecoder) AddKeyValue(_, value []byte) error {
	b.data = value
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
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
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	data, err := b.storage.ReadFile(ctx, claimCheckLocation)
	if err != nil {
		return nil, err
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, err
	}

	message := &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(claimCheckM.Value, message)
	if err != nil {
		return nil, err
	}

	b.msg = message
	return b.NextRowChangedEvent()
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
	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	conn, err := b.upstreamTiDB.Conn(ctx)
	if err != nil {
		log.Error("establish connection to the upstream tidb failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, errors.Trace(err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
		if ok {
			// Error 8055 (HY000): snapshot is older than GC safe point
			if mysqlErr.Number == 8055 {
				log.Error("set snapshot read failed, since snapshot is older than GC safe point")
			}
		}

		log.Error("set snapshot read failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, errors.Trace(err)
	}

	data := message.Data[0]
	// 2. query the whole row
	query = fmt.Sprintf("select * from `%s`.`%s` where ", schema, table)
	var whereClause string
	for name, value := range data {
		if whereClause != "" {
			whereClause += " and "
		}
		whereClause += fmt.Sprintf("`%s` = '%v'", name, value)
	}
	query += whereClause

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		log.Error("query row failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, errors.Trace(err)
	}

	holder, err := newColumnHolder(rows)
	if err != nil {
		log.Error("obtain the columns holder failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, err
	}
	for rows.Next() {
		err = rows.Scan(holder.ValuePointers...)
		if err != nil {
			log.Error("scan row failed",
				zap.String("schema", schema), zap.String("table", table),
				zap.Uint64("commitTs", commitTs), zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	var (
		data map[string]interface{}
		old  map[string]interface{}
	)

	data = make(map[string]interface{}, holder.length())
	mysqlType := make(map[string]string, holder.length())
	javaSQLType := make(map[string]int32, holder.length())
	for i := 0; i < holder.length(); i++ {
		value := holder.Values[i]
		name := holder.Types[i].Name()

		mysqlType[name] = strings.ToLower(holder.Types[i].DatabaseTypeName())
		data[name] = value
	}

	pkNames := make([]string, 0, len(data))
	for name := range data {
		pkNames = append(pkNames, name)
	}

	message = &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			Schema:  schema,
			Table:   table,
			PKNames: pkNames,

			EventType: eventType,

			MySQLType: mysqlType,
			SQLType:   javaSQLType,

			Data: []map[string]interface{}{data},
			Old:  nil,
		},
		Extensions: &tidbExtension{
			CommitTs: commitTs,
		},
	}

	b.msg = message
	return b.NextRowChangedEvent()
}

type ColumnsHolder struct {
	Values        []interface{}
	ValuePointers []interface{}
	Types         []*sql.ColumnType
}

func newColumnHolder(rows *sql.Rows) (*ColumnsHolder, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make([]interface{}, len(columnTypes))
	valuePointers := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	return &ColumnsHolder{
		Values:        values,
		ValuePointers: valuePointers,
		Types:         columnTypes,
	}, nil
}

func (h *ColumnsHolder) length() int {
	return len(h.Values)
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
