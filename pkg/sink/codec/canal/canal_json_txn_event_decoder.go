// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package canal

import (
	"bytes"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

type canalJSONTxnEventDecoder struct {
	data []byte

	config *common.Config
	msg    canalJSONMessageInterface
}

// NewCanalJSONTxnEventDecoder return a new CanalJSONTxnEventDecoder.
func NewCanalJSONTxnEventDecoder(
	codecConfig *common.Config,
) *canalJSONTxnEventDecoder {
	return &canalJSONTxnEventDecoder{
		config: codecConfig,
	}
}

// AddKeyValue set the key value to the decoder
func (d *canalJSONTxnEventDecoder) AddKeyValue(_, value []byte) error {
	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", d.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Error(err))

		return errors.Trace(err)
	}
	d.data = value
	return nil
}

// HasNext return true if there is any event can be returned.
func (d *canalJSONTxnEventDecoder) HasNext() (model.MessageType, bool, error) {
	if d.data == nil {
		return model.MessageTypeUnknown, false, nil
	}
	var (
		msg         canalJSONMessageInterface = &JSONMessage{}
		encodedData []byte
	)

	if d.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}

	idx := bytes.IndexAny(d.data, d.config.Terminator)
	if idx >= 0 {
		encodedData = d.data[:idx]
		d.data = d.data[idx+len(d.config.Terminator):]
	} else {
		encodedData = d.data
		d.data = nil
	}

	if len(encodedData) == 0 {
		return model.MessageTypeUnknown, false, nil
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		return model.MessageTypeUnknown, false, err
	}
	d.msg = msg
	return d.msg.messageType(), true, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (d *canalJSONTxnEventDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if d.msg == nil || d.msg.messageType() != model.MessageTypeRow {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found row changed event message")
	}
	result, err := d.canalJSONMessage2RowChange()
	if err != nil {
		return nil, err
	}
	d.msg = nil
	return result, nil
}

func (d *canalJSONTxnEventDecoder) canalJSONMessage2RowChange() (*model.RowChangedEvent, error) {
	msg := d.msg
	result := new(model.RowChangedEvent)
	result.TableInfo = newTableInfo(msg, nil)
	result.CommitTs = msg.getCommitTs()

	mysqlType := msg.getMySQLType()
	var err error
	if msg.eventType() == canal.EventType_DELETE {
		// for `DELETE` event, `data` contain the old data, set it as the `PreColumns`
		result.PreColumns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// for `INSERT` and `UPDATE`, `data` contain fresh data, set it as the `Columns`
	result.Columns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
	if err != nil {
		return nil, err
	}

	// for `UPDATE`, `old` contain old data, set it as the `PreColumns`
	if msg.eventType() == canal.EventType_UPDATE {
		preCols, err := canalJSONColumnMap2RowChangeColumns(msg.getOld(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		if len(preCols) < len(result.Columns) {
			newPreCols := make([]*model.ColumnData, 0, len(preCols))
			j := 0
			// Columns are ordered by name
			for _, col := range result.Columns {
				if j < len(preCols) && col.ColumnID == preCols[j].ColumnID {
					newPreCols = append(newPreCols, preCols[j])
					j += 1
				} else {
					newPreCols = append(newPreCols, col)
				}
			}
			preCols = newPreCols
		}
		result.PreColumns = preCols
		if len(preCols) != len(result.Columns) {
			log.Panic("column count mismatch", zap.Any("preCols", preCols), zap.Any("cols", result.Columns))
		}
	}
	return result, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (d *canalJSONTxnEventDecoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (d *canalJSONTxnEventDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	return nil, nil
}
