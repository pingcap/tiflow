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

package codec

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"

	"github.com/linkedin/goavro/v2"

	"golang.org/x/text/encoding"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

const isTest = true

// OperationType for row events type
type OperationType string

const (
	insertOperation OperationType = "INSERT"
	updateOperation OperationType = "UPDATE"
	deleteOperation OperationType = "DELETE"
)

const (
	schemaKeyMid    = "mid"
	schemaKeyDb     = "db"
	schemaKeySch    = "sch"
	schemaKeyTab    = "tab"
	schemaKeyOpt    = "opt"
	schemaKeyTs     = "ts"
	schemaKeyErr    = "err"
	schemaKeySrc    = "src"
	schemaKeyCur    = "cur"
	schemaKeyCus    = "cus"
	schemaKeyMap    = "map"
	schemaKeyString = "string"
)

// JdqEventBatchEncoder converts the events to binary Jdq data
type JdqEventBatchEncoder struct {
	valueSchemaManager *AvroSchemaManager
	resultBuf          []*MQMessage
	binaryEncode       *encoding.Decoder
}

type jdqEncodeResult struct {
	data       []byte
	registryID int
}

// NewJdqEventBatchEncoder creates an JdqEventBatchEncoder
func NewJdqEventBatchEncoder() EventBatchEncoder {
	return &JdqEventBatchEncoder{
		valueSchemaManager: nil,
		resultBuf:          make([]*MQMessage, 0, 4096),
		binaryEncode:       charmap.ISO8859_1.NewDecoder(),
	}
}

// SetValueSchemaManager sets the value schema manager for an Avro encoder
func (a *JdqEventBatchEncoder) SetValueSchemaManager(manager *AvroSchemaManager) {
	a.valueSchemaManager = manager
}

// GetValueSchemaManager gets the value schema manager for an Avro encoder
func (a *JdqEventBatchEncoder) GetValueSchemaManager() *AvroSchemaManager {
	return a.valueSchemaManager
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *JdqEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	mqMessage := NewMQMessage(ProtocolJdqAvro, nil, nil, e.CommitTs, model.MqMessageTypeRow, &e.Table.Schema, &e.Table.Table)
	if e.IsDelete() || e.IsInsert() || e.IsUpdate() {
		res, err := a.jdqEncode(e)
		if err != nil {
			log.Warn("AppendRowChangedEvent: jdq encoding failed", zap.String("table", e.Table.String()))
			return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not encode to Jdq")
		}

		evlp, err := res.toEnvelope()
		if err != nil {
			log.Warn("AppendRowChangedEvent: could not construct Jdq envelope", zap.String("table", e.Table.String()))
			return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not construct Jdq envelope")
		}

		mqMessage.Value = evlp
	} else {
		// log.Warn("AppendRowChangedEvent: don't deal with it", zap.String("table", e.Table.String()))
		// return EncoderNoOperation, nil
		log.Panic("AppendRowChangedEvent: row events error!!!", zap.String("table", e.Table.String()))
	}

	// message key: primary keys, separated by space
	keyStr := ""
	pkeyCols := e.HandlePrimaryKeyColumns()
	for _, key := range pkeyCols {
		data, err := a.columnToJdqNativeData(key)
		if err != nil {
			log.Warn("AppendRowChangedEvent: deal with primary key failed", zap.String("table", e.Table.String()))
			return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not change primary key to native data")
		}
		if data == nil {
			continue
		}

		str := data.(string)
		if keyStr == "" {
			keyStr = str
		} else {
			keyStr = keyStr + " " + str
		}
	}

	keyRes := &jdqEncodeResult{
		data: []byte(keyStr),
	}

	evlp, err := keyRes.toEnvelope()
	if err != nil {
		log.Warn("AppendRowChangedEvent: could not construct Avro envelope", zap.String("table", e.Table.String()))
		return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not construct Avro envelope")
	}

	mqMessage.Key = evlp
	a.resultBuf = append(a.resultBuf, mqMessage)

	return EncoderNeedAsyncWrite, nil
}

// AppendResolvedEvent is no-op for Jdq
func (a *JdqEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

// EncodeCheckpointEvent is no-op for now
func (a *JdqEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return nil, nil
}

// EncodeDDLEvent is no-op now
func (a *JdqEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	return nil, nil
}

// Build MQ Messages
func (a *JdqEventBatchEncoder) Build() (mqMessages []*MQMessage) {
	old := a.resultBuf
	a.resultBuf = nil
	return old
}

// MixedBuild implements the EventBatchEncoder interface
func (a *JdqEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("Mixed Build only use for JsonEncoder")
}

// Reset implements the EventBatchEncoder interface
func (a *JdqEventBatchEncoder) Reset() {
	panic("Reset only used for JsonEncoder")
}

// Size is the current size of resultBuf
func (a *JdqEventBatchEncoder) Size() int {
	if a.resultBuf == nil {
		return 0
	}
	sum := 0
	for _, msg := range a.resultBuf {
		sum += len(msg.Key)
		sum += len(msg.Value)
	}
	return sum
}

// SetParams is no-op for now
func (a *JdqEventBatchEncoder) SetParams(params map[string]string) error {
	// no op
	return nil
}

func (a *JdqEventBatchEncoder) jdqEncode(e *model.RowChangedEvent) (*jdqEncodeResult, error) {
	jdwSchema := `
                  {"type":"record","name":"JdwData","namespace":"com.jd.bdp.jdw.avro",
                   "fields":[{"name":"mid","type":"long"},
                              {"name":"db","type":"string"},
                              {"name":"sch","type":"string"},
                              {"name":"tab","type":"string"},
                              {"name":"opt","type":"string"},
                              {"name":"ts","type":"long"},
                              {"name":"err","type":["string","null"]},
                              {"name":"src","type":[{"type":"map","values":["string","null"]},"null"]},
                              {"name":"cur","type":[{"type":"map","values":["string","null"]},"null"]},
                              {"name":"cus","type":[{"type":"map","values":["string","null"]},"null"]}]}
                  `
	var codec *goavro.Codec
	var registryID int
	var err error
	if isTest {
		schemaGen := func() (string, error) {
			return jdwSchema, nil
		}

		// TODO pass ctx from the upper function. Need to modify the EventBatchEncoder interface.
		codec, registryID, err = a.valueSchemaManager.GetCachedOrRegister(context.Background(), *(e.Table), e.TableInfoVersion, schemaGen)
		if err != nil {
			return nil, errors.Annotate(err, "AvroEventBatchEncoder: get-or-register failed")
		}
	} else {
		codec, err = goavro.NewCodec(jdwSchema)
		if err != nil {
			return nil, errors.Annotate(err, "jdqEncode: Could not make goavro codec")
		}
	}

	native, err := a.rowToJdqNativeData(e)
	if err != nil {
		return nil, errors.Annotate(err, "jdqEncode: converting to native failed")
	}

	bin, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroEncodeToBinary, err), "jdqEncode: converting to avro binary failed")
	}

	return &jdqEncodeResult{
		data:       bin,
		registryID: registryID,
	}, nil
}

func (a *JdqEventBatchEncoder) rowToJdqNativeData(e *model.RowChangedEvent) (interface{}, error) {
	data := make(map[string]interface{})
	data[schemaKeyMid] = int64(e.StartTs)
	data[schemaKeyDb] = e.Table.GetSchema()
	data[schemaKeySch] = e.Table.GetSchema()
	data[schemaKeyTab] = e.Table.GetTable()
	data[schemaKeyTs] = int64(e.CommitTs)
	data[schemaKeyErr] = nil
	op := insertOperation
	if e.IsUpdate() {
		op = updateOperation
	} else if e.IsDelete() {
		op = deleteOperation
	}
	data[schemaKeyOpt] = string(op)

	data[schemaKeySrc] = nil
	data[schemaKeyCur] = nil
	data[schemaKeyCus] = nil

	cols := e.Columns
	if e.IsInsert() {
		colsMap, err := a.getColumnsNativeData(cols)
		if err != nil {
			return nil, err
		}
		data[schemaKeyCur] = colsMap

	} else if e.IsUpdate() {
		preCols := e.PreColumns
		preColsMap, err := a.getColumnsNativeData(preCols)
		if err != nil {
			return nil, err
		}
		data[schemaKeySrc] = preColsMap

		colsMap, err := a.getUpdateColumnsNativeData(preCols, cols)
		if err != nil {
			return nil, err
		}
		data[schemaKeyCur] = colsMap

	} else if e.IsDelete() {
		preCols := e.PreColumns
		colsMap, err := a.getColumnsNativeData(preCols)
		if err != nil {
			return nil, err
		}
		data[schemaKeySrc] = colsMap

		keyColsMap, err := a.getKeyColumnsNativeData(preCols)
		if err != nil {
			return nil, err
		}
		data[schemaKeyCur] = keyColsMap
	} else {
		return nil, errors.New("ignore other row events")
	}

	return data, nil
}

func (a *JdqEventBatchEncoder) getColumnsNativeData(cols []*model.Column) (interface{}, error) {
	colsUnion := make(map[string]interface{}, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		data, err := a.columnToJdqNativeData(col)
		if err != nil {
			return nil, err
		}

		if data != nil {
			union := make(map[string]interface{}, 1)
			union[schemaKeyString] = data
			colsUnion[col.Name] = union
		} else {
			colsUnion[col.Name] = nil
		}
	}
	colMap := make(map[string]interface{}, 1)
	colMap[schemaKeyMap] = colsUnion
	return colMap, nil
}

func (a *JdqEventBatchEncoder) getUpdateColumnsNativeData(oldCols []*model.Column, newCols []*model.Column) (interface{}, error) {
	tmpMap := make(map[string]*model.Column, 1)
	for _, col := range oldCols {
		if col.Flag.IsPrimaryKey() {
			tmpMap[col.Name] = col
		}
	}

	for _, col := range newCols {
		if col.Flag.IsPrimaryKey() {
			tmpMap[col.Name] = nil
		}
	}

	for _, v := range tmpMap {
		if v != nil {
			newCols = append(newCols, v)
		}
	}

	colsMap, err := a.getColumnsNativeData(newCols)
	if err != nil {
		return nil, err
	}

	return colsMap, nil
}

func (a *JdqEventBatchEncoder) getKeyColumnsNativeData(cols []*model.Column) (interface{}, error) {
	var keyCols []*model.Column
	for _, col := range cols {
		if col.Flag.IsPrimaryKey() {
			keyCols = append(keyCols, col)
		}
	}

	colsMap, err := a.getColumnsNativeData(keyCols)
	if err != nil {
		return nil, err
	}

	return colsMap, nil
}

func getJdqDataTypeFallback(v interface{}) (string, error) {
	switch tp := v.(type) {
	// case bool:
	//	return "boolean", nil
	case []byte:
		return string(tp), nil
	case float64:
		return strconv.FormatFloat(tp, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(tp), 'f', -1, 32), nil
	case int64:
		return strconv.FormatInt(tp, 10), nil
	case uint64:
		return strconv.FormatUint(tp, 10), nil
	case int:
		return strconv.Itoa(tp), nil
	case int32:
		return strconv.FormatInt(int64(tp), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(tp), 10), nil
	case string:
		return tp, nil
	default:
		log.Warn("getJdqDataTypeFallback: unknown type")
		return "", cerror.ErrAvroUnknownType.GenWithStackByArgs(tp)
	}
}

func (a *JdqEventBatchEncoder) columnToJdqNativeData(col *model.Column) (interface{}, error) {
	if col.Value == nil {
		return nil, nil
	}

	handleUnsignedInt64 := func() (interface{}, error) {
		var retVal interface{}
		switch v := col.Value.(type) {
		case uint64:
			// val := big.NewRat(0, 1).SetUint64(v)
			retVal = strconv.FormatUint(v, 10)
		case int64:
			// retVal = big.NewRat(0, 1).SetInt64(v)
			retVal = strconv.FormatInt(v, 10)
		}
		return retVal, nil
	}

	switch col.Type {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		str := col.Value.(string)
		return str, nil

	case mysql.TypeDuration:
		str := col.Value.(string)
		return str, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if col.Flag.IsBinary() {
			switch val := col.Value.(type) {
			case string:
				// return []byte(val), nil
				return val, nil
			case []byte:
				// return ISO8859-1
				decoded, err := a.binaryEncode.Bytes(val)
				if err != nil {
					log.Error("Jdq avro could not process binary type", zap.Reflect("col", col))
					return nil, err
				}
				return string(decoded), nil

			}
		} else {
			switch val := col.Value.(type) {
			case string:
				return val, nil
			case []byte:
				return string(val), nil
			}
		}
		log.Panic("Jdq could not process text-like type", zap.Reflect("col", col))
		return nil, errors.New("Unknown datum type")
	case mysql.TypeYear:
		// return col.Value.(int64), nil
		return handleUnsignedInt64()
	case mysql.TypeJSON:
		return col.Value.(string), nil
	case mysql.TypeNewDecimal:
		return col.Value.(string), nil
	case mysql.TypeEnum:
		return *col.Str, nil
	case mysql.TypeSet:
		return *col.Str, nil
	case mysql.TypeBit:
		return handleUnsignedInt64()
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		// return int32(col.Value.(int64)), nil
		return handleUnsignedInt64()
	case mysql.TypeLong:
		/*
			if col.Flag.IsUnsigned() {
				return int64(col.Value.(uint64)), nil
			}
			return col.Value.(int64), nil
		*/
		return handleUnsignedInt64()
	case mysql.TypeLonglong:
		/*
			if col.Flag.IsUnsigned() {
				return handleUnsignedInt64()
			}
			return col.Value.(int64), nil
		*/
		return handleUnsignedInt64()
	default:
		res, err := getJdqDataTypeFallback(col.Value)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

const jdqMagicByte = uint8(0)

func (r *jdqEncodeResult) toEnvelope() ([]byte, error) {
	buf := new(bytes.Buffer)
	var data []interface{}
	if isTest {
		data = []interface{}{jdqMagicByte, int32(r.registryID), r.data}
	} else {
		data = []interface{}{r.data}
	}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}
