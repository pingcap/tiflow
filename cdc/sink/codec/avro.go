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
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/types"
	tijson "github.com/pingcap/tidb/types/json"
	"go.uber.org/zap"
)

// AvroEventBatchEncoder converts the events to binary Avro data
type AvroEventBatchEncoder struct {
	keySchemaManager   *AvroSchemaManager
	valueSchemaManager *AvroSchemaManager
	resultBuf          []*MQMessage
}

type avroEncodeResult struct {
	data       []byte
	registryID int
}

// NewAvroEventBatchEncoder creates an AvroEventBatchEncoder
func NewAvroEventBatchEncoder() EventBatchEncoder {
	return &AvroEventBatchEncoder{
		valueSchemaManager: nil,
		keySchemaManager:   nil,
		resultBuf:          make([]*MQMessage, 0, 4096),
	}
}

// SetValueSchemaManager sets the value schema manager for an Avro encoder
func (a *AvroEventBatchEncoder) SetValueSchemaManager(manager *AvroSchemaManager) {
	a.valueSchemaManager = manager
}

// GetValueSchemaManager gets the value schema manager for an Avro encoder
func (a *AvroEventBatchEncoder) GetValueSchemaManager() *AvroSchemaManager {
	return a.valueSchemaManager
}

// SetKeySchemaManager sets the value schema manager for an Avro encoder
func (a *AvroEventBatchEncoder) SetKeySchemaManager(manager *AvroSchemaManager) {
	a.keySchemaManager = manager
}

// GetKeySchemaManager gets the value schema manager for an Avro encoder
func (a *AvroEventBatchEncoder) GetKeySchemaManager() *AvroSchemaManager {
	return a.keySchemaManager
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *AvroEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	mqMessage := NewMQMessage(nil, nil, e.CommitTs)

	if !e.IsDelete() {
		res, err := avroEncode(e.Table, a.valueSchemaManager, e.TableInfoVersion, e.Columns)
		if err != nil {
			log.Warn("AppendRowChangedEvent: avro encoding failed", zap.String("table", e.Table.String()))
			return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not encode to Avro")
		}

		evlp, err := res.toEnvelope()
		if err != nil {
			log.Warn("AppendRowChangedEvent: could not construct Avro envelope", zap.String("table", e.Table.String()))
			return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not construct Avro envelope")
		}

		mqMessage.Value = evlp
	} else {
		mqMessage.Value = nil
	}

	pkeyCols := make([]*model.Column, 0)
	for _, col := range e.Columns {
		if col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
		}
	}

	res, err := avroEncode(e.Table, a.keySchemaManager, e.TableInfoVersion, pkeyCols)
	if err != nil {
		log.Warn("AppendRowChangedEvent: avro encoding failed", zap.String("table", e.Table.String()))
		return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not encode to Avro")
	}

	evlp, err := res.toEnvelope()
	if err != nil {
		log.Warn("AppendRowChangedEvent: could not construct Avro envelope", zap.String("table", e.Table.String()))
		return EncoderNoOperation, errors.Annotate(err, "AppendRowChangedEvent could not construct Avro envelope")
	}

	mqMessage.Key = evlp
	a.resultBuf = append(a.resultBuf, mqMessage)

	return EncoderNeedAsyncWrite, nil
}

// AppendResolvedEvent is no-op for Avro
func (a *AvroEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNeedAsyncWrite, nil
}

// EncodeCheckpointEvent is no-op for now
func (a *AvroEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return nil, nil
}

// EncodeDDLEvent is no-op now
func (a *AvroEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	return nil, nil
}

// Build MQ Messages
func (a *AvroEventBatchEncoder) Build() (mqMessages []*MQMessage) {
	old := a.resultBuf
	a.resultBuf = nil
	return old
}

// MixedBuild implements the EventBatchEncoder interface
func (a *AvroEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("Mixed Build only use for JsonEncoder")
}

// Reset implements the EventBatchEncoder interface
func (a *AvroEventBatchEncoder) Reset() {
	panic("Reset only used for JsonEncoder")
}

// Size is the current size of resultBuf
func (a *AvroEventBatchEncoder) Size() int {
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

func avroEncode(table *model.TableName, manager *AvroSchemaManager, tableVersion uint64, cols []*model.Column) (*avroEncodeResult, error) {
	schemaGen := func() (string, error) {
		schema, err := ColumnInfoToAvroSchema(table.Table, cols)
		if err != nil {
			return "", errors.Annotate(err, "AvroEventBatchEncoder: generating schema failed")
		}
		return schema, nil
	}

	// TODO pass ctx from the upper function. Need to modify the EventBatchEncoder interface.
	avroCodec, registryID, err := manager.GetCachedOrRegister(context.Background(), *table, tableVersion, schemaGen)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: get-or-register failed")
	}

	native, err := rowToAvroNativeData(cols)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: converting to native failed")
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroEncodeToBinary, err), "AvroEventBatchEncoder: converting to Avro binary failed")
	}

	return &avroEncodeResult{
		data:       bin,
		registryID: registryID,
	}, nil
}

type avroSchemaTop struct {
	Tp     string                   `json:"type"`
	Name   string                   `json:"name"`
	Fields []map[string]interface{} `json:"fields"`
}

type logicalType string

type avroLogicalType struct {
	Type        string      `json:"type"`
	LogicalType logicalType `json:"logicalType"`
	Precision   interface{} `json:"precision,omitempty"`
	Scale       interface{} `json:"scale,omitempty"`
}

const (
	timestampMillis logicalType = "timestamp-millis"
	timeMillis      logicalType = "time-millis"
	decimalType     logicalType = "decimal"
)

// ColumnInfoToAvroSchema generates the Avro schema JSON for the corresponding columns
func ColumnInfoToAvroSchema(name string, columnInfo []*model.Column) (string, error) {
	top := avroSchemaTop{
		Tp:     "record",
		Name:   name,
		Fields: nil,
	}

	for _, col := range columnInfo {
		avroType, err := getAvroDataTypeFromColumn(col)
		if err != nil {
			return "", err
		}
		field := make(map[string]interface{})
		field["name"] = col.Name
		if col.Flag.IsHandleKey() {
			field["type"] = avroType
		} else {
			field["type"] = []interface{}{"null", avroType}
			field["default"] = nil
		}

		top.Fields = append(top.Fields, field)
	}

	str, err := json.Marshal(&top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Debug("Avro Schema JSON generated", zap.ByteString("schema", str))
	return string(str), nil
}

func rowToAvroNativeData(cols []*model.Column) (interface{}, error) {
	ret := make(map[string]interface{}, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		data, str, err := columnToAvroNativeData(col)
		if err != nil {
			return nil, err
		}

		if col.Flag.IsHandleKey() {
			ret[col.Name] = data
			continue
		}
		union := make(map[string]interface{}, 1)
		union[str] = data
		ret[col.Name] = union
	}
	return ret, nil
}

func getAvroDataTypeFallback(v interface{}) (string, error) {
	switch tp := v.(type) {
	case bool:
		return "boolean", nil
	case []byte:
		return "bytes", nil
	case float64:
		return "double", nil
	case float32:
		return "float", nil
	case int64, uint64:
		return "long", nil
	case int, int32, uint32:
		return "int", nil
	case nil:
		return "null", nil
	case string:
		return "string", nil
	default:
		log.Warn("getAvroDataTypeFallback: unknown type")
		return "", cerror.ErrAvroUnknownType.GenWithStackByArgs(tp)
	}
}

var unsignedLongAvroType = avroLogicalType{
	Type:        "bytes",
	LogicalType: decimalType,
	Precision:   8,
	Scale:       0,
}

func getAvroDataTypeFromColumn(col *model.Column) (interface{}, error) {
	log.Info("DEBUG: getAvroDataTypeFromColumn", zap.Reflect("col", col))
	switch col.Type {
	case mysql.TypeFloat:
		return "float", nil
	case mysql.TypeDouble:
		return "double", nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		return "string", nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return avroLogicalType{
			Type:        "long",
			LogicalType: timestampMillis,
		}, nil
	case mysql.TypeDuration:
		return avroLogicalType{
			Type:        "int",
			LogicalType: timeMillis,
		}, nil
	case mysql.TypeEnum:
		return unsignedLongAvroType, nil
	case mysql.TypeSet:
		return unsignedLongAvroType, nil
	case mysql.TypeBit:
		return unsignedLongAvroType, nil
	case mysql.TypeNewDecimal:
		return "string", nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return "int", nil
	case mysql.TypeLong:
		if col.Flag.IsUnsigned() {
			return "long", nil
		}
		return "int", nil
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			return unsignedLongAvroType, nil
		}
		return "long", nil
	case mysql.TypeNull:
		return "null", nil
	case mysql.TypeJSON:
		return "string", nil
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return "bytes", nil
	case mysql.TypeYear:
		return "long", nil
	default:
		log.Fatal("Unknown MySql type", zap.Reflect("mysql-type", col.Type))
		return "", errors.New("Unknown Mysql type")
	}
}

func columnToAvroNativeData(col *model.Column) (interface{}, string, error) {
	if col.Value == nil {
		return nil, "null", nil
	}

	handleUnsignedInt64 := func() (interface{}, string, error) {
		var retVal interface{}
		switch v := col.Value.(type) {
		case uint64:
			retVal = big.NewRat(0, 1).SetUint64(v)
		case int64:
			retVal = big.NewRat(0, 1).SetInt64(v)
		}
		return retVal, string("bytes." + decimalType), nil
	}

	switch col.Type {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		str := col.Value.(string)
		t, err := time.Parse(types.DateFormat, str)
		const fullType = "long." + timestampMillis
		if err == nil {
			return t, string(fullType), nil
		}

		t, err = time.Parse(types.TimeFormat, str)
		if err == nil {
			return t, string(fullType), nil
		}

		t, err = time.Parse(types.TimeFSPFormat, str)
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return t, string(fullType), nil
	case mysql.TypeDuration:
		str := col.Value.(string)
		var (
			hours   int
			minutes int
			seconds int
			frac    string
		)
		_, err := fmt.Sscanf(str, "%d:%d:%d.%s", &hours, &minutes, &seconds, &frac)
		if err != nil {
			_, err := fmt.Sscanf(str, "%d:%d:%d", &hours, &minutes, &seconds)
			frac = "0"

			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
		}

		fsp := len(frac)
		fracInt, err := strconv.ParseInt(frac, 10, 32)
		if err != nil {
			return nil, "", err
		}
		fracInt = int64(float64(fracInt) * math.Pow10(6-fsp))

		d := types.NewDuration(hours, minutes, seconds, int(fracInt), int8(fsp)).Duration
		const fullType = "int." + timeMillis
		return d, string(fullType), nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if col.Flag.IsBinary() {
			switch val := col.Value.(type) {
			case string:
				return []byte(val), "bytes", nil
			case []byte:
				return val, "bytes", nil
			}
		} else {
			switch val := col.Value.(type) {
			case string:
				return val, "string", nil
			case []byte:
				return string(val), "string", nil
			}
		}
		log.Fatal("Avro could not process text-like type", zap.Reflect("col", col))
		return nil, "", errors.New("Unknown datum type")
	case mysql.TypeYear:
		return col.Value.(int64), "long", nil
	case mysql.TypeJSON:
		return col.Value.(tijson.BinaryJSON).String(), "string", nil
	case mysql.TypeNewDecimal:
		return col.Value.(string), "string", nil
	case mysql.TypeEnum:
		return handleUnsignedInt64()
	case mysql.TypeSet:
		return handleUnsignedInt64()
	case mysql.TypeBit:
		return handleUnsignedInt64()
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLong:
		if col.Flag.IsUnsigned() {
			return int64(col.Value.(uint64)), "long", nil
		}
		return col.Value.(int64), "int", nil
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			return handleUnsignedInt64()
		}
		return col.Value.(int64), "long", nil
	default:
		avroType, err := getAvroDataTypeFallback(col.Value)
		if err != nil {
			return nil, "", err
		}
		return col.Value, avroType, nil
	}
}

const magicByte = uint8(0)

func (r *avroEncodeResult) toEnvelope() ([]byte, error) {
	buf := new(bytes.Buffer)
	data := []interface{}{magicByte, int32(r.registryID), r.data}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}
