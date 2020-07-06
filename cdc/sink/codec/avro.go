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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// AvroEventBatchEncoder converts the events to binary Avro data
type AvroEventBatchEncoder struct {
	// TODO use Avro for Kafka keys
	// keySchemaManager   *AvroSchemaManager
	valueSchemaManager *AvroSchemaManager
	keyBuf             *bytes.Buffer
	valueBuf           *bytes.Buffer
}

type avroEncodeResult struct {
	data       []byte
	registryId int
}

func (a *AvroEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
	return nil
}

func (a *AvroEventBatchEncoder) avroEncode(table *model.TableName, tiSchemaId int64, cols map[string]*model.Column) (*avroEncodeResult, error) {
	avroCodec, registryId, err := a.valueSchemaManager.Lookup(context.Background(), *table, tiSchemaId)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: lookup failed")
	}

	native, err := rowToAvroNativeData(cols)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: converting to native failed")
	}


	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: converting to Avro binary failed")
	}

	return &avroEncodeResult{
		data:       bin,
		registryId: registryId,
	}, nil
}

func rowToAvroNativeData(cols map[string]*model.Column) (interface{}, error) {
	ret := make(map[string]interface{}, len(cols))
	for key, col := range cols {
		data, str, err := columnToAvroNativeData(col)
		if err != nil {
			return nil, err
		}

		union := make(map[string]interface{}, 1)
		union[str] = data
		ret[key] = union
	}
	return ret, nil
}

func getAvroDataTypeName(v interface{}) string {
	switch v.(type) {
	case bool:
		return "boolean"
	case []byte:
		return "bytes"
	case float64:
		return "double"
	case float32:
		return "float"
	case int64, uint64:
		return "long"
	case int32, uint32:
		return "int"
	case nil:
		return "null"
	case string:
		return "string"
	case time.Duration:
		return "int.time-millis"
	case time.Time:
		return "long.timestamp-millis"
	default:
		return "errorType"
	}
}

func columnToAvroNativeData(col *model.Column) (interface{}, string, error) {
	switch col.Type {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		str := col.Value.(string)
		t, err := time.Parse(types.DateFormat, str)
		if err == nil {
			return t, "long.timestamp-millis", nil
		}

		t, err = time.Parse(types.TimeFormat, str)
		if err == nil {
			return t, "long.timestamp-millis", nil
		}

		t, err = time.Parse(types.TimeFSPFormat, str)
		if err != nil {
			return nil, "error", err
		}
		return t, "long.timestamp-millis", nil
	case mysql.TypeDuration:
		str := col.Value.(string)
		d, err := time.ParseDuration(str)
		if err != nil {
			return nil, "error", err
		}
		return d, "long.timestamp-millis", nil
	case mysql.TypeJSON:
		return col.Value.(json.BinaryJSON).String(), "string", nil
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		dec := col.Value.(*types.MyDecimal)
		if dec == nil {
			return nil, "null", nil
		}
		return dec.String(), "string", nil
	case mysql.TypeEnum:
		return col.Value.(types.Enum).Value, "long", nil
	case mysql.TypeSet:
		return col.Value.(types.Set).Value, "long", nil
	case mysql.TypeBit:
		return col.Value.(uint64), "long", nil
	default:
		return col.Value, getAvroDataTypeName(col.Value), nil
	}
}

const magicByte = uint8(0)

func (r *avroEncodeResult) toEnvelope() ([]byte, error) {
	buf := new(bytes.Buffer)
	data := []interface{}{magicByte, r.registryId, r.data}
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil, errors.Annotate(err, "converting Avro data to envelop failed")
	}
	return buf.Bytes(), nil
}