// Copyright 2021 PingCAP, Inc.
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

package avro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type Codec struct {
	namespace    string
	changeFeedID string
	schemaM      SchemaManager
	config       *common.Config
	protocol     config.Protocol
}

type avroSchemaTop struct {
	Tp          string                   `json:"type"`
	Name        string                   `json:"name"`
	Namespace   string                   `json:"namespace"`
	Fields      []map[string]interface{} `json:"fields"`
	ConnectName string                   `json:"connect.name,omitempty"`
}

type avroSchema struct {
	Type string `json:"type"`
	// connect.parameters is designated field extracted by schema registry
	Parameters map[string]string `json:"connect.parameters"`
}

type avroLogicalTypeSchema struct {
	avroSchema
	LogicalType string      `json:"logicalType"`
	Precision   interface{} `json:"precision,omitempty"`
	Scale       interface{} `json:"scale,omitempty"`
}

type avroEncodeInput struct {
	columns  []*model.Column
	colInfos []rowcodec.ColInfo
}

func (r *avroEncodeInput) Less(i, j int) bool {
	return r.colInfos[i].ID < r.colInfos[j].ID
}

func (r *avroEncodeInput) Len() int {
	return len(r.columns)
}

func (r *avroEncodeInput) Swap(i, j int) {
	r.colInfos[i], r.colInfos[j] = r.colInfos[j], r.colInfos[i]
	r.columns[i], r.columns[j] = r.columns[j], r.columns[i]
}

type avroEncodeResult struct {
	data []byte
	// header is the message header, it will be encoder into the head
	// of every single avro message. Note: Confluent schema registry and
	// Aws Glue schema registry have different header format.
	header []byte
}

func (r *avroEncodeResult) toEnvelope() ([]byte, error) {
	buf := new(bytes.Buffer)
	data := []interface{}{r.header, r.data}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}

const (
	tidbType         = "tidb_type"
	tidbOp           = "_tidb_op"
	tidbCommitTs     = "_tidb_commit_ts"
	tidbPhysicalTime = "_tidb_commit_physical_time"

	// row level checksum related fields
	tidbRowLevelChecksum = "_tidb_row_level_checksum"
	tidbChecksumVersion  = "_tidb_checksum_version"
	tidbCorrupted        = "_tidb_corrupted"

	// subject suffix for schema registry
	keySchemaSuffix   = "-key"
	valueSchemaSuffix = "-value"

	// operation
	insertOperation = "c"
	updateOperation = "u"
	deleteOperation = "d"

	// sanitized constant
	replacementChar = "_"
	numberPrefix    = "_"

	// debezium field names
	debeziumBefore = "before"
	debeziumAfter  = "after"
	debeziumOp     = "op"
	debeziumTsMs   = "ts_ms"

	debeziumSource            = "source"
	debeziumSourceCdc         = "cdc"
	debeziumSourceVersion     = "version"
	debeziumSourceChaneFeedId = "changefeed_id"
	debeziumSourceDB          = "db"
	debeziumSourceTable       = "table"
	debeziumSourceTsMs        = "ts_ms"
	debeziumSourceCommitTS    = "commit_ts"
)

func NewAvroCodec(
	namespace string,
	changeFeedID string,
	schemaM SchemaManager,
	config *common.Config,
) codec.Format {
	return &Codec{
		config:       config,
		schemaM:      schemaM,
		namespace:    namespace,
		changeFeedID: changeFeedID,
		protocol:     config.Protocol,
	}
}

func (c *Codec) EncodeKey(ctx context.Context, topic string, e *model.RowChangedEvent) ([]byte, error) {
	cols, colInfos := e.HandleKeyColInfos()
	// result may be nil if the event has no handle key columns, this may happen in the force replicate mode.
	// todo: disallow force replicate mode if using the avro.
	if len(cols) == 0 {
		return nil, nil
	}

	keyColumns := &avroEncodeInput{
		columns:  cols,
		colInfos: colInfos,
	}
	avroCodec, header, err := c.getKeySchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.Version, keyColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := c.columns2AvroData(keyColumns)
	if err != nil {
		log.Error("avro: key converting to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		log.Error("avro: key converting to Avro binary failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroEncodeToBinary, err)
	}

	result := &avroEncodeResult{
		data:   bin,
		header: header,
	}
	data, err := result.toEnvelope()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (c *Codec) EncodeValue(ctx context.Context, topic string, e *model.RowChangedEvent) ([]byte, error) {
	columns := e.GetColumns()
	// Handle delete event for protocol avro and debezium
	if e.IsDelete() {
		if c.protocol == config.ProtocolAvro {
			return nil, nil
		} else if c.protocol == config.ProtocolDebezium {
			columns = e.GetPreColumns()
		}
	}

	input := &avroEncodeInput{
		columns:  columns,
		colInfos: e.TableInfo.GetColInfosForRowChangedEvent(),
	}
	if len(input.columns) == 0 {
		return nil, nil
	}

	avroCodec, header, err := c.getValueSchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.Version, input)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var native map[string]interface{}
	if c.protocol == config.ProtocolDebezium {
		native, err = c.columns2DebeziumAvroData(topic, e)
	} else {
		native, err = c.columns2AvroData(input)
	}
	if err != nil {
		log.Error("avro: converting value to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if c.config.EnableTiDBExtension {
		native = c.nativeValueWithExtension(native, e)
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		log.Error("avro: converting value to Avro binary failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroEncodeToBinary, err)
	}

	result := &avroEncodeResult{
		data:   bin,
		header: header,
	}
	data, err := result.toEnvelope()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func GetOperation(e *model.RowChangedEvent) string {
	if e.IsInsert() {
		return insertOperation
	} else if e.IsUpdate() {
		return updateOperation
	} else if e.IsDelete() {
		return deleteOperation
	}
	return ""
}

func (c *Codec) nativeValueWithExtension(
	native map[string]interface{},
	e *model.RowChangedEvent,
) map[string]interface{} {
	native[tidbOp] = GetOperation(e)
	native[tidbCommitTs] = int64(e.CommitTs)
	native[tidbPhysicalTime] = oracle.ExtractPhysical(e.CommitTs)

	if c.config.EnableRowChecksum && e.Checksum != nil {
		native[tidbRowLevelChecksum] = strconv.FormatUint(uint64(e.Checksum.Current), 10)
		native[tidbCorrupted] = e.Checksum.Corrupted
		native[tidbChecksumVersion] = e.Checksum.Version
	}
	return native
}

func (c *Codec) getKeySchemaCodec(
	ctx context.Context, topic string, tableName *model.TableName, tableVersion uint64, keyColumns *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := c.key2AvroSchema(topic, tableName, keyColumns)
		if err != nil {
			log.Error("AvroEventBatchEncoder: generating key schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, keySchemaSuffix)
	avroCodec, header, err := c.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (c *Codec) getValueSchemaCodec(
	ctx context.Context, topic string, tableName *model.TableName, tableVersion uint64, input *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := c.value2AvroSchema(topic, tableName, input)
		if err != nil {
			log.Error("avro: generating value schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, valueSchemaSuffix)
	avroCodec, header, err := c.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (c *Codec) key2AvroSchema(
	topic string,
	tableName *model.TableName,
	keyColumns *avroEncodeInput,
) (string, error) {
	top, err := c.columns2AvroSchema(tableName, keyColumns)
	if err != nil {
		return "", err
	}

	if c.protocol == config.ProtocolDebezium {
		// debezium use topic name as avro schema namespace
		top.Namespace = getDebeziumAvroNamespace(topic)
		top.ConnectName = top.Namespace + ".Key"
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: key to schema", zap.ByteString("schema", str))
	return string(str), nil
}

func (c *Codec) value2AvroSchema(
	topic string,
	tableName *model.TableName,
	input *avroEncodeInput,
) (string, error) {
	if c.config.EnableRowChecksum {
		sort.Sort(input)
	}

	top, err := c.columns2AvroSchema(tableName, input)
	if err != nil {
		return "", err
	}

	if c.protocol == config.ProtocolDebezium {
		// debezium use topic name as avro schema namespace
		top.Namespace = getDebeziumAvroNamespace(topic)
		top.ConnectName = top.Namespace + ".Envelope"

		top = schemaWithDebezium(top)
	}

	if c.config.EnableTiDBExtension {
		top = c.schemaWithExtension(top)
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: row to schema",
		zap.ByteString("schema", str),
		zap.Bool("enableTiDBExtension", c.config.EnableRowChecksum),
		zap.Bool("enableRowLevelChecksum", c.config.EnableRowChecksum))
	return string(str), nil
}

func (c *Codec) columns2AvroSchema(
	tableName *model.TableName,
	input *avroEncodeInput,
) (*avroSchemaTop, error) {
	top := &avroSchemaTop{
		Tp:        "record",
		Name:      sanitizeName(tableName.Table),
		Namespace: getAvroNamespace(c.namespace, tableName.Schema),
		Fields:    nil,
	}
	for i, col := range input.columns {
		if col == nil {
			continue
		}
		avroType, err := c.columnToAvroSchema(col, input.colInfos[i].Ft)
		if err != nil {
			return nil, err
		}
		field := make(map[string]interface{})
		field["name"] = sanitizeName(col.Name)

		copied := *col
		copied.Value = copied.Default
		defaultValue, _, err := c.columnToAvroData(&copied, input.colInfos[i].Ft)
		if err != nil {
			log.Error("fail to get default value for avro schema")
			return nil, errors.Trace(err)
		}
		// goavro doesn't support set default value for logical type
		// https://github.com/linkedin/goavro/issues/202
		if _, ok := avroType.(avroLogicalTypeSchema); ok {
			if col.Flag.IsNullable() {
				field["type"] = []interface{}{"null", avroType}
				field["default"] = nil
			} else {
				field["type"] = avroType
			}
		} else {
			if col.Flag.IsNullable() {
				// https://stackoverflow.com/questions/22938124/avro-field-default-values
				if defaultValue == nil {
					field["type"] = []interface{}{"null", avroType}
				} else {
					field["type"] = []interface{}{avroType, "null"}
				}
				field["default"] = defaultValue
			} else {
				field["type"] = avroType
				if defaultValue != nil {
					field["default"] = defaultValue
				}
			}
		}
		top.Fields = append(top.Fields, field)
	}
	return top, nil
}

func (c *Codec) columns2AvroData(
	input *avroEncodeInput,
) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(input.columns))
	for i, col := range input.columns {
		if col == nil {
			continue
		}
		data, str, err := c.columnToAvroData(col, input.colInfos[i].Ft)
		if err != nil {
			return nil, err
		}

		// https: //pkg.go.dev/github.com/linkedin/goavro/v2#Union
		if col.Flag.IsNullable() {
			ret[sanitizeName(col.Name)] = goavro.Union(str, data)
		} else {
			ret[sanitizeName(col.Name)] = data
		}
	}

	log.Debug("rowToAvroData", zap.Any("data", ret))
	return ret, nil
}

func (c *Codec) columns2DebeziumAvroData(
	topic string,
	e *model.RowChangedEvent,
) (map[string]interface{}, error) {
	var err error
	envelope := make(map[string]interface{})
	ret := make(map[string]interface{})
	// debezium use topic name as avro schema namespace
	unionKey := getDebeziumAvroNamespace(topic) + ".Value"
	colInfos := e.TableInfo.GetColInfosForRowChangedEvent()
	op := GetOperation(e)

	switch op {
	case insertOperation:
		ret[debeziumBefore] = nil
		input := &avroEncodeInput{
			columns:  e.GetColumns(),
			colInfos: colInfos,
		}
		ret, err = c.columns2AvroData(input)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDebeziumAfterAvroEncodeFailed, err)
		}
		envelope[debeziumAfter] = goavro.Union(unionKey, ret)
		break
	case updateOperation:
		// set before field value
		input := &avroEncodeInput{
			columns:  e.GetPreColumns(),
			colInfos: colInfos,
		}
		ret, err = c.columns2AvroData(input)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDebeziumBeforeAvroEncodeFailed, err)
		}
		envelope[debeziumBefore] = goavro.Union(unionKey, ret)
		// set after field value
		input = &avroEncodeInput{
			columns:  e.GetColumns(),
			colInfos: colInfos,
		}
		ret, err = c.columns2AvroData(input)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDebeziumAfterAvroEncodeFailed, err)
		}
		envelope[debeziumAfter] = goavro.Union(unionKey, ret)
		break
	case deleteOperation:
		input := &avroEncodeInput{
			columns:  e.GetPreColumns(),
			colInfos: colInfos,
		}
		ret, err = c.columns2AvroData(input)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrDebeziumBeforeAvroEncodeFailed, err)
		}
		envelope[debeziumBefore] = goavro.Union(unionKey, ret)
		envelope[debeziumAfter] = nil
		break
	default:
		// do nothing
	}

	sourceValue := make(map[string]interface{})
	sourceValue[debeziumSourceCdc] = "ticdc"
	sourceValue[debeziumSourceVersion] = version.ReleaseVersion
	sourceValue[debeziumSourceChaneFeedId] = goavro.Union("string", c.changeFeedID)
	sourceValue[debeziumSourceTsMs] = oracle.ExtractPhysical(e.CommitTs)
	sourceValue[debeziumSourceDB] = e.TableInfo.GetSchemaName()
	sourceValue[debeziumSourceTable] = goavro.Union("string", e.TableInfo.GetTableName())
	sourceValue[debeziumSourceCommitTS] = int64(e.CommitTs)
	envelope[debeziumSource] = sourceValue
	envelope[debeziumOp] = op
	envelope[debeziumTsMs] = goavro.Union("long", time.Now().UnixMilli())

	log.Debug("rowToAvroData", zap.Any("data", envelope))
	return envelope, nil
}

func (c *Codec) columnToAvroSchema(
	col *model.Column,
	ft *types.FieldType,
) (interface{}, error) {
	tt := getTiDBTypeFromColumn(col)
	switch col.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		// BOOL/TINYINT/SMALLINT/MEDIUMINT
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeLong: // INT
		if col.Flag.IsUnsigned() {
			return avroSchema{
				Type:       "long",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeLonglong: // BIGINT
		t := "long"
		if col.Flag.IsUnsigned() &&
			c.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
			t = "string"
		}
		return avroSchema{
			Type:       t,
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeFloat:
		return avroSchema{
			Type:       "float",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeDouble:
		return avroSchema{
			Type:       "double",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeBit:
		displayFlen := ft.GetFlen()
		if displayFlen == -1 {
			displayFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(col.Type)
		}
		return avroSchema{
			Type: "bytes",
			Parameters: map[string]string{
				tidbType: tt,
				"length": strconv.Itoa(displayFlen),
			},
		}, nil
	case mysql.TypeNewDecimal:
		if c.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
			defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
			displayFlen, displayDecimal := ft.GetFlen(), ft.GetDecimal()
			// length not specified, set it to system type default
			if displayFlen == -1 {
				displayFlen = defaultFlen
			}
			if displayDecimal == -1 {
				displayDecimal = defaultDecimal
			}
			return avroLogicalTypeSchema{
				avroSchema: avroSchema{
					Type:       "bytes",
					Parameters: map[string]string{tidbType: tt},
				},
				LogicalType: "decimal",
				Precision:   displayFlen,
				Scale:       displayDecimal,
			}, nil
		}
		// decimalHandlingMode == string
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	// TINYTEXT/MEDIUMTEXT/TEXT/LONGTEXT/CHAR/VARCHAR
	// TINYBLOB/MEDIUMBLOB/BLOB/LONGBLOB/BINARY/VARBINARY
	case mysql.TypeVarchar,
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob,
		mysql.TypeBlob:
		t := "string"
		if col.Flag.IsBinary() {
			t = "bytes"
		}
		return avroSchema{
			Type:       t,
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeEnum, mysql.TypeSet:
		es := make([]string, 0, len(ft.GetElems()))
		for _, e := range ft.GetElems() {
			e = escapeEnumAndSetOptions(e)
			es = append(es, e)
		}
		return avroSchema{
			Type: "string",
			Parameters: map[string]string{
				tidbType:  tt,
				"allowed": strings.Join(es, ","),
			},
		}, nil
	case mysql.TypeJSON:
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeTimestamp:
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeDate:
		if c.config.AvroTimePrecisionHandlingMode == common.TimePrecisionHandlingModeAdaptiveTimeMicroseconds {
			return avroSchema{
				Type:       "int",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeDatetime, mysql.TypeDuration:
		if c.config.AvroTimePrecisionHandlingMode == common.TimePrecisionHandlingModeAdaptiveTimeMicroseconds {
			return avroSchema{
				Type:       "long",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeYear:
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	default:
		log.Error("unknown mysql type", zap.Any("mysqlType", col.Type))
		return nil, cerror.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

func (c *Codec) columnToAvroData(
	col *model.Column,
	ft *types.FieldType,
) (interface{}, string, error) {
	if col.Value == nil {
		return nil, "null", nil
	}

	switch col.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		if v, ok := col.Value.(string); ok {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			return int32(n), "int", nil
		}
		if col.Flag.IsUnsigned() {
			return int32(col.Value.(uint64)), "int", nil
		}
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLong:
		if v, ok := col.Value.(string); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			if col.Flag.IsUnsigned() {
				return n, "long", nil
			}
			return int32(n), "int", nil
		}
		if col.Flag.IsUnsigned() {
			return int64(col.Value.(uint64)), "long", nil
		}
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLonglong:
		if v, ok := col.Value.(string); ok {
			if col.Flag.IsUnsigned() {
				if c.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
					return v, "string", nil
				}
				n, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
				}
				return int64(n), "long", nil
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			return n, "long", nil
		}
		if col.Flag.IsUnsigned() {
			if c.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeLong {
				return int64(col.Value.(uint64)), "long", nil
			}
			// bigintUnsignedHandlingMode == "string"
			return strconv.FormatUint(col.Value.(uint64), 10), "string", nil
		}
		return col.Value.(int64), "long", nil
	case mysql.TypeFloat:
		if v, ok := col.Value.(string); ok {
			n, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			return n, "float", nil
		}
		return col.Value.(float32), "float", nil
	case mysql.TypeDouble:
		if v, ok := col.Value.(string); ok {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			return n, "double", nil
		}
		return col.Value.(float64), "double", nil
	case mysql.TypeBit:
		if v, ok := col.Value.(string); ok {
			return []byte(v), "bytes", nil
		}
		return []byte(types.NewBinaryLiteralFromUint(col.Value.(uint64), -1)), "bytes", nil
	case mysql.TypeNewDecimal:
		if c.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
			v, succ := new(big.Rat).SetString(col.Value.(string))
			if !succ {
				return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack(
					"fail to encode Decimal value",
				)
			}
			return v, "bytes.decimal", nil
		}
		// decimalHandlingMode == "string"
		return col.Value.(string), "string", nil
	case mysql.TypeVarchar,
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob:
		if col.Flag.IsBinary() {
			if v, ok := col.Value.(string); ok {
				return []byte(v), "bytes", nil
			}
			return col.Value, "bytes", nil
		}
		if v, ok := col.Value.(string); ok {
			return v, "string", nil
		}
		return string(col.Value.([]byte)), "string", nil
	case mysql.TypeEnum:
		if v, ok := col.Value.(string); ok {
			return v, "string", nil
		}
		elements := ft.GetElems()
		number := col.Value.(uint64)
		enumVar, err := types.ParseEnumValue(elements, number)
		if err != nil {
			log.Info("avro encoder parse enum value failed", zap.Strings("elements", elements), zap.Uint64("number", number))
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return enumVar.Name, "string", nil
	case mysql.TypeSet:
		if v, ok := col.Value.(string); ok {
			return v, "string", nil
		}
		elements := ft.GetElems()
		number := col.Value.(uint64)
		setVar, err := types.ParseSetValue(elements, number)
		if err != nil {
			log.Info("avro encoder parse set value failed",
				zap.Strings("elements", elements), zap.Uint64("number", number), zap.Error(err))
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return setVar.Name, "string", nil
	case mysql.TypeJSON:
		return col.Value.(string), "string", nil
	case mysql.TypeTimestamp:
		return col.Value.(string), "string", nil
	case mysql.TypeDate:
		if c.config.AvroTimePrecisionHandlingMode == common.TimePrecisionHandlingModeAdaptiveTimeMicroseconds {
			t, err := time.Parse("2006-01-02", col.Value.(string))
			if err != nil {
				return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack("fail to encode Date value", err)
			}
			return int32(t.Unix() / 86400), "int", nil
		}
		return col.Value.(string), "string", nil
	case mysql.TypeDatetime:
		if c.config.AvroTimePrecisionHandlingMode == common.TimePrecisionHandlingModeAdaptiveTimeMicroseconds {
			origin := col.Value.(string)
			var t time.Time
			var err error
			if origin == "CURRENT_TIMESTAMP" {
				t = time.Now()
			} else {
				t, err = time.Parse("2006-01-02 15:04:05", origin)
				if err != nil {
					return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack("fail to encode Datetime value: %s ", origin, err)
				}
			}
			return t.UnixMilli(), "long", nil
		}
		return col.Value.(string), "string", nil
	case mysql.TypeDuration:
		if c.config.AvroTimePrecisionHandlingMode == common.TimePrecisionHandlingModeAdaptiveTimeMicroseconds {
			t, err := time.Parse("15:04:05", col.Value.(string))
			if err != nil {
				return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack("fail to encode Time value", err)
			}
			midnight, _ := time.Parse("15:04:05", "00:00:00")
			return t.Sub(midnight).Milliseconds(), "long", nil
		}
		return col.Value.(string), "string", nil
	case mysql.TypeYear:
		if v, ok := col.Value.(string); ok {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				log.Info("avro encoder parse year value failed", zap.String("value", v), zap.Error(err))
				return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
			}
			return int32(n), "int", nil
		}
		return int32(col.Value.(int64)), "int", nil
	default:
		log.Error("unknown mysql type", zap.Any("value", col.Value), zap.Any("mysqlType", col.Type))
		return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

func (c *Codec) schemaWithExtension(
	top *avroSchemaTop,
) *avroSchemaTop {
	top.Fields = append(top.Fields,
		map[string]interface{}{
			"name":    tidbOp,
			"type":    "string",
			"default": "",
		},
		map[string]interface{}{
			"name":    tidbCommitTs,
			"type":    "long",
			"default": 0,
		},
		map[string]interface{}{
			"name":    tidbPhysicalTime,
			"type":    "long",
			"default": 0,
		},
	)

	if c.config.EnableRowChecksum {
		top.Fields = append(top.Fields,
			map[string]interface{}{
				"name":    tidbRowLevelChecksum,
				"type":    "string",
				"default": "",
			},
			map[string]interface{}{
				"name":    tidbCorrupted,
				"type":    "boolean",
				"default": false,
			},
			map[string]interface{}{
				"name":    tidbChecksumVersion,
				"type":    "int",
				"default": 0,
			})
	}

	return top
}

func schemaWithDebezium(
	top *avroSchemaTop,
) *avroSchemaTop {
	fields := make([]map[string]interface{}, 0)
	// debezium before
	beforeFields := avroSchemaTop{
		Tp:          "record",
		Name:        "Value",
		Fields:      top.Fields,
		ConnectName: top.Namespace + ".Value",
	}
	before := make(map[string]interface{})
	before["name"] = debeziumBefore
	before["type"] = []interface{}{"null", beforeFields}
	before["default"] = nil
	// debezium after
	after := make(map[string]interface{})
	after["name"] = debeziumAfter
	after["type"] = []interface{}{"null", "Value"}
	after["default"] = nil
	// debezium source
	source := make(map[string]interface{})
	source["name"] = debeziumSource
	source["type"] = debeziumSourceAvroSchema()
	// debezium op for event type
	op := make(map[string]interface{})
	op["name"] = debeziumOp
	op["type"] = "string"
	// debezium tsms for processing time
	tsms := make(map[string]interface{})
	tsms["name"] = debeziumTsMs
	tsms["type"] = []string{"null", "long"}
	tsms["default"] = nil
	// debezium fields
	top.Fields = append(fields, before, after, source, op, tsms)

	return top
}

func debeziumSourceAvroSchema() avroSchemaTop {
	sourceFields := make([]map[string]interface{}, 0)

	cdcField := make(map[string]interface{})
	cdcField["name"] = debeziumSourceCdc
	cdcField["type"] = "string"

	versionField := make(map[string]interface{})
	versionField["name"] = debeziumSourceVersion
	versionField["type"] = "string"

	changeFeedIDField := make(map[string]interface{})
	changeFeedIDField["name"] = debeziumSourceChaneFeedId
	changeFeedIDField["type"] = []string{"null", "string"}
	changeFeedIDField["default"] = nil

	tsmsColumns := make(map[string]interface{})
	tsmsColumns["name"] = debeziumSourceTsMs
	tsmsColumns["type"] = "long"

	dbField := make(map[string]interface{})
	dbField["name"] = debeziumSourceDB
	dbField["type"] = "string"

	tableField := make(map[string]interface{})
	tableField["name"] = debeziumSourceTable
	tableField["type"] = []string{"null", "string"}
	tableField["default"] = nil

	commitTSField := make(map[string]interface{})
	commitTSField["name"] = debeziumSourceCommitTS
	commitTSField["type"] = "long"

	sourceFields = append(
		sourceFields, cdcField, versionField, changeFeedIDField, tsmsColumns, dbField, tableField, commitTSField)

	return avroSchemaTop{
		Tp:          "record",
		Name:        "Source",
		Fields:      sourceFields,
		ConnectName: "ticdc.Source",
	}
}

var type2TiDBType = map[byte]string{
	mysql.TypeTiny:       "INT",
	mysql.TypeShort:      "INT",
	mysql.TypeInt24:      "INT",
	mysql.TypeLong:       "INT",
	mysql.TypeLonglong:   "BIGINT",
	mysql.TypeFloat:      "FLOAT",
	mysql.TypeDouble:     "DOUBLE",
	mysql.TypeBit:        "BIT",
	mysql.TypeNewDecimal: "DECIMAL",
	mysql.TypeTinyBlob:   "TEXT",
	mysql.TypeMediumBlob: "TEXT",
	mysql.TypeBlob:       "TEXT",
	mysql.TypeLongBlob:   "TEXT",
	mysql.TypeVarchar:    "TEXT",
	mysql.TypeVarString:  "TEXT",
	mysql.TypeString:     "TEXT",
	mysql.TypeEnum:       "ENUM",
	mysql.TypeSet:        "SET",
	mysql.TypeJSON:       "JSON",
	mysql.TypeDate:       "DATE",
	mysql.TypeDatetime:   "DATETIME",
	mysql.TypeTimestamp:  "TIMESTAMP",
	mysql.TypeDuration:   "TIME",
	mysql.TypeYear:       "YEAR",
}

func getTiDBTypeFromColumn(col *model.Column) string {
	tt := type2TiDBType[col.Type]
	if col.Flag.IsUnsigned() && (tt == "INT" || tt == "BIGINT") {
		return tt + " UNSIGNED"
	}
	if col.Flag.IsBinary() && tt == "TEXT" {
		return "BLOB"
	}
	return tt
}

// sanitizeName escapes not permitted chars for avro
// debezium-core/src/main/java/io/debezium/schema/FieldNameSelector.java
// https://avro.apache.org/docs/current/spec.html#names
func sanitizeName(name string) string {
	return sanitize(name, true)
}

func sanitize(name string, sanitizedDot bool) string {
	changed := false
	var sb strings.Builder
	for i, c := range name {
		if i == 0 && (c >= '0' && c <= '9') {
			sb.WriteString(numberPrefix)
			sb.WriteRune(c)
			changed = true
		} else if c == '.' {
			if sanitizedDot {
				sb.WriteString(replacementChar)
				changed = true
			} else {
				sb.WriteRune(c)
			}
		} else if !(c == '_' ||
			('a' <= c && c <= 'z') ||
			('A' <= c && c <= 'Z') ||
			('0' <= c && c <= '9')) {
			sb.WriteString(replacementChar)
			changed = true
		} else {
			sb.WriteRune(c)
		}
	}

	sanitizedName := sb.String()
	if changed {
		log.Warn(
			"Name is potentially not safe for serialization, replace it",
			zap.String("name", name),
			zap.String("replacedName", sanitizedName),
		)
	}
	return sanitizedName
}

// sanitizeTopic escapes ".", it may have special meanings for sink connectors
func sanitizeTopic(name string) string {
	return strings.ReplaceAll(name, ".", replacementChar)
}

// https://github.com/debezium/debezium/blob/9f7ede0e0695f012c6c4e715e96aed85eecf6b5f \
// /debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/antlr/ \
// MySqlAntlrDdlParser.java#L374
func escapeEnumAndSetOptions(option string) string {
	option = strings.ReplaceAll(option, ",", "\\,")
	option = strings.ReplaceAll(option, "\\'", "'")
	option = strings.ReplaceAll(option, "''", "'")
	return option
}

func getAvroNamespace(namespace string, schema string) string {
	return sanitizeName(namespace) + "." + sanitizeName(schema)
}

func getDebeziumAvroNamespace(name string) string {
	return sanitize(name, false)
}

func topicName2SchemaSubjects(topicName, subjectSuffix string) string {
	return topicName + subjectSuffix
}
