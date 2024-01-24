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

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// BatchEncoder converts the events to binary Avro data
type BatchEncoder struct {
	namespace string
	schemaM   SchemaManager
	result    []*common.Message

	config *common.Config
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

func (a *BatchEncoder) encodeKey(ctx context.Context, topic string, e *model.RowChangedEvent) ([]byte, error) {
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
	avroCodec, header, err := a.getKeySchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.Version, keyColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := a.columns2AvroData(keyColumns)
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

func topicName2SchemaSubjects(topicName, subjectSuffix string) string {
	return topicName + subjectSuffix
}

func (a *BatchEncoder) getValueSchemaCodec(
	ctx context.Context, topic string, tableName *model.TableName, tableVersion uint64, input *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := a.value2AvroSchema(tableName, input)
		if err != nil {
			log.Error("avro: generating value schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, valueSchemaSuffix)
	avroCodec, header, err := a.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (a *BatchEncoder) getKeySchemaCodec(
	ctx context.Context, topic string, tableName *model.TableName, tableVersion uint64, keyColumns *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := a.key2AvroSchema(tableName, keyColumns)
		if err != nil {
			log.Error("AvroEventBatchEncoder: generating key schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, keySchemaSuffix)
	avroCodec, header, err := a.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (a *BatchEncoder) encodeValue(ctx context.Context, topic string, e *model.RowChangedEvent) ([]byte, error) {
	if e.IsDelete() {
		return nil, nil
	}

	input := &avroEncodeInput{
		columns:  e.Columns,
		colInfos: e.ColInfos,
	}
	if len(input.columns) == 0 {
		return nil, nil
	}

	avroCodec, header, err := a.getValueSchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.Version, input)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := a.columns2AvroData(input)
	if err != nil {
		log.Error("avro: converting value to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if a.config.EnableTiDBExtension {
		native = a.nativeValueWithExtension(native, e)
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

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	topic = sanitizeTopic(topic)

	key, err := a.encodeKey(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding key failed", zap.Error(err))
		return errors.Trace(err)
	}

	value, err := a.encodeValue(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding value failed", zap.Error(err))
		return errors.Trace(err)
	}

	message := common.NewMsg(
		config.ProtocolAvro,
		key,
		value,
		e.CommitTs,
		model.MessageTypeRow,
		e.TableInfo.GetSchemaNamePtr(),
		e.TableInfo.GetTableNamePtr(),
	)
	message.Callback = callback
	message.IncRowsCount()

	if message.Length() > a.config.MaxMessageBytes {
		log.Warn("Single message is too large for avro",
			zap.Int("maxMessageBytes", a.config.MaxMessageBytes),
			zap.Int("length", message.Length()),
			zap.Any("table", e.TableInfo.TableName))
		return cerror.ErrMessageTooLarge.GenWithStackByArgs(message.Length())
	}

	a.result = append(a.result, message)
	return nil
}

// EncodeCheckpointEvent only encode checkpoint event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		data := []interface{}{checkpointByte, ts}
		for _, v := range data {
			err := binary.Write(buf, binary.BigEndian, v)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
			}
		}

		value := buf.Bytes()
		return common.NewResolvedMsg(config.ProtocolAvro, nil, value, ts), nil
	}
	return nil, nil
}

type ddlEvent struct {
	Query    string             `json:"query"`
	Type     timodel.ActionType `json:"type"`
	Schema   string             `json:"schema"`
	Table    string             `json:"table"`
	CommitTs uint64             `json:"commitTs"`
}

// EncodeDDLEvent only encode DDL event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.BigEndian, ddlByte)

		event := &ddlEvent{
			Query:    e.Query,
			Type:     e.Type,
			Schema:   e.TableInfo.TableName.Schema,
			Table:    e.TableInfo.TableName.Table,
			CommitTs: e.CommitTs,
		}
		data, err := json.Marshal(event)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
		buf.Write(data)

		value := buf.Bytes()
		return common.NewDDLMsg(config.ProtocolAvro, nil, value, e), nil
	}

	return nil, nil
}

// Build Messages
func (a *BatchEncoder) Build() (messages []*common.Message) {
	result := a.result
	a.result = nil
	return result
}

const (
	insertOperation = "c"
	updateOperation = "u"
)

func getOperation(e *model.RowChangedEvent) string {
	if e.IsInsert() {
		return insertOperation
	} else if e.IsUpdate() {
		return updateOperation
	}
	return ""
}

func (a *BatchEncoder) nativeValueWithExtension(
	native map[string]interface{},
	e *model.RowChangedEvent,
) map[string]interface{} {
	native[tidbOp] = getOperation(e)
	native[tidbCommitTs] = int64(e.CommitTs)
	native[tidbPhysicalTime] = oracle.ExtractPhysical(e.CommitTs)

	if a.config.EnableRowChecksum && e.Checksum != nil {
		native[tidbRowLevelChecksum] = strconv.FormatUint(uint64(e.Checksum.Current), 10)
		native[tidbCorrupted] = e.Checksum.Corrupted
		native[tidbChecksumVersion] = e.Checksum.Version
	}
	return native
}

type avroSchemaTop struct {
	Tp        string                   `json:"type"`
	Name      string                   `json:"name"`
	Namespace string                   `json:"namespace"`
	Fields    []map[string]interface{} `json:"fields"`
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
)

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

func flagFromTiDBType(tp string) model.ColumnFlagType {
	var flag model.ColumnFlagType
	if strings.Contains(tp, "UNSIGNED") {
		flag.SetIsUnsigned()
	}
	return flag
}

func mysqlTypeFromTiDBType(tidbType string) byte {
	var result byte
	switch tidbType {
	case "INT", "INT UNSIGNED":
		result = mysql.TypeLong
	case "BIGINT", "BIGINT UNSIGNED":
		result = mysql.TypeLonglong
	case "FLOAT":
		result = mysql.TypeFloat
	case "DOUBLE":
		result = mysql.TypeDouble
	case "BIT":
		result = mysql.TypeBit
	case "DECIMAL":
		result = mysql.TypeNewDecimal
	case "TEXT":
		result = mysql.TypeVarchar
	case "BLOB":
		result = mysql.TypeLongBlob
	case "ENUM":
		result = mysql.TypeEnum
	case "SET":
		result = mysql.TypeSet
	case "JSON":
		result = mysql.TypeJSON
	case "DATE":
		result = mysql.TypeDate
	case "DATETIME":
		result = mysql.TypeDatetime
	case "TIMESTAMP":
		result = mysql.TypeTimestamp
	case "TIME":
		result = mysql.TypeDuration
	case "YEAR":
		result = mysql.TypeYear
	default:
		log.Panic("this should not happen, unknown TiDB type", zap.String("type", tidbType))
	}
	return result
}

const (
	replacementChar = "_"
	numberPrefix    = "_"
)

// sanitizeName escapes not permitted chars for avro
// debezium-core/src/main/java/io/debezium/schema/FieldNameSelector.java
// https://avro.apache.org/docs/current/spec.html#names
func sanitizeName(name string) string {
	changed := false
	var sb strings.Builder
	for i, c := range name {
		if i == 0 && (c >= '0' && c <= '9') {
			sb.WriteString(numberPrefix)
			sb.WriteRune(c)
			changed = true
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

func (a *BatchEncoder) schemaWithExtension(
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

	if a.config.EnableRowChecksum {
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

func (a *BatchEncoder) columns2AvroSchema(
	tableName *model.TableName,
	input *avroEncodeInput,
) (*avroSchemaTop, error) {
	top := &avroSchemaTop{
		Tp:        "record",
		Name:      sanitizeName(tableName.Table),
		Namespace: getAvroNamespace(a.namespace, tableName.Schema),
		Fields:    nil,
	}
	for i, col := range input.columns {
		if col == nil {
			continue
		}
		avroType, err := a.columnToAvroSchema(col, input.colInfos[i].Ft)
		if err != nil {
			return nil, err
		}
		field := make(map[string]interface{})
		field["name"] = sanitizeName(col.Name)

		copied := *col
		copied.Value = copied.Default
		defaultValue, _, err := a.columnToAvroData(&copied, input.colInfos[i].Ft)
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

func (a *BatchEncoder) value2AvroSchema(
	tableName *model.TableName,
	input *avroEncodeInput,
) (string, error) {
	if a.config.EnableRowChecksum {
		sort.Sort(input)
	}

	top, err := a.columns2AvroSchema(tableName, input)
	if err != nil {
		return "", err
	}

	if a.config.EnableTiDBExtension {
		top = a.schemaWithExtension(top)
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: row to schema",
		zap.ByteString("schema", str),
		zap.Bool("enableTiDBExtension", a.config.EnableRowChecksum),
		zap.Bool("enableRowLevelChecksum", a.config.EnableRowChecksum))
	return string(str), nil
}

func (a *BatchEncoder) key2AvroSchema(
	tableName *model.TableName,
	keyColumns *avroEncodeInput,
) (string, error) {
	top, err := a.columns2AvroSchema(tableName, keyColumns)
	if err != nil {
		return "", err
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: key to schema", zap.ByteString("schema", str))
	return string(str), nil
}

func (a *BatchEncoder) columns2AvroData(
	input *avroEncodeInput,
) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(input.columns))
	for i, col := range input.columns {
		if col == nil {
			continue
		}
		data, str, err := a.columnToAvroData(col, input.colInfos[i].Ft)
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

func (a *BatchEncoder) columnToAvroSchema(
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
			a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
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
		if a.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
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
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
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

func (a *BatchEncoder) columnToAvroData(
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
				if a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
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
			if a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeLong {
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
		if a.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
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
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
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

const (
	// avro does not send ddl and checkpoint message, the following 2 field is used to distinguish
	// TiCDC DDL event and checkpoint event, only used for testing purpose, not for production
	ddlByte        = uint8(1)
	checkpointByte = uint8(2)
)

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

type batchEncoderBuilder struct {
	namespace string
	config    *common.Config
	schemaM   SchemaManager
}

const (
	keySchemaSuffix   = "-key"
	valueSchemaSuffix = "-value"
)

// NewBatchEncoderBuilder creates an avro batchEncoderBuilder.
func NewBatchEncoderBuilder(
	ctx context.Context, config *common.Config,
) (codec.RowEventEncoderBuilder, error) {
	var schemaM SchemaManager
	var err error

	schemaRegistryType := config.SchemaRegistryType()
	switch schemaRegistryType {
	case common.SchemaRegistryTypeConfluent:
		schemaM, err = NewConfluentSchemaManager(ctx, config.AvroConfluentSchemaRegistry, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case common.SchemaRegistryTypeGlue:
		schemaM, err = NewGlueSchemaManager(ctx, config.AvroGlueSchemaRegistry)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs(schemaRegistryType)
	}

	return &batchEncoderBuilder{
		namespace: config.ChangefeedID.Namespace,
		config:    config,
		schemaM:   schemaM,
	}, nil
}

// Build an AvroEventBatchEncoder.
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return NewAvroEncoder(b.namespace, b.schemaM, b.config)
}

// CleanMetrics is a no-op for AvroEventBatchEncoder.
func (b *batchEncoderBuilder) CleanMetrics() {}

// NewAvroEncoder return a avro encoder.
func NewAvroEncoder(namespace string, schemaM SchemaManager, config *common.Config) codec.RowEventEncoder {
	return &BatchEncoder{
		namespace: namespace,
		schemaM:   schemaM,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}
}

// SetupEncoderAndSchemaRegistry4Testing start a local schema registry for testing.
func SetupEncoderAndSchemaRegistry4Testing(
	ctx context.Context,
	config *common.Config,
) (*BatchEncoder, error) {
	startHTTPInterceptForTestingRegistry()
	schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BatchEncoder{
		namespace: model.DefaultNamespace,
		schemaM:   schemaM,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}, nil
}

// TeardownEncoderAndSchemaRegistry4Testing stop the local schema registry for testing.
func TeardownEncoderAndSchemaRegistry4Testing() {
	stopHTTPInterceptForTestingRegistry()
}
