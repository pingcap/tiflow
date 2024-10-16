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
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/contextutil"
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
	namespace          string
	keySchemaManager   *schemaManager
	valueSchemaManager *schemaManager
	result             []*common.Message
	config             *common.Config
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
	data       []byte
	registryID int
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	message := common.NewMsg(
		config.ProtocolAvro,
		nil,
		nil,
		e.CommitTs,
		model.MessageTypeRow,
		&e.Table.Schema,
		&e.Table.Table,
	)
	message.Callback = callback
	topic = sanitizeTopic(topic)

	if !e.IsDelete() {
		res, err := a.avroEncode(ctx, e, topic, false)
		if err != nil {
			log.Error("AppendRowChangedEvent: avro encoding failed", zap.Error(err), zap.Any("event", e))
			return errors.Trace(err)
		}

		evlp, err := res.toEnvelope()
		if err != nil {
			log.Error("AppendRowChangedEvent: could not construct Avro envelope", zap.Error(err))
			return errors.Trace(err)
		}

		message.Value = evlp
	} else {
		message.Value = nil
	}

	res, err := a.avroEncode(ctx, e, topic, true)
	if err != nil {
		log.Error("AppendRowChangedEvent: avro encoding failed", zap.Error(err), zap.Any("event", e))
		return errors.Trace(err)
	}

	if res != nil {
		evlp, err := res.toEnvelope()
		if err != nil {
			log.Error("AppendRowChangedEvent: could not construct Avro envelope", zap.Error(err))
			return errors.Trace(err)
		}
		message.Key = evlp
	} else {
		message.Key = nil
	}
	message.IncRowsCount()
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

func (a *BatchEncoder) avroEncode(
	ctx context.Context,
	e *model.RowChangedEvent,
	topic string,
	isKey bool,
) (*avroEncodeResult, error) {
	var (
		input *avroEncodeInput

		cols                   []*model.Column
		colInfos               []rowcodec.ColInfo
		enableTiDBExtension    bool
		enableRowLevelChecksum bool
		schemaManager          *schemaManager
		operation              string
	)
	if isKey {
		cols, colInfos = e.HandleKeyColInfos()
		input = &avroEncodeInput{
			columns:  cols,
			colInfos: colInfos,
		}
		enableTiDBExtension = false
		enableRowLevelChecksum = false
		schemaManager = a.keySchemaManager
	} else {
		input = &avroEncodeInput{
			columns:  e.Columns,
			colInfos: e.ColInfos,
		}

		enableTiDBExtension = a.config.EnableTiDBExtension
		enableRowLevelChecksum = a.config.EnableRowChecksum
		schemaManager = a.valueSchemaManager
		if e.IsInsert() {
			operation = insertOperation
		} else if e.IsUpdate() {
			operation = updateOperation
		} else {
			log.Error("unknown operation", zap.Any("rowChangedEvent", e))
			return nil, cerror.ErrAvroEncodeFailed.GenWithStack("unknown operation")
		}
	}

	if len(input.columns) == 0 {
		return nil, nil
	}

	namespace := getAvroNamespace(a.namespace, e.Table)

	schemaGen := func() (string, error) {
		schema, err := rowToAvroSchema(
			namespace,
			e.Table.Table,
			input,
			enableTiDBExtension,
			enableRowLevelChecksum,
			a.config.AvroDecimalHandlingMode,
			a.config.AvroBigintUnsignedHandlingMode,
		)
		if err != nil {
			log.Error("AvroEventBatchEncoder: generating schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	avroCodec, registryID, err := schemaManager.GetCachedOrRegister(
		ctx,
		topic,
		e.TableInfo.Version,
		schemaGen,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := rowToAvroData(
		input,
		e.CommitTs,
		operation,
		enableTiDBExtension,
		a.config.AvroDecimalHandlingMode,
		a.config.AvroBigintUnsignedHandlingMode,
	)
	if err != nil {
		log.Error("AvroEventBatchEncoder: converting to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}

	if enableRowLevelChecksum && enableTiDBExtension && e.Checksum != nil {
		native[tidbRowLevelChecksum] = strconv.FormatUint(uint64(e.Checksum.Current), 10)
		native[tidbCorrupted] = e.Checksum.Corrupted
		native[tidbChecksumVersion] = e.Checksum.Version
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		log.Error("AvroEventBatchEncoder: converting to Avro binary failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroEncodeToBinary, err)
	}

	return &avroEncodeResult{
		data:       bin,
		registryID: registryID,
	}, nil
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

// https://github.com/debezium/debezium/blob/9f7ede0e0695f012c6c4e715e96aed85eecf6b5f \
// /debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/antlr/ \
// MySqlAntlrDdlParser.java#L374
func escapeEnumAndSetOptions(option string) string {
	option = strings.ReplaceAll(option, ",", "\\,")
	option = strings.ReplaceAll(option, "\\'", "'")
	option = strings.ReplaceAll(option, "''", "'")
	return option
}

func getAvroNamespace(namespace string, tableName *model.TableName) string {
	return sanitizeName(namespace) + "." + sanitizeName(tableName.Schema)
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

func rowToAvroSchema(
	namespace string,
	name string,
	input *avroEncodeInput,
	enableTiDBExtension bool,
	enableRowLevelChecksum bool,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
) (string, error) {
	if enableRowLevelChecksum {
		sort.Sort(input)
	}

	top := avroSchemaTop{
		Tp:        "record",
		Name:      sanitizeName(name),
		Namespace: namespace,
		Fields:    nil,
	}

	for i, col := range input.columns {
		avroType, err := columnToAvroSchema(
			col,
			input.colInfos[i].Ft,
			decimalHandlingMode,
			bigintUnsignedHandlingMode,
		)
		if err != nil {
			return "", err
		}
		field := make(map[string]interface{})
		field["name"] = sanitizeName(col.Name)

		copy := *col
		copy.Value = copy.Default
		defaultValue, _, err := columnToAvroData(
			&copy,
			input.colInfos[i].Ft,
			decimalHandlingMode,
			bigintUnsignedHandlingMode,
		)
		if err != nil {
			log.Error("fail to get default value for avro schema")
			return "", errors.Trace(err)
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

	if enableTiDBExtension {
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

		if enableRowLevelChecksum {
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

	}

	str, err := json.Marshal(&top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Info("rowToAvroSchema",
		zap.ByteString("schema", str),
		zap.Bool("enableTiDBExtension", enableTiDBExtension),
		zap.Bool("enableRowLevelChecksum", enableRowLevelChecksum))
	return string(str), nil
}

func rowToAvroData(
	input *avroEncodeInput,
	commitTs uint64,
	operation string,
	enableTiDBExtension bool,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(input.columns))
	for i, col := range input.columns {
		if col == nil {
			continue
		}
		data, str, err := columnToAvroData(
			col,
			input.colInfos[i].Ft,
			decimalHandlingMode,
			bigintUnsignedHandlingMode,
		)
		if err != nil {
			return nil, err
		}

		// https://pkg.go.dev/github.com/linkedin/goavro/v2#Union
		if col.Flag.IsNullable() {
			ret[sanitizeName(col.Name)] = goavro.Union(str, data)
		} else {
			ret[sanitizeName(col.Name)] = data
		}
	}

	if enableTiDBExtension {
		ret[tidbOp] = operation
		ret[tidbCommitTs] = int64(commitTs)
		ret[tidbPhysicalTime] = oracle.ExtractPhysical(commitTs)
	}

	log.Debug("rowToAvroData", zap.Any("data", ret))
	return ret, nil
}

func columnToAvroSchema(
	col *model.Column,
	ft *types.FieldType,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
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
		if col.Flag.IsUnsigned() &&
			bigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
			return avroSchema{
				Type:       "string",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "long",
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
		if decimalHandlingMode == common.DecimalHandlingModePrecise {
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
		if col.Flag.IsBinary() {
			return avroSchema{
				Type:       "bytes",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "string",
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

func columnToAvroData(
	col *model.Column,
	ft *types.FieldType,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
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
				if bigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
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
			if bigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeLong {
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
		if decimalHandlingMode == common.DecimalHandlingModePrecise {
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
	// confluent avro wire format, the first byte is always 0
	// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
	magicByte = uint8(0)

	// avro does not send ddl and checkpoint message, the following 2 field is used to distinguish
	// TiCDC DDL event and checkpoint event, only used for testing purpose, not for production
	ddlByte        = uint8(1)
	checkpointByte = uint8(2)
)

// confluent avro wire format, confluent avro is not same as apache avro
// https://rmoff.net/2020/07/03/why-json-isnt-the-same-as-json-schema-in-kafka-connect-converters \
// -and-ksqldb-viewing-kafka-messages-bytes-as-hex/
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

type batchEncoderBuilder struct {
	namespace          string
	config             *common.Config
	keySchemaManager   *schemaManager
	valueSchemaManager *schemaManager
}

const (
	keySchemaSuffix   = "-key"
	valueSchemaSuffix = "-value"
)

// NewBatchEncoderBuilder creates an avro batchEncoderBuilder.
func NewBatchEncoderBuilder(ctx context.Context,
	config *common.Config,
) (codec.RowEventEncoderBuilder, error) {
	keySchemaManager, err := NewAvroSchemaManager(
		ctx,
		nil,
		config.AvroSchemaRegistry,
		keySchemaSuffix,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	valueSchemaManager, err := NewAvroSchemaManager(
		ctx,
		nil,
		config.AvroSchemaRegistry,
		valueSchemaSuffix,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &batchEncoderBuilder{
		namespace:          contextutil.ChangefeedIDFromCtx(ctx).Namespace,
		config:             config,
		keySchemaManager:   keySchemaManager,
		valueSchemaManager: valueSchemaManager,
	}, nil
}

// Build an AvroEventBatchEncoder.
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return &BatchEncoder{
		result:             make([]*common.Message, 0, 1),
		namespace:          b.namespace,
		keySchemaManager:   b.keySchemaManager,
		valueSchemaManager: b.valueSchemaManager,
		config:             b.config,
	}
}
