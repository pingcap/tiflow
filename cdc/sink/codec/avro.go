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
	"math/big"
	"strconv"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// AvroEventBatchEncoder converts the events to binary Avro data
type AvroEventBatchEncoder struct {
	keySchemaManager   *AvroSchemaManager
	valueSchemaManager *AvroSchemaManager
	resultBuf          []*MQMessage

	enableTiDBExtension        bool
	decimalHandlingMode        string
	bigintUnsignedHandlingMode string
}

type avroEncodeResult struct {
	data       []byte
	registryID int
}

func newAvroEventBatchEncoder() *AvroEventBatchEncoder {
	return &AvroEventBatchEncoder{
		valueSchemaManager: nil,
		keySchemaManager:   nil,
		resultBuf:          make([]*MQMessage, 0, 4096),
	}
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *AvroEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
	mqMessage := NewMQMessage(
		config.ProtocolAvro,
		nil,
		nil,
		e.CommitTs,
		model.MqMessageTypeRow,
		&e.Table.Schema,
		&e.Table.Table,
	)
	ctx := context.Background()

	if !e.IsDelete() {
		res, err := a.avroEncode(ctx, e, false)
		if err != nil {
			log.Warn(
				"AppendRowChangedEvent: avro encoding failed",
				zap.String("table", e.Table.String()),
			)
			return errors.Annotate(err, "AppendRowChangedEvent could not encode to Avro")
		}

		evlp, err := res.toEnvelope()
		if err != nil {
			log.Warn(
				"AppendRowChangedEvent: could not construct Avro envelope",
				zap.String("table", e.Table.String()),
			)
			return errors.Annotate(err, "AppendRowChangedEvent could not construct Avro envelope")
		}

		mqMessage.Value = evlp
	} else {
		mqMessage.Value = nil
	}

	res, err := a.avroEncode(ctx, e, true)
	if err != nil {
		log.Warn(
			"AppendRowChangedEvent: avro encoding failed",
			zap.String("table", e.Table.String()),
		)
		return errors.Annotate(err, "AppendRowChangedEvent could not encode to Avro")
	}

	evlp, err := res.toEnvelope()
	if err != nil {
		log.Warn(
			"AppendRowChangedEvent: could not construct Avro envelope",
			zap.String("table", e.Table.String()),
		)
		return errors.Annotate(err, "AppendRowChangedEvent could not construct Avro envelope")
	}

	mqMessage.Key = evlp
	mqMessage.IncRowsCount()
	a.resultBuf = append(a.resultBuf, mqMessage)

	return nil
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

func (a *AvroEventBatchEncoder) avroEncode(
	ctx context.Context,
	e *model.RowChangedEvent,
	key bool,
) (*avroEncodeResult, error) {
	var cols []*model.Column
	var colInfos []rowcodec.ColInfo
	var enableTiDBExtension bool
	var schemaManager *AvroSchemaManager
	var operation string
	if key {
		cols, colInfos = e.HandleKeyColInfos()
		enableTiDBExtension = false
		schemaManager = a.keySchemaManager
	} else {
		cols = e.Columns
		colInfos = e.ColInfos
		enableTiDBExtension = a.enableTiDBExtension
		schemaManager = a.valueSchemaManager
		if e.IsInsert() {
			operation = "c"
		} else if e.IsUpdate() {
			operation = "u"
		} else {
			log.Panic("unknown operation", zap.Reflect("rowChangedEvent", e))
			return nil, cerror.ErrAvroEncodeFailed.GenWithStack("unknown operation")
		}
	}

	qualifiedName := getQualifiedNameFromTableName(e.Table)

	schemaGen := func() (string, error) {
		schema, err := rowToAvroSchema(
			qualifiedName,
			cols,
			colInfos,
			enableTiDBExtension,
			a.decimalHandlingMode,
			a.bigintUnsignedHandlingMode,
		)
		if err != nil {
			return "", errors.Annotate(err, "AvroEventBatchEncoder: generating schema failed")
		}
		return schema, nil
	}

	avroCodec, registryID, err := schemaManager.GetCachedOrRegister(
		ctx,
		qualifiedName,
		e.TableInfoVersion,
		schemaGen,
	)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: get-or-register failed")
	}

	native, err := rowToAvroData(
		cols,
		colInfos,
		e.CommitTs,
		operation,
		enableTiDBExtension,
		a.decimalHandlingMode,
		a.bigintUnsignedHandlingMode,
	)
	if err != nil {
		return nil, errors.Annotate(err, "AvroEventBatchEncoder: converting to native failed")
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(
				cerror.ErrAvroEncodeToBinary,
				err,
			),
			"AvroEventBatchEncoder: converting to Avro binary failed",
		)
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

const (
	tidbType         = "tidb_type"
	tidbOp           = "_tidb_op"
	tidbCommitTs     = "_tidb_commit_ts"
	tidbPhysicalTime = "_tidb_physical_time"
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

// debezium-core/src/main/java/io/debezium/schema/FieldNameSelector.java
// https://avro.apache.org/docs/current/spec.html#names
func sanitizeColumnName(name string) string {
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
			fmt.Sprintf(
				"Field '%s' name potentially not safe for serialization, replaced with '%s'",
				name,
				sanitizedName,
			),
		)
	}
	return sanitizedName
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

func getQualifiedNameFromTableName(tableName *model.TableName) string {
	return tableName.Schema + "." + tableName.Table
}

type avroSchema struct {
	Type       string            `json:"type"`
	Parameters map[string]string `json:"connect.parameters"`
}

type avroLogicalTypeSchema struct {
	avroSchema
	LogicalType string      `json:"logicalType"`
	Precision   interface{} `json:"precision,omitempty"`
	Scale       interface{} `json:"scale,omitempty"`
}

func rowToAvroSchema(
	qualifiedName string,
	columnInfo []*model.Column,
	colInfos []rowcodec.ColInfo,
	enableTiDBExtension bool,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
) (string, error) {
	top := avroSchemaTop{
		Tp:     "record",
		Name:   qualifiedName,
		Fields: nil,
	}

	for i, col := range columnInfo {
		avroType, err := columnToAvroSchema(
			col,
			colInfos[i].Ft,
			decimalHandlingMode,
			bigintUnsignedHandlingMode,
		)
		if err != nil {
			return "", err
		}
		field := make(map[string]interface{})
		field["name"] = sanitizeColumnName(col.Name)
		if col.Flag.IsNullable() {
			field["type"] = []interface{}{"null", avroType}
			field["default"] = nil
		} else {
			field["type"] = avroType
		}

		top.Fields = append(top.Fields, field)
	}

	if enableTiDBExtension {
		top.Fields = append(top.Fields,
			map[string]interface{}{
				"name": tidbOp,
				"type": "string",
			},
			map[string]interface{}{
				"name": tidbCommitTs,
				"type": "long",
			},
			map[string]interface{}{
				"name": tidbPhysicalTime,
				"type": "long",
			},
		)
	}

	str, err := json.Marshal(&top)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrAvroMarshalFailed, err)
	}
	log.Debug("Avro Schema JSON generated", zap.ByteString("schema", str))
	return string(str), nil
}

func rowToAvroData(
	cols []*model.Column,
	colInfos []rowcodec.ColInfo,
	commitTs uint64,
	operation string,
	enableTiDBExtension bool,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		data, str, err := columnToAvroData(
			col,
			colInfos[i].Ft,
			decimalHandlingMode,
			bigintUnsignedHandlingMode,
		)
		if err != nil {
			return nil, err
		}

		// https://pkg.go.dev/github.com/linkedin/goavro/v2#Union
		if col.Flag.IsNullable() {
			ret[sanitizeColumnName(col.Name)] = goavro.Union(str, data)
		} else {
			ret[sanitizeColumnName(col.Name)] = data
		}
	}

	if enableTiDBExtension {
		ret[tidbOp] = operation
		ret[tidbCommitTs] = int64(commitTs)
		ret[tidbPhysicalTime] = oracle.ExtractPhysical(commitTs)
	}

	return ret, nil
}

func columnToAvroSchema(
	col *model.Column,
	ft *types.FieldType,
	decimalHandlingMode string,
	bigintUnsignedHandlingMode string,
) (interface{}, error) {
	log.Debug("getAvroDataTypeFromColumn", zap.Reflect("col", col))
	tt := getTiDBTypeFromColumn(col)
	switch col.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24: // BOOL/TINYINT/SMALLINT/MEDIUMINT
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
		if col.Flag.IsUnsigned() && bigintUnsignedHandlingMode == "string" {
			return avroSchema{
				Type:       "string",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "long",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeFloat, mysql.TypeDouble:
		return avroSchema{
			Type:       "double",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeBit:
		displayFlen := ft.Flen
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
		if decimalHandlingMode == "precise" {
			defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.Tp)
			displayFlen, displayDecimal := ft.Flen, ft.Decimal
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
		es := make([]string, 0, len(ft.Elems))
		for _, e := range ft.Elems {
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
		log.Panic("unknown mysql type", zap.Reflect("mysqlType", col.Type))
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
		if col.Flag.IsUnsigned() {
			return int32(col.Value.(uint64)), "int", nil
		}
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLong:
		if col.Flag.IsUnsigned() {
			return int64(col.Value.(uint64)), "long", nil
		}
		return int32(col.Value.(int64)), "int", nil
	case mysql.TypeLonglong:
		if col.Flag.IsUnsigned() {
			if bigintUnsignedHandlingMode == "long" {
				return int64(col.Value.(uint64)), "long", nil
			}
			// bigintUnsignedHandlingMode == "string"
			return strconv.FormatUint(col.Value.(uint64), 10), "string", nil
		}
		return col.Value.(int64), "long", nil
	case mysql.TypeFloat, mysql.TypeDouble:
		return col.Value.(float64), "double", nil
	case mysql.TypeBit:
		return []byte(types.NewBinaryLiteralFromUint(col.Value.(uint64), -1)), "bytes", nil
	case mysql.TypeNewDecimal:
		if decimalHandlingMode == "precise" {
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
			return col.Value, "bytes", nil
		}
		return string(col.Value.([]byte)), "string", nil
	case mysql.TypeEnum:
		enumVar, err := types.ParseEnumValue(ft.Elems, col.Value.(uint64))
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return enumVar.Name, "string", nil
	case mysql.TypeSet:
		setVar, err := types.ParseSetValue(ft.Elems, col.Value.(uint64))
		if err != nil {
			return nil, "", cerror.WrapError(cerror.ErrAvroEncodeFailed, err)
		}
		return setVar.Name, "string", nil
	case mysql.TypeJSON:
		return col.Value.(string), "string", nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return col.Value.(string), "string", nil
	case mysql.TypeYear:
		return col.Value.(int64), "int", nil
	default:
		log.Panic("unknown mysql type", zap.Reflect("mysqlType", col.Type))
		return nil, "", cerror.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

const magicByte = uint8(0)

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

type avroEventBatchEncoderBuilder struct {
	config             *Config
	keySchemaManager   *AvroSchemaManager
	valueSchemaManager *AvroSchemaManager
}

const (
	keySchemaSuffix   = "-key"
	valueSchemaSuffix = "-value"
)

func newAvroEventBatchEncoderBuilder(config *Config) (EncoderBuilder, error) {
	ctx := context.Background()
	keySchemaManager, err := NewAvroSchemaManager(
		ctx,
		nil,
		config.avroSchemaRegistry,
		keySchemaSuffix,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	valueSchemaManager, err := NewAvroSchemaManager(
		ctx,
		nil,
		config.avroSchemaRegistry,
		valueSchemaSuffix,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &avroEventBatchEncoderBuilder{
		config:             config,
		keySchemaManager:   keySchemaManager,
		valueSchemaManager: valueSchemaManager,
	}, nil
}

// Build an AvroEventBatchEncoder.
func (b *avroEventBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := newAvroEventBatchEncoder()
	encoder.keySchemaManager = b.keySchemaManager
	encoder.valueSchemaManager = b.valueSchemaManager
	encoder.enableTiDBExtension = b.config.enableTiDBExtension
	encoder.decimalHandlingMode = b.config.avroDecimalHandlingMode
	encoder.bigintUnsignedHandlingMode = b.config.avroBigintUnsignedHandlingMode

	return encoder
}
