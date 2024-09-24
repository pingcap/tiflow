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
package cloudstorage

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

const (
	defaultTableDefinitionVersion = 1
	marshalPrefix                 = ""
	marshalIndent                 = "    "
)

// TableCol denotes the column info for a table definition.
type TableCol struct {
	ID        string      `json:"ColumnId,omitempty"`
	Name      string      `json:"ColumnName" `
	Tp        string      `json:"ColumnType"`
	Default   interface{} `json:"ColumnDefault,omitempty"`
	Precision string      `json:"ColumnPrecision,omitempty"`
	Scale     string      `json:"ColumnScale,omitempty"`
	Nullable  string      `json:"ColumnNullable,omitempty"`
	IsPK      string      `json:"ColumnIsPk,omitempty"`
}

// FromTiColumnInfo converts from TiDB ColumnInfo to TableCol.
func (t *TableCol) FromTiColumnInfo(col *timodel.ColumnInfo, outputColumnID bool) {
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
	isDecimalNotDefault := col.GetDecimal() != defaultDecimal &&
		col.GetDecimal() != 0 &&
		col.GetDecimal() != types.UnspecifiedLength

	displayFlen, displayDecimal := col.GetFlen(), col.GetDecimal()
	if displayFlen == types.UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == types.UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	if outputColumnID {
		t.ID = strconv.FormatInt(col.ID, 10)
	}
	t.Name = col.Name.O
	t.Tp = strings.ToUpper(types.TypeToStr(col.GetType(), col.GetCharset()))
	if mysql.HasUnsignedFlag(col.GetFlag()) {
		t.Tp += " UNSIGNED"
	}
	if mysql.HasPriKeyFlag(col.GetFlag()) {
		t.IsPK = "true"
	}
	if mysql.HasNotNullFlag(col.GetFlag()) {
		t.Nullable = "false"
	}
	t.Default = model.GetColumnDefaultValue(col)

	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		if isDecimalNotDefault {
			t.Scale = strconv.Itoa(displayDecimal)
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		t.Precision = strconv.Itoa(displayFlen)
		if isDecimalNotDefault {
			t.Scale = strconv.Itoa(displayDecimal)
		}
	case mysql.TypeNewDecimal:
		t.Precision = strconv.Itoa(displayFlen)
		t.Scale = strconv.Itoa(displayDecimal)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		t.Precision = strconv.Itoa(displayFlen)
	case mysql.TypeYear:
		t.Precision = strconv.Itoa(displayFlen)
	}
}

// ToTiColumnInfo converts from TableCol to TiDB ColumnInfo.
func (t *TableCol) ToTiColumnInfo(colID int64) (*timodel.ColumnInfo, error) {
	col := new(timodel.ColumnInfo)

	if t.ID != "" {
		var err error
		col.ID, err = strconv.ParseInt(t.ID, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	col.ID = colID
	col.Name = pmodel.NewCIStr(t.Name)
	tp := types.StrToType(strings.ToLower(strings.TrimSuffix(t.Tp, " UNSIGNED")))
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(t.Tp, "UNSIGNED") {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if t.IsPK == "true" {
		col.AddFlag(mysql.PriKeyFlag)
	}
	if t.Nullable == "false" {
		col.AddFlag(mysql.NotNullFlag)
	}
	col.DefaultValue = t.Default
	if strings.Contains(t.Tp, "BLOB") || strings.Contains(t.Tp, "BINARY") {
		col.SetCharset(charset.CharsetBin)
	} else {
		col.SetCharset(charset.CharsetUTF8MB4)
	}
	setFlen := func(precision string) error {
		if len(precision) > 0 {
			flen, err := strconv.Atoi(precision)
			if err != nil {
				return errors.Trace(err)
			}
			col.SetFlen(flen)
		}
		return nil
	}
	setDecimal := func(scale string) error {
		if len(scale) > 0 {
			decimal, err := strconv.Atoi(scale)
			if err != nil {
				return errors.Trace(err)
			}
			col.SetDecimal(decimal)
		}
		return nil
	}
	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		err := setDecimal(t.Scale)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case mysql.TypeDouble, mysql.TypeFloat, mysql.TypeNewDecimal:
		err := setFlen(t.Precision)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = setDecimal(t.Scale)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeYear:
		err := setFlen(t.Precision)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return col, nil
}

// TableDefinition is the detailed table definition used for cloud storage sink.
// TODO: find a better name for this struct.
type TableDefinition struct {
	Table        string             `json:"Table"`
	Schema       string             `json:"Schema"`
	Version      uint64             `json:"Version"`
	TableVersion uint64             `json:"TableVersion"`
	Query        string             `json:"Query"`
	Type         timodel.ActionType `json:"Type"`
	Columns      []TableCol         `json:"TableColumns"`
	TotalColumns int                `json:"TableColumnsTotal"`
}

// tableDefWithoutQuery is the table definition without query, which ignores the
// Query, Type and TableVersion field.
type tableDefWithoutQuery struct {
	Table        string     `json:"Table"`
	Schema       string     `json:"Schema"`
	Version      uint64     `json:"Version"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

// FromDDLEvent converts from DDLEvent to TableDefinition.
func (t *TableDefinition) FromDDLEvent(event *model.DDLEvent, outputColumnID bool) {
	if event.CommitTs != event.TableInfo.Version {
		log.Panic("commit ts and table info version should be equal",
			zap.Any("event", event), zap.Any("tableInfo", event.TableInfo),
		)
	}
	t.FromTableInfo(event.TableInfo, event.TableInfo.Version, outputColumnID)
	t.Query = event.Query
	t.Type = event.Type
}

// ToDDLEvent converts from TableDefinition to DDLEvent.
func (t *TableDefinition) ToDDLEvent() (*model.DDLEvent, error) {
	tableInfo, err := t.ToTableInfo()
	if err != nil {
		return nil, err
	}

	return &model.DDLEvent{
		TableInfo: tableInfo,
		CommitTs:  t.TableVersion,
		Type:      t.Type,
		Query:     t.Query,
	}, nil
}

// FromTableInfo converts from TableInfo to TableDefinition.
func (t *TableDefinition) FromTableInfo(
	info *model.TableInfo, tableInfoVersion model.Ts, outputColumnID bool,
) {
	t.Version = defaultTableDefinitionVersion
	t.TableVersion = tableInfoVersion

	t.Schema = info.TableName.Schema
	if info.TableInfo == nil {
		return
	}
	t.Table = info.TableName.Table
	t.TotalColumns = len(info.Columns)
	for _, col := range info.Columns {
		var tableCol TableCol
		tableCol.FromTiColumnInfo(col, outputColumnID)
		t.Columns = append(t.Columns, tableCol)
	}
}

// ToTableInfo converts from TableDefinition to DDLEvent.
func (t *TableDefinition) ToTableInfo() (*model.TableInfo, error) {
	tidbTableInfo := &timodel.TableInfo{
		Name: pmodel.NewCIStr(t.Table),
	}
	nextMockID := int64(100) // 100 is an arbitrary number
	for _, col := range t.Columns {
		tiCol, err := col.ToTiColumnInfo(nextMockID)
		if err != nil {
			return nil, err
		}
		if mysql.HasPriKeyFlag(tiCol.GetFlag()) {
			// use PKIsHandle to make sure that the primary keys can be detected by `WrapTableInfo`
			tidbTableInfo.PKIsHandle = true
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
		nextMockID += 1
	}
	info := model.WrapTableInfo(100, t.Schema, 100, tidbTableInfo)

	return info, nil
}

// IsTableSchema returns whether the TableDefinition is a table schema.
func (t *TableDefinition) IsTableSchema() bool {
	if len(t.Columns) != t.TotalColumns {
		log.Panic("invalid table definition", zap.Any("tableDef", t))
	}
	return t.TotalColumns != 0
}

// MarshalWithQuery marshals TableDefinition with Query field.
func (t *TableDefinition) MarshalWithQuery() ([]byte, error) {
	data, err := json.MarshalIndent(t, marshalPrefix, marshalIndent)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}
	return data, nil
}

// marshalWithoutQuery marshals TableDefinition without Query field.
func (t *TableDefinition) marshalWithoutQuery() ([]byte, error) {
	// sort columns by name
	sortedColumns := make([]TableCol, len(t.Columns))
	copy(sortedColumns, t.Columns)
	sort.Slice(sortedColumns, func(i, j int) bool {
		return sortedColumns[i].Name < sortedColumns[j].Name
	})

	defWithoutQuery := tableDefWithoutQuery{
		Table:        t.Table,
		Schema:       t.Schema,
		Columns:      sortedColumns,
		TotalColumns: t.TotalColumns,
	}

	data, err := json.MarshalIndent(defWithoutQuery, marshalPrefix, marshalIndent)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}
	return data, nil
}

// Sum32 returns the 32-bits hash value of TableDefinition.
func (t *TableDefinition) Sum32(hasher *hash.PositionInertia) (uint32, error) {
	if hasher == nil {
		hasher = hash.NewPositionInertia()
	}
	hasher.Reset()
	data, err := t.marshalWithoutQuery()
	if err != nil {
		return 0, err
	}

	hasher.Write(data)
	return hasher.Sum32(), nil
}

// GenerateSchemaFilePath generates the schema file path for TableDefinition.
func (t *TableDefinition) GenerateSchemaFilePath() (string, error) {
	checksum, err := t.Sum32(nil)
	if err != nil {
		return "", err
	}
	if !t.IsTableSchema() && t.Table != "" {
		log.Panic("invalid table definition", zap.Any("tableDef", t))
	}
	return generateSchemaFilePath(t.Schema, t.Table, t.TableVersion, checksum), nil
}
