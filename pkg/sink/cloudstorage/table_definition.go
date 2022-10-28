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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
)

// TableCol denotes the column info for a table definition.
type TableCol struct {
	Name      string `json:"ColumnName" `
	Tp        string `json:"ColumnType"`
	Length    string `json:"ColumnLength,omitempty"`
	Precision string `json:"ColumnPrecision,omitempty"`
	Scale     string `json:"ColumnScale,omitempty"`
	Nullable  string `json:"ColumnNullable,omitempty"`
	IsPK      string `json:"ColumnIsPk,omitempty"`
}

// FromTiColumnInfo converts from TiDB ColumnInfo to TableCol.
func (t *TableCol) FromTiColumnInfo(col *timodel.ColumnInfo) {
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

func (t *TableCol) ToTiColumnInfo() (*timodel.ColumnInfo, error) {
	col := new(timodel.ColumnInfo)

	tp := types.StrToType(strings.ToLower(strings.TrimSuffix(t.Tp, " UNSIGNED")))
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(t.Tp, "UNSIGNED") {
		col.SetFlag(mysql.UnsignedFlag)
	}
	if t.IsPK == "true" {
		col.SetFlag(mysql.PriKeyFlag)
	}
	if t.Nullable == "false" {
		col.SetFlag(mysql.NotNullFlag)
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

// TableDetail is the detailed table definition used for cloud storage sink.
type TableDetail struct {
	Table        string     `json:"Table"`
	Schema       string     `json:"Schema"`
	Version      uint64     `json:"Version"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

// FromTableInfo converts from TableInfo to TableDetail.
func (t *TableDetail) FromTableInfo(info *model.TableInfo) {
	t.Table = info.TableName.Table
	t.Schema = info.TableName.Schema
	t.Version = info.TableInfoVersion
	t.TotalColumns = len(info.Columns)
	for _, col := range info.Columns {
		var tableCol TableCol
		tableCol.FromTiColumnInfo(col)
		t.Columns = append(t.Columns, tableCol)
	}
}

func (t *TableDetail) ToTableInfo() (*model.TableInfo, error) {
	info := &model.TableInfo{
		TableName: model.TableName{
			Schema: t.Schema,
			Table:  t.Table,
		},
		TableInfo: &timodel.TableInfo{
			Name: timodel.NewCIStr(t.Table),
		},
	}
	for _, col := range t.Columns {
		tiCol, err := col.ToTiColumnInfo()
		if err != nil {
			return nil, err
		}
		info.Columns = append(info.Columns, tiCol)
	}

	return info, nil
}
