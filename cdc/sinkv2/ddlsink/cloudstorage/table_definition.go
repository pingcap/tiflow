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

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
)

type tableColumn struct {
	Name      string `json:"ColumnName" `
	Tp        string `json:"ColumnType"`
	Length    string `json:"ColumnLength,omitempty"`
	Precision string `json:"ColumnPrecision,omitempty"`
	Scale     string `json:"ColumnScale,omitempty"`
	Nullable  string `json:"ColumnNullable,omitempty"`
	IsPK      string `json:"ColumnIsPk,omitempty"`
}

func (t *tableColumn) fromTiColumnInfo(col *timodel.ColumnInfo) {
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

type tableDef struct {
	Table        string        `json:"Table"`
	Schema       string        `json:"Schema"`
	Version      uint64        `json:"Version"`
	Columns      []tableColumn `json:"TableColumns"`
	TotalColumns int           `json:"TableColumnsTotal"`
}

func (t *tableDef) fromTableInfo(info *model.TableInfo) {
	t.Table = info.TableName.Table
	t.Schema = info.TableName.Schema
	t.Version = info.TableInfoVersion
	t.TotalColumns = len(info.Columns)
	for _, col := range info.Columns {
		var tableCol tableColumn
		tableCol.fromTiColumnInfo(col)
		t.Columns = append(t.Columns, tableCol)
	}
}
