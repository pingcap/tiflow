// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestGetColumns(t *testing.T) {
	sql := "CREATE TABLE test (id INT PRIMARY KEY, val1 datetime default current_timestamp, val2 time(2) default 0,  val3 timestamp(3) default now(), val4 YEAR(4) default 1970 comment 'first');"
	columnInfos := []*timodel.ColumnInfo{
		{
			Name:      pmodel.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
		},
		{
			Name:      pmodel.NewCIStr("val1"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
		{
			Name:      pmodel.NewCIStr("val2"),
			FieldType: *types.NewFieldType(mysql.TypeDuration),
		},
		{
			Name:      pmodel.NewCIStr("val3"),
			FieldType: *types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			Name:      pmodel.NewCIStr("val4"),
			FieldType: *types.NewFieldType(mysql.TypeYear),
		},
	}
	parseColumns(sql, columnInfos)
	require.Equal(t, columnInfos[1].GetDefaultValue(), "CURRENT_TIMESTAMP")
	require.Equal(t, columnInfos[2].GetDecimal(), 2)
	require.Equal(t, columnInfos[2].GetDefaultValue(), "0")
	require.Equal(t, columnInfos[3].GetDecimal(), 3)
	require.Equal(t, columnInfos[3].GetDefaultValue(), "CURRENT_TIMESTAMP")
	require.Equal(t, columnInfos[4].GetFlen(), 4)
	require.Equal(t, columnInfos[4].GetDefaultValue(), "1970")
	require.Equal(t, columnInfos[4].Comment, "")
}

func TestGetValue(t *testing.T) {
	column := &model.Column{
		Default: 1,
	}
	data := model.Column2ColumnDataXForTest(column)
	v := getValue(data)
	require.Equal(t, v, int64(1))
	data.Value = 2
	v = getValue(data)
	require.Equal(t, v, 2)
}

func TestGetSchemaTopicName(t *testing.T) {
	namespace := "default"
	schema := "1A.B"
	table := "columnNameWith中文"
	name := getSchemaTopicName(namespace, schema, table)
	require.Equal(t, name, "default._1A_B.columnNameWith__")
}
