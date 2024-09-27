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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetColumns(t *testing.T) {
	sql := "CREATE TABLE test (id INT PRIMARY KEY, name enum('a','b','c'));"
	ft1 := types.NewFieldType(mysql.TypeLong)
	ft2 := types.NewFieldType(mysql.TypeEnum)
	columnInfos := []*timodel.ColumnInfo{
		{
			Name:      model.NewCIStr("id"),
			FieldType: *ft1,
		},
		{
			Name:      model.NewCIStr("name"),
			FieldType: *ft2,
		},
	}
	newColumnInfos := getColumns(sql, columnInfos)
	require.Equal(t, columnInfos, newColumnInfos)
}
