// Copyright 2022 PingCAP, Inc.
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

package mysql

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

var (
	tiCtx = mock.NewContext()
)

func showCreateTable(t *testing.T, ti *timodel.TableInfo) string {
	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := executor.ConstructResultOfShowCreateTable(tiCtx, ti, autoid.Allocators{}, result)
	require.NoError(t, err)
	return result.String()
}

func TestRecoverTableInfo(t *testing.T) {
	cases := []struct {
		preCols      []*model.Column
		postCols     []*model.Column
		indexColumns [][]int
	}{
		{
			preCols: []*model.Column{
				nil,
				{
					Name:  "a1",
					Type:  mysql.TypeLong,
					Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				}, {
					Name:  "a3",
					Type:  mysql.TypeLong,
					Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				}},
			indexColumns: [][]int{{1, 2}},
		},
		{
			postCols: []*model.Column{
				nil,
				{
					Name:  "a1",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				}, {
					Name:  "a3",
					Type:  mysql.TypeLong,
					Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				}},
		},
	}

	for _, c := range cases {
		tableInfo := recoverTableInfo(c.preCols, c.postCols, c.indexColumns)
		t.Log(showCreateTable(t, tableInfo))
	}
}
