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

package txn

import (
	"testing"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestGenKeyListCaseInSensitive(t *testing.T) {
	t.Parallel()

	columns := []*model.Column{
		{
			Value: "XyZ",
		},
	}
	colInfo := []*timodel.ColumnInfo{
		{
			FieldType: *types.NewFieldTypeWithCollation(mysql.TypeVarchar, "utf8_unicode_ci", 10),
		},
	}

	first := genKeyList(columns, colInfo, 0, []int{0}, 1)

	columns = []*model.Column{
		{
			Value: "xYZ",
		},
	}
	colInfo = []*timodel.ColumnInfo{
		{
			FieldType: *types.NewFieldTypeWithCollation(mysql.TypeVarchar, "utf8_unicode_ci", 10),
		},
	}
	second := genKeyList(columns, colInfo, 0, []int{0}, 1)

	require.Equal(t, first, second)
}
