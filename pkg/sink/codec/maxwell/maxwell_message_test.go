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

package maxwell

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestMaxwellFormatCol(t *testing.T) {
	t.Parallel()
	row := &maxwellMessage{
		Ts:       1,
		Database: "a",
		Table:    "b",
		Type:     "delete",
		Xid:      1,
		Xoffset:  1,
		Position: "",
		Gtid:     "",
		Data: map[string]interface{}{
			"id": "1",
		},
	}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	require.NotNil(t, rowEncode)
}

func TestEncodeBinaryToMaxwell(t *testing.T) {
	t.Parallel()

	column := &model.Column{
		Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
		Flag: model.BinaryFlag,
	}

	e := &model.RowChangedEvent{
		Table:   &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{column},
	}

	key, msg := rowChangeToMaxwellMsg(e, false)
	require.NotNil(t, key)
	require.NotNil(t, msg)
}
