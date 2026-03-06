// Copyright 2025 PingCAP, Inc.
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

package sqlmodel

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestCausalityKeysWithForeignKey(t *testing.T) {
	t.Parallel()

	parentTable := mockTableInfo(t, "CREATE TABLE a(id INT PRIMARY KEY, note VARCHAR(5))")
	childTable := mockTableInfo(t, "CREATE TABLE b(id INT PRIMARY KEY, a_id INT, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a(id))")

	parentName := (&cdcmodel.TableName{Schema: "db", Table: "a"}).String()
	relation := ForeignKeyCausalityRelation{
		ParentTable:    parentName,
		ParentColumns:  []*timodel.ColumnInfo{parentTable.Columns[0]},
		ChildColumnIdx: []int{1},
	}

	change := NewRowChange(
		&cdcmodel.TableName{Schema: "db", Table: "b"},
		nil,
		nil,
		[]interface{}{2, 10},
		childTable,
		nil,
		nil,
	)
	change.SetForeignKeyRelations([]ForeignKeyCausalityRelation{relation})

	require.Equal(t, []string{"2.id.db.b", "10.id.db.a"}, change.CausalityKeys())
}

func TestCausalityKeysWithNullForeignKey(t *testing.T) {
	t.Parallel()

	parentTable := mockTableInfo(t, "CREATE TABLE a(id INT PRIMARY KEY, note VARCHAR(5))")
	childTable := mockTableInfo(t, "CREATE TABLE b(id INT PRIMARY KEY, a_id INT, CONSTRAINT fk_b_a FOREIGN KEY (a_id) REFERENCES a(id))")

	parentName := (&cdcmodel.TableName{Schema: "db", Table: "a"}).String()
	relation := ForeignKeyCausalityRelation{
		ParentTable:    parentName,
		ParentColumns:  []*timodel.ColumnInfo{parentTable.Columns[0]},
		ChildColumnIdx: []int{1},
	}

	change := NewRowChange(
		&cdcmodel.TableName{Schema: "db", Table: "b"},
		nil,
		nil,
		[]interface{}{3, nil},
		childTable,
		nil,
		nil,
	)
	change.SetForeignKeyRelations([]ForeignKeyCausalityRelation{relation})

	require.Equal(t, []string{"3.id.db.b"}, change.CausalityKeys())
}
