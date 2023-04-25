// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
)

func prepareDataOneColoumnPK(t *testing.T, batch int) []*RowChange {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI := mockTableInfo(t, `CREATE TABLE tb (c INT, c2 INT, c3 INT,
	c4 VARCHAR(10), c5 VARCHAR(100), c6 VARCHAR(1000), PRIMARY KEY (c))`)
	targetTI := mockTableInfo(t, `CREATE TABLE tb (c INT, c2 INT, c3 INT,
	c4 VARCHAR(10), c5 VARCHAR(100), c6 VARCHAR(1000), PRIMARY KEY (c))`)

	changes := make([]*RowChange, 0, batch)
	for i := 0; i < batch; i++ {
		change := NewRowChange(source, target,
			[]interface{}{i + 1, i + 2, i + 3, "c4", "c5", "c6"},
			[]interface{}{i + 10, i + 20, i + 30, "c4", "c5", "c6"},
			sourceTI, targetTI, nil)
		changes = append(changes, change)
	}
	return changes
}

func prepareDataMultiColumnsPK(t *testing.T, batch int) []*RowChange {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI := mockTableInfo(t, `CREATE TABLE tb (c1 INT, c2 INT, c3 INT, c4 INT,
	c5 VARCHAR(10), c6 VARCHAR(100), c7 VARCHAR(1000), c8 timestamp, c9 timestamp,
	PRIMARY KEY (c1, c2, c3, c4))`)
	targetTI := mockTableInfo(t, `CREATE TABLE tb (c1 INT, c2 INT, c3 INT, c4 INT,
	c5 VARCHAR(10), c6 VARCHAR(100), c7 VARCHAR(1000), c8 timestamp, c9 timestamp,
	PRIMARY KEY (c1, c2, c3, c4))`)

	changes := make([]*RowChange, 0, batch)
	for i := 0; i < batch; i++ {
		change := NewRowChange(source, target,
			[]interface{}{i + 1, i + 2, i + 3, i + 4, "c4", "c5", "c6", "c7", time.Time{}, time.Time{}},
			[]interface{}{i + 10, i + 20, i + 30, i + 40, "c4", "c5", "c6", "c7", time.Time{}, time.Time{}},
			sourceTI, targetTI, nil)
		changes = append(changes, change)
	}
	return changes
}

// bench cmd: go test -run='^$' -benchmem -bench '^(BenchmarkGenUpdate)$' github.com/pingcap/tiflow/pkg/sqlmodel
func BenchmarkGenUpdate(b *testing.B) {
	t := &testing.T{}
	type genCase struct {
		name    string
		fn      genSQLFunc
		prepare func(t *testing.T, batch int) []*RowChange
	}
	batchSizes := []int{
		1, 2, 4, 8, 16, 32, 64, 128,
	}
	benchCases := []genCase{
		{
			name:    "OneColumnPK-GenUpdateSQL",
			fn:      GenUpdateSQL,
			prepare: prepareDataOneColoumnPK,
		},
		{
			name:    "OneColumnPK-GenUpdateSQL",
			fn:      GenUpdateSQL,
			prepare: prepareDataOneColoumnPK,
		},
		{
			name:    "MultiColumnsPK-GenUpdateSQL",
			fn:      GenUpdateSQL,
			prepare: prepareDataMultiColumnsPK,
		},
		{
			name:    "MultiColumnsPK-GenUpdateSQL",
			fn:      GenUpdateSQL,
			prepare: prepareDataMultiColumnsPK,
		},
	}
	for _, bc := range benchCases {
		for _, batch := range batchSizes {
			name := fmt.Sprintf("%s-Batch%d", bc.name, batch)
			b.Run(name, func(b *testing.B) {
				changes := prepareDataOneColoumnPK(t, batch)
				for i := 0; i < b.N; i++ {
					bc.fn(changes...)
				}
			})
		}
	}
}
