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

package syncer

import (
	"strings"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

type Cond struct {
	Table    *TableDiff
	PkValues [][]string
}

func (c *Cond) GetArgs() []interface{} {
	var res []interface{}
	for _, v := range c.PkValues {
		for _, val := range v {
			res = append(res, val)
		}
	}
	return res
}

func (c *Cond) GetWhere() string {
	var b strings.Builder
	pk := c.Table.PrimaryKey
	b.WriteString("(")
	for i := 0; i < len(pk.Columns); i++ {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(pk.Columns[i].Name.O)
	}
	b.WriteString(") in (")
	for i := range c.PkValues {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := 0; j < len(pk.Columns); j++ {
			if j != 0 {
				b.WriteString(",")
			}
			b.WriteString("?")
		}
		b.WriteString(")")
	}
	b.WriteString(")")
	return b.String()
}

type SimpleRowsIterator struct {
	Rows []map[string]*dbutil.ColumnData
	Idx  int
}

func (b *SimpleRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	if b.Idx >= len(b.Rows) {
		return nil, nil
	}
	row := b.Rows[b.Idx]
	b.Idx++
	return row, nil
}

func (b *SimpleRowsIterator) Close() {
	// skip
}
