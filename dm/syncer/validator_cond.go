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
	isOneKey := len(pk.Columns) == 1
	if !isOneKey {
		b.WriteString("(")
	}
	for i := 0; i < len(pk.Columns); i++ {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(pk.Columns[i].Name.O)
	}
	if !isOneKey {
		b.WriteString(") in (")
	} else {
		b.WriteString(" in (")
	}
	for i := range c.PkValues {
		if i != 0 {
			b.WriteString(",")
		}
		if !isOneKey {
			b.WriteString("(")
			for j := 0; j < len(pk.Columns); j++ {
				if j != 0 {
					b.WriteString(",")
				}
				b.WriteString("?")
			}
			b.WriteString(")")
		} else {
			b.WriteString("?")
		}
	}
	b.WriteString(")")
	return b.String()
}
