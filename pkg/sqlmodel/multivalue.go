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

package sqlmodel

// SameColumns check whether two row change have same columns so they can be merged to one DML.
func SameColumns(lhs *RowChange, rhs *RowChange) bool {
	var lhsCols, rhsCols []string
	if lhs.tp == RowChangeDelete {
		lhsCols, _ = lhs.whereColumnsAndValues()
		rhsCols, _ = rhs.whereColumnsAndValues()
	} else {
		// if source table is same, columns will be same.
		if lhs.sourceTable.Schema == rhs.sourceTable.Schema && lhs.sourceTable.Table == rhs.sourceTable.Table {
			return true
		}
		for _, col := range lhs.sourceTableInfo.Columns {
			lhsCols = append(lhsCols, col.Name.O)
		}
		for _, col := range rhs.sourceTableInfo.Columns {
			rhsCols = append(rhsCols, col.Name.O)
		}
	}
	if len(lhsCols) != len(rhsCols) {
		return false
	}
	for i := 0; i < len(lhsCols); i++ {
		if lhsCols[i] != rhsCols[i] {
			return false
		}
	}
	return true
}
