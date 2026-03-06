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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
)

// ForeignKeyCausalityRelation describes how a row's values relate to a root parent table for causality checks.
// ChildColumnIdx indexes refer to the row values of the current table (excluding hidden columns),
// while ParentColumns and ParentTable describe the ultimate parent key the row should collide with.
type ForeignKeyCausalityRelation struct {
	ParentTable    string
	ParentColumns  []*timodel.ColumnInfo
	ChildColumnIdx []int
}
