// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"sort"

	"github.com/pingcap/tiflow/cdc/model"
)

// SortTableIDs sorts a slice of table IDs in ascending order.
func SortTableIDs(tableIDs []model.TableID) {
	sort.Slice(tableIDs, func(i, j int) bool {
		return tableIDs[i] < tableIDs[j]
	})
}
