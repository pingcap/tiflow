// Copyright 2020 PingCAP, Inc.
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

package shardddl

import (
	"sort"

	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
)

// PessimismInfoSlice attaches the methods of Interface to []pessimism.Info,
// sorting in increasing order according to `Source` field.
type PessimismInfoSlice []pessimism.Info

// Len implements Sorter.Len.
func (p PessimismInfoSlice) Len() int {
	return len(p)
}

// Less implements Sorter.Less.
func (p PessimismInfoSlice) Less(i, j int) bool {
	return p[i].Source < p[j].Source
}

// Swap implements Sorter.Swap.
func (p PessimismInfoSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// pessimismInfoMapToSlice converts a `map[string]pessimism.Info` to `[]pessimism.Info` in increasing order according to `Source` field.
func pessimismInfoMapToSlice(ifm map[string]pessimism.Info) []pessimism.Info {
	ret := make(PessimismInfoSlice, 0, len(ifm))
	for _, info := range ifm {
		ret = append(ret, info)
	}
	sort.Sort(ret)
	return ret
}
