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

package tests

import (
	"sort"

	"golang.org/x/exp/rand"
)

type workloadGenerator interface {
	Next() []uint64
}

type uniformGenerator struct {
	workingSetSize int64
	batchSize      int
	numSlots       uint64
}

func newUniformGenerator(workingSetSize int64, batchSize int, numSlots uint64) *uniformGenerator {
	return &uniformGenerator{
		workingSetSize: workingSetSize,
		batchSize:      batchSize,
		numSlots:       numSlots,
	}
}

func (g *uniformGenerator) Next() []uint64 {
	set := make(map[uint64]struct{}, g.batchSize)
	for i := 0; i < g.batchSize; i++ {
		key := uint64(rand.Int63n(g.workingSetSize))
		set[key] = struct{}{}
	}

	ret := make([]uint64, 0, g.batchSize)
	for key := range set {
		ret = append(ret, key)
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i]%g.numSlots < ret[j]%g.numSlots })
	return ret
}
