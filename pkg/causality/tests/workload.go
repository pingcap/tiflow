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

import "golang.org/x/exp/rand"

type workloadGenerator interface {
	Next() []int64
}

type uniformGenerator struct {
	workingSetSize int64
	batchSize      int
}

func newUniformGenerator(workingSetSize int64, batchSize int) *uniformGenerator {
	return &uniformGenerator{
		workingSetSize: workingSetSize,
		batchSize:      batchSize,
	}
}

func (g *uniformGenerator) Next() []int64 {
	ret := make([]int64, 0, g.batchSize)
	for i := 0; i < g.batchSize; i++ {
		ret = append(ret, rand.Int63n(g.workingSetSize))
	}
	return ret
}
