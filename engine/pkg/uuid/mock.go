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

package uuid

import "github.com/pingcap/log"

type MockGenerator struct {
	list []string
}

func NewMock() *MockGenerator {
	return &MockGenerator{}
}

func (g *MockGenerator) NewString() (ret string) {
	if len(g.list) == 0 {
		log.L().Panic("Empty uuid list. Please use Push() to add a uuid to the list.")
	}

	ret, g.list = g.list[0], g.list[1:]
	return
}

func (g *MockGenerator) Push(uuid string) {
	g.list = append(g.list, uuid)
}
