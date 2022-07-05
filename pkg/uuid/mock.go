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

// MockGenerator is a mocked uuid generator
type MockGenerator struct {
	list []string
}

// NewMock creates a new MockGenerator instance
func NewMock() *MockGenerator {
	return &MockGenerator{}
}

// NewString implements Generator.NewString
func (g *MockGenerator) NewString() (ret string) {
	if len(g.list) == 0 {
		log.Panic("Empty uuid list. Please use Push() to add a uuid to the list.")
	}

	ret, g.list = g.list[0], g.list[1:]
	return
}

// Push adds a candidate uuid in FIFO list
func (g *MockGenerator) Push(uuid string) {
	g.list = append(g.list, uuid)
}

// ConstGenerator is a mocked uuid generator, which always generate a pre defined uuid
type ConstGenerator struct {
	uid string
}

// NewConstGenerator creates a new ConstGenerator instance
func NewConstGenerator(uid string) *ConstGenerator {
	return &ConstGenerator{uid: uid}
}

// NewString implements Generator.NewString
func (g *ConstGenerator) NewString() string {
	return g.uid
}
