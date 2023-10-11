// Copyright 2023 PingCAP, Inc.
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

package sql

import (
	"hash"
	"hash/crc64"
	"hash/fnv"
	"sync/atomic"

	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/uuid"
)

type uuidGenerator interface {
	GenChangefeedUUID() metadata.ChangefeedUUID
}

// NewUUIDGenerator creates a new UUID generator.
func NewUUIDGenerator(config string) uuidGenerator {
	switch config {
	case "mock":
		return newMockUUIDGenerator()
	case "random-crc64":
		hasher := crc64.New(crc64.MakeTable(crc64.ISO))
		return newRandomUUIDGenerator(hasher)
	case "random-fnv64":
		return newRandomUUIDGenerator(fnv.New64())
	}
	return nil
}

type mockUUIDGenerator struct {
	epoch atomic.Uint64
}

func newMockUUIDGenerator() uuidGenerator {
	return &mockUUIDGenerator{}
}

// GenChangefeedUUID implements uuidGenerator interface.
func (g *mockUUIDGenerator) GenChangefeedUUID() metadata.ChangefeedUUID {
	return g.epoch.Add(1)
}

type randomUUIDGenerator struct {
	uuidGen uuid.Generator
	hasher  hash.Hash64
}

func newRandomUUIDGenerator(hasher hash.Hash64) uuidGenerator {
	return &randomUUIDGenerator{
		uuidGen: uuid.NewGenerator(),
		hasher:  hasher,
	}
}

// GenChangefeedUUID implements uuidGenerator interface.
func (g *randomUUIDGenerator) GenChangefeedUUID() metadata.ChangefeedUUID {
	g.hasher = crc64.New(crc64.MakeTable(crc64.ISO))
	g.hasher.Reset()
	g.hasher.Write([]byte(g.uuidGen.NewString()))
	return g.hasher.Sum64()
}

// TODO: implement sql based UUID generator.
