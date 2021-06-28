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

package hash

import (
	"hash"
	"hash/crc32"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const hashMagicNumber = 0

// PositionInertia is a 8-bits hash which is bytes partitions inertia
type PositionInertia struct {
	hashValue uint32
	hasher    hash.Hash32
}

// NewPositionInertia creates a new position inertia algorithm hash builder
func NewPositionInertia() *PositionInertia {
	return &PositionInertia{
		hashValue: hashMagicNumber,
		hasher:    crc32.NewIEEE(),
	}
}

// Write writes the bytes into the PositionInertia
func (p *PositionInertia) Write(bss ...[]byte) {
	p.hasher.Reset()
	for _, bs := range bss {
		_, err := p.hasher.Write(bs)
		if err != nil {
			log.Panic("failed to write hash", zap.Error(err))
		}
	}
	rawHash := p.hasher.Sum32()
	p.hashValue ^= rawHash
}

// Sum32 returns the 32-bits hash
func (p *PositionInertia) Sum32() uint32 {
	return p.hashValue
}

// Reset resets the PositionInertia
func (p *PositionInertia) Reset() {
	p.hashValue = hashMagicNumber
}
