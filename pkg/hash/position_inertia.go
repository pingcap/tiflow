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

const hashMagicNumber = 0x6A

// PositionInertia is a 8-bits hash which is bytes partitions inertia
type PositionInertia struct {
	hashValue byte
	hasher    hash.Hash32
}

func NewPositionInertia() *PositionInertia {
	return &PositionInertia{
		hashValue: hashMagicNumber,
		hasher:    crc32.NewIEEE(),
	}
}

func (p *PositionInertia) Write(bss ...[]byte) {
	p.hasher.Reset()
	for _, bs := range bss {
		_, err := p.hasher.Write(bs)
		if err != nil {
			log.Fatal("failed to write hash", zap.Error(err))
		}
	}
	rawHash := p.hasher.Sum32()
	rawHash ^= rawHash >> 16
	rawHash ^= rawHash >> 8
	p.hashValue ^= byte(rawHash)
}

func (p *PositionInertia) Sum8() byte {
	return p.hashValue
}

func (p *PositionInertia) Reset() {
	p.hashValue = hashMagicNumber
}
