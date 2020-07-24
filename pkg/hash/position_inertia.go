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

// PositionInertia is a 8-bits hash which is bytes partitions inertia
type PositionInertia byte

func (p *PositionInertia) Write(bss ...[]byte) {
	var blockHash byte
	var i int
	for _, bs := range bss {
		for _, b := range bs {
			blockHash ^= loopLeftMove(b, i)
			i += 1
		}
	}
	*p ^= PositionInertia(blockHash)
}

func loopLeftMove(source byte, step int) byte {
	step %= 8
	if step < 0 {
		step += 8
	}
	return source>>(8-step) | (source << step)
}
