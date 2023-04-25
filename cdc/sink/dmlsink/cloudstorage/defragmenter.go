// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"context"

	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
)

// defragmenter is used to handle event fragments which can be registered
// out of order.
type defragmenter struct {
	lastWritten uint64
	future      map[uint64]eventFragment
	inputCh     <-chan eventFragment
	outputChs   []*chann.DrainableChann[eventFragment]
	hasher      *hash.PositionInertia
}

func newDefragmenter(
	inputCh <-chan eventFragment,
	outputChs []*chann.DrainableChann[eventFragment],
) *defragmenter {
	return &defragmenter{
		future:    make(map[uint64]eventFragment),
		inputCh:   inputCh,
		outputChs: outputChs,
		hasher:    hash.NewPositionInertia(),
	}
}

func (d *defragmenter) run(ctx context.Context) error {
	defer d.close()
	for {
		select {
		case <-ctx.Done():
			d.future = nil
			return errors.Trace(ctx.Err())
		case frag, ok := <-d.inputCh:
			if !ok {
				return nil
			}
			// check whether to write messages to output channel right now
			next := d.lastWritten + 1
			if frag.seqNumber == next {
				d.writeMsgsConsecutive(ctx, frag)
			} else if frag.seqNumber > next {
				d.future[frag.seqNumber] = frag
			} else {
				return nil
			}
		}
	}
}

func (d *defragmenter) writeMsgsConsecutive(
	ctx context.Context,
	start eventFragment,
) {
	d.dispatchFragToDMLWorker(start)

	d.lastWritten++
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		next := d.lastWritten + 1
		if frag, ok := d.future[next]; ok {
			delete(d.future, next)
			d.dispatchFragToDMLWorker(frag)
			d.lastWritten = next
		} else {
			return
		}
	}
}

func (d *defragmenter) dispatchFragToDMLWorker(frag eventFragment) {
	tableName := frag.versionedTable.TableNameWithPhysicTableID
	d.hasher.Reset()
	d.hasher.Write([]byte(tableName.Schema), []byte(tableName.Table))
	workerID := d.hasher.Sum32() % uint32(len(d.outputChs))
	d.outputChs[workerID].In() <- frag
}

func (d *defragmenter) close() {
	for _, ch := range d.outputChs {
		ch.CloseAndDrain()
	}
}
