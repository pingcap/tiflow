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
	"sync"

	"github.com/pingcap/tiflow/pkg/chann"
)

// defragmenter is used to handle event fragments which can be registered
// out of order.
type defragmenter struct {
	lastWritten uint64
	future      map[uint64]eventFragment
	wg          sync.WaitGroup
	inputCh     *chann.Chann[eventFragment]
	outputCh    *chann.Chann[eventFragment]
}

func newDefragmenter(ctx context.Context) *defragmenter {
	d := &defragmenter{
		future:   make(map[uint64]eventFragment),
		inputCh:  chann.New[eventFragment](),
		outputCh: chann.New[eventFragment](),
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.defragMsgs(ctx)
	}()
	return d
}

func (d *defragmenter) registerFrag(frag eventFragment) {
	d.inputCh.In() <- frag
}

func (d *defragmenter) orderedOut() *chann.Chann[eventFragment] {
	return d.outputCh
}

func (d *defragmenter) defragMsgs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			d.future = nil
			return
		case frag, ok := <-d.inputCh.Out():
			if !ok {
				return
			}
			// check whether to write messages to output channel right now
			next := d.lastWritten + 1
			if frag.seqNumber == next {
				d.writeMsgsConsecutive(ctx, frag)
			} else if frag.seqNumber > next {
				d.future[frag.seqNumber] = frag
			} else {
				return
			}
		}
	}
}

func (d *defragmenter) writeMsgsConsecutive(
	ctx context.Context,
	start eventFragment,
) {
	d.outputCh.In() <- start

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
			d.outputCh.In() <- frag
			d.lastWritten = next
		} else {
			return
		}
	}
}

func (d *defragmenter) close() {
	d.wg.Wait()
	d.inputCh.Close()
	for range d.inputCh.Out() {
	}
	d.outputCh.Close()
	for range d.outputCh.Out() {
	}
}
