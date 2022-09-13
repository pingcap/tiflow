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
	"errors"

	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/chann"
)

type defragmenter struct {
	lastWritten     int64
	lastSeqNumber   int64
	written         int64
	future          map[int64]eventFragment
	registryCh      chan eventFragment
	lastSeqNotifyCh chan int64
}

func newDefragmenter() *defragmenter {
	return &defragmenter{
		future:          make(map[int64]eventFragment),
		registryCh:      make(chan eventFragment),
		lastSeqNotifyCh: make(chan int64),
	}
}

func (d *defragmenter) register(frag eventFragment) {
	d.registryCh <- frag
}

func (d *defragmenter) setLast(lastSeq int64) {
	d.lastSeqNotifyCh <- lastSeq
}

func (d *defragmenter) output(ctx context.Context, dst *chann.Chann[*common.Message]) (int64, error) {
	for {
		if d.lastWritten >= d.lastSeqNumber && d.lastSeqNumber > 0 {
			break
		}

		select {
		case <-ctx.Done():
			d.future = nil
			return 0, ctx.Err()
		case frag := <-d.registryCh:
			// check whether to output right now.
			next := d.lastWritten + 1
			if frag.seqNumber == next {
				n, err := d.outputConsecutive(ctx, dst, frag)
				d.written += n
				if err != nil {
					return d.written, err
				}
			} else if frag.seqNumber > next {
				d.future[frag.seqNumber] = frag
			} else {
				return d.written, errors.New("unexpected error")
			}
		case d.lastSeqNumber = <-d.lastSeqNotifyCh:
			continue
		}
	}

	return d.written, nil
}

func (d *defragmenter) outputConsecutive(
	ctx context.Context,
	dst *chann.Chann[*common.Message],
	start eventFragment,
) (int64, error) {
	var written int64
	for _, msg := range start.encodedMsgs {
		dst.In() <- msg
	}

	d.lastWritten++

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		next := d.lastWritten + 1
		if frag, ok := d.future[next]; ok {
			delete(d.future, next)
			for _, msg := range frag.encodedMsgs {
				dst.In() <- msg
				written += int64(len(msg.Value))
			}

			d.lastWritten = next
		} else {
			return written, nil
		}
	}
}
