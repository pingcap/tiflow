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
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type defragmenter struct {
	lastWritten   uint64
	lastSeqNumber uint64
	written       uint64
	future        map[uint64]eventFragment
	registryCh    *chann.Chann[eventFragment]
}

func newDefragmenter() *defragmenter {
	return &defragmenter{
		future:     make(map[uint64]eventFragment),
		registryCh: chann.New[eventFragment](),
	}
}

func (d *defragmenter) register(frag eventFragment) {
	d.registryCh.In() <- frag
}

func (d *defragmenter) writeMsgs(ctx context.Context, dst *chann.Chann[*common.Message]) (uint64, error) {
	for {
		if d.lastWritten >= d.lastSeqNumber && d.lastSeqNumber > 0 {
			dst.In() <- nil
			break
		}

		select {
		case <-ctx.Done():
			d.future = nil
			return 0, ctx.Err()
		case frag := <-d.registryCh.Out():
			// check whether we meet an ending mark
			if frag.event == nil {
				d.lastSeqNumber = frag.seqNumber
				continue
			}

			// check whether to write messages to output channel right now
			next := d.lastWritten + 1
			if frag.seqNumber == next {
				n, err := d.writeMsgsConsecutive(ctx, dst, frag)
				d.written += n
				if err != nil {
					return d.written, err
				}
			} else if frag.seqNumber > next {
				d.future[frag.seqNumber] = frag
			} else {
				return d.written, cerror.WrapError(cerror.ErrCloudStorageDefragmentFailed,
					errors.New("unexpected error"))
			}
		}
	}

	return d.written, nil
}

func (d *defragmenter) writeMsgsConsecutive(
	ctx context.Context,
	dst *chann.Chann[*common.Message],
	start eventFragment,
) (uint64, error) {
	var written uint64
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
				written += uint64(len(msg.Value))
			}

			d.lastWritten = next
		} else {
			return written, nil
		}
	}
}
