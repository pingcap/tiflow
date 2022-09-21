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

// defragmenter is used to handle event fragments which can be registered
// out of order. `writeMsgs` will decide whether an event fragment can be sent
// to the destination channel or it must wait the previous fragments arrive in defragmenter.
// e.g. five events numbered as 2,1,3,5,4 are registered to defragmenter.
// - event 2 cannot be sent right away because event 1 doesn't arrive.
// - event 1 arrives and it observes that event 2 is already there, so event 1~2 are sent.
// - event 3 arrives and no subsequent event (i.e. event 4) arrives, so event 3 is sent immediately.
// - event 5 cannot be sent right away because event 4 doesn't arrive.
// - event 4 arrives and it observes that event 5 is already there, so event 4~5 are sent.
type defragmenter struct {
	// the last written sequence number.
	lastWritten uint64
	// lastSeqNumber is used to mark the end of a series of eventFragment.
	lastSeqNumber uint64
	// the total written bytes.
	written    uint64
	future     map[uint64]eventFragment
	registryCh *chann.Chann[eventFragment]
}

// newDefragmenter creates a defragmenter.
func newDefragmenter() *defragmenter {
	return &defragmenter{
		future:     make(map[uint64]eventFragment),
		registryCh: chann.New[eventFragment](),
	}
}

// register pushes an eventFragment to registryCh.
func (d *defragmenter) register(frag eventFragment) {
	d.registryCh.In() <- frag
}

// writeMsgs write messages to desination channel in correct order.
func (d *defragmenter) writeMsgs(ctx context.Context, dst *chann.Chann[*common.Message]) (uint64, error) {
	for {
		// if we encounter an ending mark, push nil to desination channel so that we can finish processing
		// in the consumer side.
		if d.lastWritten >= d.lastSeqNumber && d.lastSeqNumber > 0 {
			dst.In() <- nil
			break
		}

		select {
		case <-ctx.Done():
			d.future = nil
			return 0, ctx.Err()
		case frag := <-d.registryCh.Out():
			// check whether we meet an ending mark.
			if frag.event == nil {
				d.lastSeqNumber = frag.seqNumber
				continue
			}

			// check whether to write messages to destination channel right now.
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

// writeMsgsConsecutive write consective messages as much as possible.
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
