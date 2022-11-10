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
	"github.com/google/btree"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
)

// defragmenter is used to handle event fragments which can be registered
// out of order.
type defragmenter struct {
	// the last output sequence number
	lastOutput uint64
	fragments  *btree.BTreeG[eventFragment]
	msgs       []*common.Message
}

// newDefragmenter creates a defragmenter.
func newDefragmenter() *defragmenter {
	return &defragmenter{
		fragments: btree.NewG[eventFragment](16, eventFragmentLess),
	}
}

// registerFrag adds an eventFragment.
func (d *defragmenter) registerFrag(frag eventFragment) {
	d.fragments.ReplaceOrInsert(frag)
}

// reassmeble reassmeble fragments in correct order.
func (d *defragmenter) reassmebleFrag() []*common.Message {
	for {
		frag, exist := d.fragments.DeleteMin()
		if !exist {
			break
		}

		if d.lastOutput+1 != frag.seqNumber {
			d.fragments.ReplaceOrInsert(frag)
			break
		}

		d.msgs = append(d.msgs, frag.encodedMsgs...)
		d.lastOutput++
	}

	result := d.msgs
	d.msgs = d.msgs[:0]
	return result
}
