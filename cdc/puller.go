// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"github.com/pingcap/tidb-cdc/kv_entry"
)

type Range struct {
	Start []byte
	End   []byte
}

type Puller interface {
	Pull(ranges []Range) <-chan kv_entry.RawKVEntry
}

// puller pull data from tikv and push changes into a buffer
type puller struct {
	checkpointTS uint64
	spans        []Span
	detail       ChangeFeedDetail
	buf          *buffer
}

func newPuller(
	checkpointTS uint64,
	spans []Span,
	detail ChangeFeedDetail,
	buf *buffer,
) *puller {
	p := &puller{
		checkpointTS: checkpointTS,
		spans:        spans,
		detail:       detail,
		buf:          buf,
	}

	return p
}

func (p *puller) Run(ctx context.Context) error {
	// TODO pull from tikv and push into buf
	// need buffer in memory first
	<-ctx.Done()
	return nil
}
