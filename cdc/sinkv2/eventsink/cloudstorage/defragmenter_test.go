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
	"math/rand"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDeframenter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defrag := newDefragmenter(ctx)
	uri := "file:///tmp/test"
	txnCnt := 50
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, config.ProtocolCanalJSON,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	require.Nil(t, err)

	var seqNumbers []uint64
	for i := 0; i < txnCnt; i++ {
		seqNumbers = append(seqNumbers, uint64(i+1))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(seqNumbers), func(i, j int) {
		seqNumbers[i], seqNumbers[j] = seqNumbers[j], seqNumbers[i]
	})

	for i := 0; i < txnCnt; i++ {
		go func(seq uint64) {
			encoder := encoderBuilder.Build()
			frag := eventFragment{
				versionedTable: versionedTable{
					TableName: model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
				},
				seqNumber: seq,
				event: &eventsink.TxnCallbackableEvent{
					Event: &model.SingleTableTxn{},
				},
			}

			rand.Seed(time.Now().UnixNano())
			n := 1 + rand.Intn(1000)
			for j := 0; j < n; j++ {
				row := (&model.BoundedRowChangedEvent{
					Table: &model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
					Columns: []*model.BoundedColumn{
						{Name: "c1", Value: j + 1},
						{Name: "c2", Value: "hello world"},
					},
				}).Unbound()
				frag.event.Event.Rows = append(frag.event.Event.Rows, row)
				encoder.AppendRowChangedEvent(ctx, "", row, nil)
			}
			frag.encodedMsgs = encoder.Build()

			for _, msg := range frag.encodedMsgs {
				msg.Key = []byte(strconv.Itoa(int(seq)))
			}
			defrag.registerFrag(frag)
		}(uint64(i + 1))
	}

	prevSeq := 0
LOOP:
	for {
		select {
		case frag := <-defrag.orderedOut():
			for _, msg := range frag.encodedMsgs {
				curSeq, err := strconv.Atoi(string(msg.Key))
				require.Nil(t, err)
				require.GreaterOrEqual(t, curSeq, prevSeq)
				prevSeq = curSeq
			}
		case <-time.After(5 * time.Second):
			break LOOP
		}
	}
	cancel()
	defrag.close()
}
