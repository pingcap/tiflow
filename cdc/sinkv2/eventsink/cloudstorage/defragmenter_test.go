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
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDeframenter(t *testing.T) {
	defrag := newDefragmenter()
	uri := "file:///tmp/test"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, config.ProtocolCanalJSON,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	require.Nil(t, err)

	for i := 0; i < 50; i++ {
		go func(seq uint64) {
			encoder := encoderBuilder.Build()
			frag := eventFragment{
				tableName: model.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				seqNumber: seq,
				event: &eventsink.TxnCallbackableEvent{
					Event: &model.SingleTableTxn{},
				},
			}

			rand.Seed(time.Now().UnixNano())
			n := 1 + rand.Intn(1000)
			for j := 0; j < n; j++ {
				row := &model.RowChangedEvent{
					Table: &model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
					Columns: []*model.Column{
						{Name: "c1", Value: j + 1},
						{Name: "c2", Value: "hello world"},
					},
				}
				frag.event.Event.Rows = append(frag.event.Event.Rows, row)
				encoder.AppendRowChangedEvent(ctx, "", row, nil)
			}
			frag.encodedMsgs = encoder.Build()

			for _, msg := range frag.encodedMsgs {
				msg.Key = []byte(strconv.Itoa(int(seq)))
			}
			defrag.register(frag)
		}(uint64(i + 1))
	}
	defrag.register(eventFragment{seqNumber: 50})

	dstCh := chann.New[*common.Message]()
	_, err = defrag.writeMsgs(ctx, dstCh)
	require.Nil(t, err)
	prevSeq := 0
	for msg := range dstCh.Out() {
		if msg == nil {
			break
		}
		curSeq, err := strconv.Atoi(string(msg.Key))
		require.Nil(t, err)
		require.GreaterOrEqual(t, curSeq, prevSeq)
		prevSeq = curSeq
	}
}
