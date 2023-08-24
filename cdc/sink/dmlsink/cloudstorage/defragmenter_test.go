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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDeframenter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	inputCh := make(chan eventFragment)
	outputCh := chann.NewAutoDrainChann[eventFragment]()
	defrag := newDefragmenter(inputCh, []*chann.DrainableChann[eventFragment]{outputCh})
	eg.Go(func() error {
		return defrag.run(egCtx)
	})

	uri := "file:///tmp/test"
	txnCnt := 50
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewTxnEventEncoderBuilder(encoderConfig)
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
				versionedTable: cloudstorage.VersionedTableName{
					TableNameWithPhysicTableID: model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
				},
				seqNumber: seq,
				event: &dmlsink.TxnCallbackableEvent{
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
					ColInfos: []rowcodec.ColInfo{
						{
							ID:            1,
							IsPKHandle:    false,
							VirtualGenCol: false,
							Ft:            types.NewFieldType(mysql.TypeLong),
						},
						{
							ID:            2,
							IsPKHandle:    false,
							VirtualGenCol: false,
							Ft:            types.NewFieldType(mysql.TypeString),
						},
					},
				}
				frag.event.Event.Rows = append(frag.event.Event.Rows, row)
			}
			err := encoder.AppendTxnEvent(frag.event.Event, nil)
			require.NoError(t, err)
			frag.encodedMsgs = encoder.Build()

			for _, msg := range frag.encodedMsgs {
				msg.Key = []byte(strconv.Itoa(int(seq)))
			}
			inputCh <- frag
		}(uint64(i + 1))
	}

	prevSeq := 0
LOOP:
	for {
		select {
		case frag := <-outputCh.Out():
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
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}
