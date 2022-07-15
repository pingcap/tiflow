// Copyright 2022 PingCAP, Inc.
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

package txn

import (
	"bytes"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/stretchr/testify/require"
)

type blackhole struct {
	block int32
	n     notify.Notifier
}

func (b *blackhole) onTxnEvent(e *eventsink.TxnCallbackableEvent) error {
	for {
		if atomic.LoadInt32(&b.block) > 0 {
			time.Sleep(time.Millisecond * time.Duration(100))
		} else {
			break
		}
	}
	e.Callback()
	return nil
}

func (b *blackhole) onNotify() error {
	return nil
}

func (b *blackhole) notifier() *notify.Receiver {
	receiver, _ := b.n.NewReceiver(time.Second * time.Duration(1000000))
	return receiver
}

func TestTxnSink(t *testing.T) {
	t.Parallel()

	bes := make([]backend, 0, 4)
	for i := 0; i < 4; i++ {
		bes = append(bes, &blackhole{block: int32(1), n: notify.Notifier{}})
	}
	sink := newSink(bes)

	// Test `WriteEvents` shouldn't be blocked by slow workers.
	var handled uint32 = 0
	for i := 0; i < 100; i++ {
		e := &eventsink.CallbackableEvent[*model.SingleTableTxn]{
			Event: &model.SingleTableTxn{
				Rows: []*model.RowChangedEvent{
					{
						Table: &model.TableName{Schema: "test", Table: "t1"},
						Columns: []*model.Column{
							{Name: "a", Value: 1},
							{Name: "b", Value: 2},
						},
					},
				},
			},
			Callback: func() { atomic.AddUint32(&handled, 1) },
		}
		sink.WriteEvents(e)
	}

	time.Sleep(time.Second)
	require.Equal(t, uint32(0), atomic.LoadUint32(&handled))

	for _, be := range bes {
		atomic.StoreInt32(&be.(*blackhole).block, 0)
	}
	time.Sleep(time.Duration(100) * time.Millisecond)
	require.Equal(t, uint32(100), atomic.LoadUint32(&handled))

	require.Nil(t, sink.Close())
}

func TestGenKeys(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		txn      *model.SingleTableTxn
		expected [][]byte
	}{{
		txn:      &model.SingleTableTxn{},
		expected: nil,
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 12,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1, 2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
		},
		expected: [][]byte{
			{'1', '2', 0x0, '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, '2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 12,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1}, {2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1}, {2}},
				},
			},
		},
		expected: [][]byte{
			{'2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', '2', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.NullableFlag,
						Value: nil,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.NullableFlag,
						Value: nil,
					}},
					IndexColumns: [][]int{{1}, {2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1}, {2}},
				},
			},
		},
		expected: [][]uint8{
			{'2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}}
	for _, tc := range testCases {
		keys := genTxnKeys(tc.txn)
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) > 0
		})
		require.Equal(t, tc.expected, keys)
	}
}
