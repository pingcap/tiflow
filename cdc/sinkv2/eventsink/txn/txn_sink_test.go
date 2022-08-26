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
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	mock_txn "github.com/pingcap/tiflow/cdc/sinkv2/eventsink/txn/mock"
	"github.com/stretchr/testify/require"
)

type blackhole struct {
	blockOnEvents int32
	panicOnFlush  int32
	noAutoFlush   int32
}

func (b *blackhole) OnTxnEvent(e *eventsink.TxnCallbackableEvent) bool {
	for {
		if atomic.LoadInt32(&b.blockOnEvents) > 0 {
			time.Sleep(time.Millisecond * time.Duration(100))
		} else {
			break
		}
	}
	e.Callback()
	return true
}

func (b *blackhole) Flush(ctx context.Context) error {
	if atomic.LoadInt32(&b.panicOnFlush) > 0 {
		panic("blackhole panics")
	}
	return nil
}

func (b *blackhole) MaxFlushInterval() time.Duration {
	if atomic.LoadInt32(&b.noAutoFlush) > 0 {
		return time.Second * time.Duration(86400)
	}
	return 100 * time.Millisecond
}

func (b *blackhole) Close() error {
	return nil
}

// TestTxnSinkNolocking checks TxnSink must be nonblocking even if the associated
// backends can be blocked in OnTxnEvent.
func TestTxnSinkNolocking(t *testing.T) {
	t.Parallel()

	bes := make([]backend, 0, 4)
	for i := 0; i < 4; i++ {
		bes = append(bes, &blackhole{blockOnEvents: 1})
	}
	errCh := make(chan error, 1)
	sink := newSink(context.Background(), bes, errCh, DefaultConflictDetectorSlots)

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
		atomic.StoreInt32(&be.(*blackhole).blockOnEvents, 0)
	}
	time.Sleep(time.Second)
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

func TestTxnSinkClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	// Create a sink with 3 mock backends. All of them will fail on `Flush`.
	bes := make([]backend, 0, 3)
	for i := 0; i < 3; i++ {
		mockBackend := mock_txn.NewMockbackend(gomock.NewController(t))
		if i == 0 {
			mockBackend.EXPECT().MaxFlushInterval().Return(10 * time.Millisecond)
			mockBackend.EXPECT().Flush(ctx).Return(errors.New("injected flush error"))
		} else {
			mockBackend.EXPECT().MaxFlushInterval().Return(86400 * time.Second)
		}
		mockBackend.EXPECT().Close().Return(nil)
		bes = append(bes, mockBackend)
	}

	// Wait a while so that the first worker can meet the error.
	errCh := make(chan error, 1)
	sink := newSink(ctx, bes, errCh, DefaultConflictDetectorSlots)
	time.Sleep(100 * time.Millisecond)
	select {
	case <-errCh:
	default:
		log.Fatal("a flush error is expected")
	}

	// sink.Close can close all background goroutines. No goroutines should be leak.
	require.Nil(t, sink.Close())
	select {
	case <-errCh:
		log.Fatal("no error is expected")
	default:
	}

	err := sink.WriteEvents(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{},
	})
	require.NotNil(t, err)
	cancel()
}

func TestBackendPanic(t *testing.T) {
	t.Parallel()

	bes := make([]backend, 0, 1)
	bes = append(bes, &blackhole{panicOnFlush: 1})
	errCh := make(chan error, 1)
	sink := newSink(context.Background(), bes, errCh, DefaultConflictDetectorSlots)

	time.Sleep(1 * time.Second)
	select {
	case <-errCh:
	default:
		panic("should get an error")
	}
	require.Nil(t, sink.Close())
}
