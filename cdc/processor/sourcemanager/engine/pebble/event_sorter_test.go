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

package pebble

import (
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTableOperations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := New(cf, []*pebble.DB{db})
	defer s.Close()

	require.True(t, s.IsTableBased())

	s.AddTable(1)
	s.AddTable(1)

	require.Equal(t, model.Ts(0), s.GetStatsByTable(1).ReceivedMaxResolvedTs)

	s.RemoveTable(1)
	s.RemoveTable(1)
}

// TestNoResolvedTs tests resolved timestamps shouldn't be emitted.
func TestNoResolvedTs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := New(cf, []*pebble.DB{db})
	defer s.Close()

	require.True(t, s.IsTableBased())

	s.AddTable(1)
	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	s.Add(model.TableID(1), model.NewResolvedPolymorphicEvent(0, 1))
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(model.TableID(1), engine.Position{}, engine.Position{CommitTs: ts})
		event, _, err := iter.Next()
		require.Nil(t, event)
		require.Nil(t, err)
	case <-timer.C:
		panic("must get a resolved timestamp instead of timeout")
	}
}

// TestEventFetch tests events can be sorted correctly.
func TestEventFetch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := New(cf, []*pebble.DB{db})
	defer s.Close()

	require.True(t, s.IsTableBased())

	s.AddTable(1)
	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	inputEvents := []*model.PolymorphicEvent{
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 3,
			CRTs:    4,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 2,
			CRTs:    4,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
	}

	s.Add(1, inputEvents...)
	s.Add(model.TableID(1), model.NewResolvedPolymorphicEvent(0, 4))

	sortedEvents := make([]*model.PolymorphicEvent, 0, len(inputEvents))
	sortedPositions := make([]engine.Position, 0, len(inputEvents))

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(1, engine.Position{}, engine.Position{CommitTs: ts, StartTs: ts - 1})
		for {
			event, pos, err := iter.Next()
			require.Nil(t, err)
			if event == nil {
				break
			}
			sortedEvents = append(sortedEvents, event)
			sortedPositions = append(sortedPositions, pos)
		}
	case <-timer.C:
		panic("must get a resolved timestamp instead of timeout")
	}

	sort.Slice(inputEvents, func(i, j int) bool {
		return model.ComparePolymorphicEvents(inputEvents[i], inputEvents[j])
	})

	require.Equal(t, inputEvents, sortedEvents)

	expectPositions := []engine.Position{
		{CommitTs: 0, StartTs: 0},
		{CommitTs: 2, StartTs: 1},
		{CommitTs: 4, StartTs: 2},
		{CommitTs: 4, StartTs: 3},
	}
	require.Equal(t, expectPositions, sortedPositions)
}

func TestCleanData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := New(cf, []*pebble.DB{db})
	defer s.Close()

	require.True(t, s.IsTableBased())

	s.AddTable(1)
	require.NoError(t, s.CleanByTable(2, engine.Position{}))
	require.Nil(t, s.CleanByTable(1, engine.Position{}))
}

func TestEventFetchWithLargeData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := New(cf, []*pebble.DB{db})
	defer s.Close()

	require.True(t, s.IsTableBased())
	tableID := model.TableID(1)
	s.AddTable(tableID)

	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	// 生成测试数据
	const (
		numTransactions = 1000   // 1000个事务
		rowsPerTrans    = 200000 // 每个事务20万行
	)

	// 用于记录写入的事件总数
	totalEvents := numTransactions * rowsPerTrans

	// 生成并写入事件
	t.Logf("开始生成 %d 个事务，每个事务 %d 行数据", numTransactions, rowsPerTrans)
	startTime := time.Now()

	for i := 0; i < numTransactions; i++ {
		events := make([]*model.PolymorphicEvent, 0, rowsPerTrans)
		commitTs := model.Ts(i + 1)

		for j := 0; j < rowsPerTrans; j++ {
			key := []byte{byte(tableID)}
			event := model.NewPolymorphicEvent(&model.RawKVEntry{
				OpType:  model.OpTypePut,
				Key:     key,
				StartTs: commitTs - 1,
				CRTs:    commitTs,
			})
			events = append(events, event)
		}

		s.Add(tableID, events...)
		s.Add(tableID, model.NewResolvedPolymorphicEvent(0, commitTs))

		if (i+1)%100 == 0 {
			t.Logf("已完成 %d 个事务的写入", i+1)
		}
	}

	writeDuration := time.Since(startTime)
	t.Logf("数据写入完成，耗时: %v", writeDuration)

	// 读取并验证数据
	t.Log("开始读取数据...")
	readStartTime := time.Now()

	var (
		readCount     int64
		expectedCount = int64(totalEvents)
	)

	timer := time.NewTimer(30 * time.Minute) // 设置较长的超时时间
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(1, engine.Position{}, engine.Position{CommitTs: ts, StartTs: ts - 1})
		for {
			event, _, err := iter.Next()
			require.Nil(t, err)
			if event == nil {
				break
			}
			atomic.AddInt64(&readCount, 1)

			// 每读取10万条数据打印一次进度
			if readCount%100000 == 0 {
				t.Logf("已读取 %d 条数据", readCount)
			}
		}
	case <-timer.C:
		t.Fatal("读取数据超时")
	}

	readDuration := time.Since(readStartTime)
	t.Logf("数据读取完成，耗时: %v", readDuration)

	// 验证数据完整性
	require.Equal(t, expectedCount, readCount,
		"读取的数据条数(%d)与写入的数据条数(%d)不匹配",
		readCount, expectedCount)

	t.Logf("测试完成:")
	t.Logf("- 写入数据: %d 条", totalEvents)
	t.Logf("- 读取数据: %d 条", readCount)
	t.Logf("- 写入耗时: %v", writeDuration)
	t.Logf("- 读取耗时: %v", readDuration)
}
