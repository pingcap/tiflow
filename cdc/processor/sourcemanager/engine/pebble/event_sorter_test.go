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
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
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

	span := spanz.TableIDToComparableSpan(1)
	s.AddTable(span, 2)
	s.AddTable(span, 1)

	require.Equal(t, model.Ts(2), s.GetStatsByTable(span).ReceivedMaxResolvedTs)

	s.RemoveTable(span)
	s.RemoveTable(span)
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

	span := spanz.TableIDToComparableSpan(1)
	s.AddTable(span, 0)
	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ tablepb.Span, ts model.Ts) { resolvedTs <- ts })

	s.Add(span, model.NewResolvedPolymorphicEvent(0, 1))
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(span, engine.Position{}, engine.Position{CommitTs: ts})
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

	span := spanz.TableIDToComparableSpan(1)
	s.AddTable(span, 1)
	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ tablepb.Span, ts model.Ts) { resolvedTs <- ts })

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

	s.Add(span, inputEvents...)
	s.Add(span, model.NewResolvedPolymorphicEvent(0, 4))
	require.Equal(t, model.Ts(4), s.GetStatsByTable(span).ReceivedMaxResolvedTs)

	sortedEvents := make([]*model.PolymorphicEvent, 0, len(inputEvents))
	sortedPositions := make([]engine.Position, 0, len(inputEvents))

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(span, engine.Position{}, engine.Position{CommitTs: ts, StartTs: ts - 1})
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

	span := spanz.TableIDToComparableSpan(1)
	s.AddTable(span, 0)
	require.NoError(t, s.CleanByTable(spanz.TableIDToComparableSpan(2), engine.Position{}))
	require.Nil(t, s.CleanByTable(span, engine.Position{}))
}
