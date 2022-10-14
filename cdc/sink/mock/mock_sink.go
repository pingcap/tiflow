// Copyright 2021 PingCAP, Inc.
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

package mock

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

// ReceivedData is the data received by mock sink.
// Only used in test.
type ReceivedData struct {
	ResolvedTs model.Ts
	Row        *model.RowChangedEvent
}

type mockSink struct {
	Received   []ReceivedData
	CloseTimes int
}

// NewNormalMockSink returns a mock sink for test.
func NewNormalMockSink() *mockSink {
	return &mockSink{}
}

func (s *mockSink) AddTable(tableID model.TableID) error {
	return nil
}

func (s *mockSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		s.Received = append(s.Received, ReceivedData{Row: row})
	}
	return nil
}

func (s *mockSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (s *mockSink) FlushRowChangedEvents(
	ctx context.Context, _ model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	s.Received = append(s.Received, ReceivedData{ResolvedTs: resolved.Ts})
	return resolved, nil
}

func (s *mockSink) EmitCheckpointTs(_ context.Context, _ uint64, _ []*model.TableInfo) error {
	panic("unreachable")
}

func (s *mockSink) Close(ctx context.Context) error {
	s.CloseTimes++
	return nil
}

func (s *mockSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *mockSink) Check(t *testing.T, expected []ReceivedData,
) {
	require.Equal(t, expected, s.Received)
}

func (s *mockSink) Reset() {
	s.Received = s.Received[:0]
}

type mockCloseControlSink struct {
	mockSink
	closeCh chan interface{}
}

// NewMockCloseControlSink returns a mock sink for test.
// It will close the closeCh when Close is called.
func NewMockCloseControlSink(closeCh chan interface{}) *mockCloseControlSink {
	return &mockCloseControlSink{
		mockSink: mockSink{},
		closeCh:  closeCh,
	}
}

func (s *mockCloseControlSink) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return nil
	}
}

type flushSink struct {
	mockSink
}

// NewMockFlushSink returns a mock sink for test.
// It will simulate the situation that the sink is fall back.
func NewMockFlushSink() *flushSink {
	return &flushSink{
		mockSink: mockSink{},
	}
}

// use to simulate the situation that resolvedTs return from sink manager
// fall back
var fallBackResolvedTs = uint64(10)

func (s *flushSink) FlushRowChangedEvents(
	ctx context.Context, _ model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	if resolved.Ts == fallBackResolvedTs {
		return model.NewResolvedTs(0), nil
	}
	return resolved, nil
}

type errorCloseSink struct {
	mockSink
}

// NewMockErrorCloseSink returns a mock sink for test.
// It will return error when Close is called.
func NewMockErrorCloseSink() *errorCloseSink {
	return &errorCloseSink{
		mockSink: mockSink{},
	}
}

func (e *errorCloseSink) Close(ctx context.Context) error {
	return errors.New("close sink failed")
}
