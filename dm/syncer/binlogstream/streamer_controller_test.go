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

package binlogstream

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/stretchr/testify/require"
)

func TestToBinlogType(t *testing.T) {
	testCases := []struct {
		relay relay.Process
		tp    BinlogType
	}{
		{
			&relay.Relay{},
			LocalBinlog,
		}, {
			nil,
			RemoteBinlog,
		},
	}

	for _, testCase := range testCases {
		tp := RelayToBinlogType(testCase.relay)
		require.Equal(t, testCase.tp, tp)
	}
}

func TestCanErrorRetry(t *testing.T) {
	relay2 := &relay.Relay{}
	controller := NewStreamerController(
		replication.BinlogSyncerConfig{},
		true,
		nil,
		"",
		nil,
		relay2,
		log.L(),
	)

	mockErr := errors.New("test")

	// local binlog puller can always retry
	for i := 0; i < 5; i++ {
		require.True(t, controller.CanRetry(mockErr))
	}

	origCfg := minErrorRetryInterval
	minErrorRetryInterval = 100 * time.Millisecond
	defer func() {
		minErrorRetryInterval = origCfg
	}()

	// test with remote binlog
	controller = NewStreamerController(
		replication.BinlogSyncerConfig{},
		true,
		nil,
		"",
		nil,
		nil,
		log.L(),
	)

	require.True(t, controller.CanRetry(mockErr))
	require.False(t, controller.CanRetry(mockErr))
	time.Sleep(100 * time.Millisecond)
	require.True(t, controller.CanRetry(mockErr))
}

type mockStream struct {
	i      int
	events []*replication.BinlogEvent
}

func (m *mockStream) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if m.i < len(m.events) {
		e := m.events[m.i]
		m.i++
		return e, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type mockStreamProducer struct {
	stream *mockStream
}

func (m *mockStreamProducer) GenerateStreamer(location binlog.Location) (reader.Streamer, error) {
	return m.stream, nil
}

type expectedInfo struct {
	pos    uint32
	suffix int
	data   []byte
	op     pb.ErrorOp
}

func TestGetEventWithInject(t *testing.T) {
	upstream := &mockStream{
		events: []*replication.BinlogEvent{
			{
				Header: &replication.EventHeader{LogPos: 1010, EventSize: 10},
			},
			{
				Header:  &replication.EventHeader{LogPos: 1020, EventSize: 10},
				RawData: []byte("should inject before me at 1010"),
			},
		},
	}
	producer := &mockStreamProducer{upstream}

	controller := NewStreamerController4Test(producer, upstream)
	controller.currBinlogFile = "bin.000001"

	injectReq := &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Inject,
		BinlogPos: "(bin.000001, 1010)",
	}
	injectEvents := []*replication.BinlogEvent{
		{
			Header:  &replication.EventHeader{},
			RawData: []byte("inject 1"),
		},
		{
			Header:  &replication.EventHeader{},
			RawData: []byte("inject 2"),
		},
	}
	err := controller.Set(injectReq, injectEvents)
	require.NoError(t, err)
	controller.streamModifier.reset(binlog.Location{Position: mysql.Position{
		Name: "bin.000001",
		Pos:  1000,
	}})

	expecteds := []expectedInfo{
		{1010, 0, nil, pb.ErrorOp_InvalidErrorOp},
		{1010, 1, []byte("inject 1"), pb.ErrorOp_Inject},
		{1010, 2, []byte("inject 2"), pb.ErrorOp_Inject},
		{1020, 0, []byte("should inject before me at 1010"), pb.ErrorOp_InvalidErrorOp},
	}

	checkGetEvent(t, controller, expecteds)
}

func TestGetEventWithReplace(t *testing.T) {
	upstream := &mockStream{
		events: []*replication.BinlogEvent{
			{
				Header: &replication.EventHeader{LogPos: 1010, EventSize: 10},
			},
			{
				Header:  &replication.EventHeader{LogPos: 1020, EventSize: 10},
				RawData: []byte("should replace me at 1010"),
			},
		},
	}
	producer := &mockStreamProducer{upstream}

	controller := NewStreamerController4Test(producer, upstream)
	controller.currBinlogFile = "bin.000001"

	replaceReq := &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Replace,
		BinlogPos: "(bin.000001, 1010)",
	}
	replaceEvents := []*replication.BinlogEvent{
		{
			Header:  &replication.EventHeader{},
			RawData: []byte("replace 1"),
		},
		{
			Header:  &replication.EventHeader{},
			RawData: []byte("replace 2"),
		},
	}
	err := controller.Set(replaceReq, replaceEvents)
	require.NoError(t, err)
	controller.streamModifier.reset(binlog.Location{Position: mysql.Position{
		Name: "bin.000001",
		Pos:  1000,
	}})

	expecteds := []expectedInfo{
		{1010, 0, nil, pb.ErrorOp_InvalidErrorOp},
		{1010, 1, []byte("replace 1"), pb.ErrorOp_Replace},
		{1020, 0, []byte("replace 2"), pb.ErrorOp_Replace},
	}

	checkGetEvent(t, controller, expecteds)
}

func TestGetEventWithSkip(t *testing.T) {
	upstream := &mockStream{
		events: []*replication.BinlogEvent{
			{
				Header: &replication.EventHeader{LogPos: 1010, EventSize: 10},
			},
			{
				Header:  &replication.EventHeader{LogPos: 1020, EventSize: 10},
				RawData: []byte("should skip me at 1010"),
			},
		},
	}
	producer := &mockStreamProducer{upstream}

	controller := NewStreamerController4Test(producer, upstream)
	controller.currBinlogFile = "bin.000001"

	replaceReq := &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Skip,
		BinlogPos: "(bin.000001, 1010)",
	}
	err := controller.Set(replaceReq, nil)
	require.NoError(t, err)
	controller.streamModifier.reset(binlog.Location{Position: mysql.Position{
		Name: "bin.000001",
		Pos:  1000,
	}})

	expecteds := []expectedInfo{
		{1010, 0, nil, pb.ErrorOp_InvalidErrorOp},
		{1020, 0, []byte("should skip me at 1010"), pb.ErrorOp_Skip},
	}

	checkGetEvent(t, controller, expecteds)
}

func checkGetEvent(t *testing.T, controller *StreamerController, expecteds []expectedInfo) {
	ctx := tcontext.Background()
	for i, expected := range expecteds {
		t.Logf("#%d", i)
		event, suffix, op, err := controller.GetEvent(ctx)
		require.NoError(t, err)
		require.Equal(t, expected.pos, event.Header.LogPos)
		require.Equal(t, expected.suffix, suffix)
		require.Equal(t, expected.op, op)
	}
	ctx, cancel := ctx.WithTimeout(10 * time.Millisecond)
	defer cancel()
	_, _, _, err := controller.GetEvent(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
