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

package relay

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/stretchr/testify/require"
)

func TestInterface(t *testing.T) {
	cases := []*replication.BinlogEvent{
		{RawData: []byte{1}},
		{RawData: []byte{2}},
		{RawData: []byte{3}},
	}

	cfg := &RConfig{
		SyncConfig: replication.BinlogSyncerConfig{
			ServerID: 101,
		},
		MasterID: "test-master",
	}

	// test with position
	r := NewUpstreamReader(cfg)
	testInterfaceWithReader(t, r, cases)

	// test with GTID
	cfg.EnableGTID = true
	r = NewUpstreamReader(cfg)
	testInterfaceWithReader(t, r, cases)
}

func testInterfaceWithReader(t *testing.T, r Reader, cases []*replication.BinlogEvent) {
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*upstreamReader)
	require.NotNil(t, concreteR)
	mockR := reader.NewMockReader()
	concreteR.in = mockR

	// start reader
	err := r.Start()
	require.NoError(t, err)
	err = r.Start() // call multi times
	require.Error(t, err)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	concreteMR := mockR.(*reader.MockReader)
	go func() {
		for _, cs := range cases {
			require.NoError(t, concreteMR.PushEvent(ctx, cs))
		}
	}()
	obtained := make([]*replication.BinlogEvent, 0, len(cases))
	for {
		result, err2 := r.GetEvent(ctx)
		require.Nil(t, err2)
		obtained = append(obtained, result.Event)
		if len(obtained) == len(cases) {
			break
		}
	}
	require.Equal(t, cases, obtained)

	// close reader
	err = r.Close()
	require.NoError(t, err)
	err = r.Close()
	require.Error(t, err) // call multi times

	// getEvent from a closed reader
	result, err := r.GetEvent(ctx)
	require.Error(t, err)
	require.Nil(t, result.Event)
}

func TestGetEventWithError(t *testing.T) {
	cfg := &RConfig{
		SyncConfig: replication.BinlogSyncerConfig{
			ServerID: 101,
		},
		MasterID: "test-master",
	}

	r := NewUpstreamReader(cfg)
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*upstreamReader)
	require.NotNil(t, concreteR)
	mockR := reader.NewMockReader()
	concreteR.in = mockR

	errOther := errors.New("other error")
	in := []error{
		context.Canceled,
		errOther,
	}
	expected := []error{
		context.Canceled,
		errOther,
	}

	err := r.Start()
	require.NoError(t, err)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concreteMR := mockR.(*reader.MockReader)
	go func() {
		for _, cs := range in {
			require.NoError(t, concreteMR.PushError(ctx, cs))
		}
	}()

	results := make([]error, 0, len(expected))
	for {
		_, err2 := r.GetEvent(ctx)
		require.NotNil(t, err2)
		results = append(results, errors.Cause(err2))
		if err2 == errOther {
			break // all received
		}
	}
	require.Equal(t, expected, results)
}
