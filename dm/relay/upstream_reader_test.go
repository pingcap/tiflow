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
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
)

var _ = check.Suite(&testRemoteReaderSuite{})

type testRemoteReaderSuite struct{}

func (t *testRemoteReaderSuite) TestInterface(c *check.C) {
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
	t.testInterfaceWithReader(c, r, cases)

	// test with GTID
	cfg.EnableGTID = true
	r = NewUpstreamReader(cfg)
	t.testInterfaceWithReader(c, r, cases)
}

func (t *testRemoteReaderSuite) testInterfaceWithReader(c *check.C, r Reader, cases []*replication.BinlogEvent) {
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*upstreamReader)
	c.Assert(concreteR, check.NotNil)
	mockR := reader.NewMockReader()
	concreteR.in = mockR

	// start reader
	err := r.Start()
	c.Assert(err, check.IsNil)
	err = r.Start() // call multi times
	c.Assert(err, check.NotNil)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	concreteMR := mockR.(*reader.MockReader)
	go func() {
		for _, cs := range cases {
			c.Assert(concreteMR.PushEvent(ctx, cs), check.IsNil)
		}
	}()
	obtained := make([]*replication.BinlogEvent, 0, len(cases))
	for {
		result, err2 := r.GetEvent(ctx)
		c.Assert(err2, check.IsNil)
		obtained = append(obtained, result.Event)
		if len(obtained) == len(cases) {
			break
		}
	}
	c.Assert(obtained, check.DeepEquals, cases)

	// close reader
	err = r.Close()
	c.Assert(err, check.IsNil)
	err = r.Close()
	c.Assert(err, check.NotNil) // call multi times

	// getEvent from a closed reader
	result, err := r.GetEvent(ctx)
	c.Assert(err, check.NotNil)
	c.Assert(result.Event, check.IsNil)
}

func (t *testRemoteReaderSuite) TestGetEventWithError(c *check.C) {
	cfg := &RConfig{
		SyncConfig: replication.BinlogSyncerConfig{
			ServerID: 101,
		},
		MasterID: "test-master",
	}

	r := NewUpstreamReader(cfg)
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*upstreamReader)
	c.Assert(concreteR, check.NotNil)
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
	c.Assert(err, check.IsNil)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concreteMR := mockR.(*reader.MockReader)
	go func() {
		for _, cs := range in {
			c.Assert(concreteMR.PushError(ctx, cs), check.IsNil)
		}
	}()

	results := make([]error, 0, len(expected))
	for {
		_, err2 := r.GetEvent(ctx)
		c.Assert(err2, check.NotNil)
		results = append(results, errors.Cause(err2))
		if err2 == errOther {
			break // all received
		}
	}
	c.Assert(results, check.DeepEquals, expected)
}
