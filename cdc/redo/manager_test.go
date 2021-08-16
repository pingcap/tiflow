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

package redo

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type logManagerSuite struct{}

var _ = check.Suite(&logManagerSuite{})

func (s *logManagerSuite) TestConsistentConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	levelCases := []struct {
		level string
		valid bool
	}{
		{"normal", true},
		{"eventual", true},
		{"NORMAL", false},
		{"", false},
	}
	for _, lc := range levelCases {
		c.Assert(IsValidConsistentLevel(lc.level), check.Equals, lc.valid)
	}

	storageCases := []struct {
		storage string
		valid   bool
	}{
		{"local", true},
		{"s3", true},
		{"blackhole", true},
		{"Local", false},
		{"nfs", false},
		{"", false},
	}
	for _, sc := range storageCases {
		c.Assert(IsValidConsistentStorage(sc.storage), check.Equals, sc.valid)
	}
}

func (s *logManagerSuite) TestLogManagerInProcessor(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkResovledTs := func(mgr LogManager, expectedRts uint64) {
		time.Sleep(time.Millisecond*200 + updateRtsInterval)
		resolvedTs := mgr.GetMinResolvedTs()
		c.Assert(resolvedTs, check.Equals, expectedRts)
	}

	cfg := &ConsistentConfig{
		Level:   string(consistentLevelEventual),
		Storage: string(consistentStorageBlackhole),
	}
	errCh := make(chan error, 1)
	opts := &ManagerOptions{
		EnableBgRunner: true,
		ErrCh:          errCh,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	c.Assert(err, check.IsNil)

	// check emit row changed events can move forward resolved ts
	tables := []model.TableID{53, 55, 57, 59}
	startTs := uint64(100)
	for _, tableID := range tables {
		logMgr.AddTable(tableID, startTs)
	}
	testCases := []struct {
		tableID model.TableID
		rows    []*model.RowChangedEvent
	}{
		{
			tableID: 53,
			rows: []*model.RowChangedEvent{
				{CommitTs: 120, Table: &model.TableName{TableID: 53}},
				{CommitTs: 125, Table: &model.TableName{TableID: 53}},
				{CommitTs: 130, Table: &model.TableName{TableID: 53}},
			},
		},
		{
			tableID: 55,
			rows: []*model.RowChangedEvent{
				{CommitTs: 130, Table: &model.TableName{TableID: 55}},
				{CommitTs: 135, Table: &model.TableName{TableID: 55}},
			},
		},
		{
			tableID: 57,
			rows: []*model.RowChangedEvent{
				{CommitTs: 130, Table: &model.TableName{TableID: 57}},
			},
		},
		{
			tableID: 59,
			rows: []*model.RowChangedEvent{
				{CommitTs: 128, Table: &model.TableName{TableID: 59}},
				{CommitTs: 130, Table: &model.TableName{TableID: 59}},
				{CommitTs: 133, Table: &model.TableName{TableID: 59}},
			},
		},
	}
	for _, tc := range testCases {
		err := logMgr.EmitRowChangedEvents(ctx, tc.tableID, tc.rows...)
		c.Assert(err, check.IsNil)
	}
	checkResovledTs(logMgr, uint64(130))

	// check FlushLog can move forward the resolved ts when there is not row event.
	flushResolvedTs := uint64(150)
	for _, tableID := range tables {
		err := logMgr.FlushLog(ctx, tableID, flushResolvedTs)
		c.Assert(err, check.IsNil)
	}
	checkResovledTs(logMgr, flushResolvedTs)

	// check remove table can work normally
	removeTable := tables[len(tables)-1]
	tables = tables[:len(tables)-1]
	logMgr.RemoveTable(removeTable)
	flushResolvedTs = uint64(200)
	for _, tableID := range tables {
		err := logMgr.FlushLog(ctx, tableID, flushResolvedTs)
		c.Assert(err, check.IsNil)
	}
	checkResovledTs(logMgr, flushResolvedTs)
}

func (s *logManagerSuite) TestLogManagerInOwner(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &ConsistentConfig{
		Level:   string(consistentLevelEventual),
		Storage: string(consistentStorageBlackhole),
	}
	opts := &ManagerOptions{
		EnableBgRunner: false,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	c.Assert(err, check.IsNil)

	ddl := &model.DDLEvent{StartTs: 100, CommitTs: 120, Query: "CREATE TABLE `TEST.T1`"}
	err = logMgr.EmitDDLEvent(ctx, ddl)
	c.Assert(err, check.IsNil)

	err = logMgr.FlushResolvedAndCheckpointTs(ctx, 130 /*resolvedTs*/, 120 /*CheckPointTs*/)
	c.Assert(err, check.IsNil)
}
