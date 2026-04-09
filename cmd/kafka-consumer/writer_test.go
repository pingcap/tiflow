// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestUpdateWatermarkIgnoresFallbackFromNewerOffset(t *testing.T) {
	// Scenario: at-least-once delivery replays an older resolved event with a newer Kafka offset.
	// Steps: advance the partition watermark, then feed a smaller resolved ts from a larger offset.
	// Expectation: the consumer keeps the existing watermark instead of panicking.
	progress := &partitionProgress{partition: 1}
	progress.updateWatermark(120, kafka.Offset(10))

	require.NotPanics(t, func() {
		progress.updateWatermark(100, kafka.Offset(20))
	})
	require.Equal(t, uint64(120), progress.watermark)
	require.Equal(t, kafka.Offset(10), progress.watermarkOffset)
}

func TestAppendDDLIgnoresEquivalentReplay(t *testing.T) {
	// Scenario: Kafka replays the same DDL as a freshly decoded event object.
	// Steps: append one DDL, then append a separate object with identical logical fields.
	// Expectation: the writer keeps only one pending DDL.
	w := &writer{}
	ddl := &model.DDLEvent{
		StartTs:  100,
		CommitTs: 120,
		Query:    "create table t(id int primary key)",
		Seq:      1,
	}
	dup := &model.DDLEvent{
		StartTs:  100,
		CommitTs: 120,
		Query:    "create table t(id int primary key)",
		Seq:      1,
	}

	w.appendDDL(ddl, kafka.Offset(1))
	w.appendDDL(dup, kafka.Offset(2))

	require.Len(t, w.ddlList, 1)
	require.Same(t, ddl, w.ddlWithMaxCommitTs)
}

func TestAppendDDLKeepsSplitDDLSequenceDistinct(t *testing.T) {
	// Scenario: one logical DDL job is split into multiple DDL events with the same commitTs.
	// Steps: append two DDLs whose startTs, commitTs, and query text match but Seq differs.
	// Expectation: both events remain queued because Seq is part of the DDL identity.
	w := &writer{}
	first := &model.DDLEvent{
		StartTs:  200,
		CommitTs: 220,
		Query:    "rename tables split ddl",
		Seq:      1,
	}
	second := &model.DDLEvent{
		StartTs:  200,
		CommitTs: 220,
		Query:    "rename tables split ddl",
		Seq:      2,
	}

	w.appendDDL(first, kafka.Offset(3))
	w.appendDDL(second, kafka.Offset(4))

	require.Len(t, w.ddlList, 2)
	require.Same(t, second, w.ddlWithMaxCommitTs)
}

func TestAppendDDLIgnoresReplayedSplitDDLSequence(t *testing.T) {
	// Scenario: Kafka replays an already queued split DDL sequence after later parts were seen.
	// Steps: append the original split DDLs, then replay the same logical DDLs as fresh objects.
	// Expectation: the queue keeps only the original sequence and does not append duplicates.
	w := &writer{}
	first := &model.DDLEvent{
		StartTs:  300,
		CommitTs: 320,
		Query:    "rename table test.t1 to test.t2",
		Seq:      0,
	}
	second := &model.DDLEvent{
		StartTs:  300,
		CommitTs: 320,
		Query:    "rename table test.t3 to test.t4",
		Seq:      1,
	}
	replayFirst := &model.DDLEvent{
		StartTs:  300,
		CommitTs: 320,
		Query:    "rename table test.t1 to test.t2",
		Seq:      0,
	}
	replaySecond := &model.DDLEvent{
		StartTs:  300,
		CommitTs: 320,
		Query:    "rename table test.t3 to test.t4",
		Seq:      1,
	}

	w.appendDDL(first, kafka.Offset(10))
	w.appendDDL(second, kafka.Offset(11))
	w.appendDDL(replayFirst, kafka.Offset(12))
	w.appendDDL(replaySecond, kafka.Offset(13))

	require.Len(t, w.ddlList, 2)
	require.Same(t, first, w.ddlList[0])
	require.Same(t, second, w.ddlList[1])
	require.Same(t, second, w.ddlWithMaxCommitTs)
}

func TestAppendDDLKeepsDifferentDDLsWithSameCommitTsWithoutSeq(t *testing.T) {
	// Scenario: a multi-DDL job can emit different DDLs that share the same commitTs while Seq stays at zero.
	// Steps: append two distinct DDLs with the same timestamps but different queries and default Seq.
	// Expectation: both DDLs remain queued because Query is still part of the logical DDL identity.
	w := &writer{}
	first := &model.DDLEvent{
		StartTs:  400,
		CommitTs: 420,
		Query:    "create table test.t1(id int primary key)",
	}
	second := &model.DDLEvent{
		StartTs:  400,
		CommitTs: 420,
		Query:    "create table test.t2(id int primary key)",
	}

	w.appendDDL(first, kafka.Offset(20))
	w.appendDDL(second, kafka.Offset(21))

	require.Len(t, w.ddlList, 2)
	require.Same(t, first, w.ddlList[0])
	require.Same(t, second, w.ddlList[1])
	require.Same(t, second, w.ddlWithMaxCommitTs)
}
