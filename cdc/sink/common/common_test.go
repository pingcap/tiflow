// Copyright 2020 PingCAP, Inc.
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

package common

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type SinkCommonSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&SinkCommonSuite{})

func (s SinkCommonSuite) TestSplitResolvedTxn(c *check.C) {
	testCases := []struct {
		unresolvedTxns         map[model.TableName][]*model.Txn
		resolvedTs             uint64
		expectedResolvedTxns   map[model.TableName][]*model.Txn
		expectedUnresolvedTxns map[model.TableName][]*model.Txn
		expectedMinTs          uint64
	}{{
		unresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		resolvedTs:           5,
		expectedResolvedTxns: map[model.TableName][]*model.Txn{},
		expectedUnresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		expectedMinTs: 5,
	}, {
		unresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		resolvedTs: 23,
		expectedResolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}},
			{Table: "t2"}: {{CommitTs: 23}},
		},
		expectedUnresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		expectedMinTs: 11,
	}, {
		unresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		resolvedTs: 30,
		expectedResolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		expectedUnresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 33}, {CommitTs: 34}},
		},
		expectedMinTs: 11,
	}, {
		unresolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		resolvedTs: 40,
		expectedResolvedTxns: map[model.TableName][]*model.Txn{
			{Table: "t1"}: {{CommitTs: 11}, {CommitTs: 21}, {CommitTs: 21}, {CommitTs: 23}, {CommitTs: 33}, {CommitTs: 34}},
			{Table: "t2"}: {{CommitTs: 23}, {CommitTs: 24}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 26}, {CommitTs: 29}},
		},
		expectedUnresolvedTxns: map[model.TableName][]*model.Txn{},
		expectedMinTs:          11,
	}}
	for _, tc := range testCases {
		minTs, resolvedTxns := splitResolvedTxn(tc.resolvedTs, tc.unresolvedTxns)
		c.Assert(minTs, check.Equals, tc.expectedMinTs)
		c.Assert(resolvedTxns, check.DeepEquals, tc.expectedResolvedTxns)
		c.Assert(tc.unresolvedTxns, check.DeepEquals, tc.expectedUnresolvedTxns)
	}
}
