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

package syncer

import (
	"math"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

func (s *testSyncerSuite) TestDetectConflict() {
	ca := &causality{
		relation: newCausalityRelation(),
	}
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}

	assertRelationsEq := func(expectMap map[string]string) {
		s.Require().Equal(len(expectMap), ca.relation.len())
		for k, expV := range expectMap {
			v, ok := ca.relation.get(k)
			s.Require().True(ok)
			s.Require().Equal(expV, v)
		}
	}

	s.Require().False(ca.detectConflict(caseData))
	ca.add(caseData)
	assertRelationsEq(excepted)
	s.Require().False(ca.detectConflict([]string{"test_4"}))
	ca.add([]string{"test_4"})
	excepted["test_4"] = "test_4"
	assertRelationsEq(excepted)
	conflictData := []string{"test_4", "test_3"}
	s.Require().True(ca.detectConflict(conflictData))
	ca.relation.clear()
	s.Require().Equal(0, ca.relation.len())
}

func TestCausality(t *testing.T) {
	t.Parallel()

	schemaStr := "create table tb(a int primary key, b int unique);"
	ti := mockTableInfo(t, schemaStr)

	jobCh := make(chan *job, 10)
	syncer := &Syncer{
		cfg: &config.SubTaskConfig{
			SyncerConfig: config.SyncerConfig{
				QueueSize: 1024,
			},
			Name:     "task",
			SourceID: "source",
		},
		tctx:           tcontext.Background().WithLogger(log.L()),
		sessCtx:        utils.NewSessionCtx(map[string]string{"time_zone": "UTC"}),
		metricsProxies: &metrics.Proxies{},
	}
	syncer.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source")
	causalityCh := causalityWrap(jobCh, syncer)
	testCases := []struct {
		preVals  []interface{}
		postVals []interface{}
	}{
		{
			postVals: []interface{}{1, 2},
		},
		{
			postVals: []interface{}{2, 3},
		},
		{
			preVals:  []interface{}{2, 3},
			postVals: []interface{}{3, 4},
		},
		{
			preVals: []interface{}{1, 2},
		},
		{
			postVals: []interface{}{1, 3},
		},
	}
	results := []opType{dml, dml, dml, dml, conflict, dml}
	table := &cdcmodel.TableName{Schema: "test", Table: "t1"}
	location := binlog.MustZeroLocation(mysql.MySQLFlavor)
	ec := &eventContext{startLocation: location, endLocation: location, lastLocation: location}

	for _, tc := range testCases {
		change := sqlmodel.NewRowChange(table, nil, tc.preVals, tc.postVals, ti, nil, nil)
		job := newDMLJob(change, ec)
		jobCh <- job
	}

	require.Eventually(t, func() bool {
		return len(causalityCh) == len(results)
	}, 3*time.Second, 100*time.Millisecond)

	for _, op := range results {
		job := <-causalityCh
		require.Equal(t, op, job.tp)
	}
}

func (s *testSyncerSuite) TestCasualityRelation() {
	rm := newCausalityRelation()
	s.Require().Equal(0, rm.len())
	s.Require().Equal(1, len(rm.groups))

	testCases := []struct {
		key string
		val string
	}{
		{key: "1.key", val: "1.val"},
		{key: "2.key", val: "2.val"},
		{key: "3.key", val: "3.val"},
		{key: "4.key", val: "4.val"},
		{key: "5.key", val: "5.val"},
		{key: "6.key", val: "6.val"},
		{key: "7.key", val: "7.val"},
		{key: "8.key", val: "8.val"},
		{key: "9.key", val: "9.val"},
		{key: "10.key", val: "10.val"},
		{key: "11.key", val: "11.val"},
	}

	// test without rotate
	for _, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
	}

	s.Require().Equal(len(testCases), rm.len())

	for _, testcase := range testCases {
		val, ok := rm.get(testcase.key)
		s.Require().Equal(true, ok)
		s.Require().Equal(testcase.val, val)
	}

	rm.rotate(1)
	rm.gc(1)
	s.Require().Equal(0, rm.len())

	// test gc max
	for _, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
	}

	rm.gc(math.MaxInt64)
	s.Require().Equal(0, rm.len())

	// test with rotate
	for index, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
		rm.rotate(int64(index))
	}

	s.Require().Equal(len(testCases), rm.len())

	for _, testcase := range testCases {
		val, ok := rm.get(testcase.key)
		s.Require().Equal(true, ok)
		s.Require().Equal(testcase.val, val)
	}

	for index := range testCases {
		rm.gc(int64(index))

		for _, rmMap := range rm.groups[1:] {
			s.Require().NotEqual(int64(index), rmMap.prevFlushJobSeq)
		}

		for ti := 0; ti < index; ti++ {
			_, ok := rm.get(testCases[ti].key)
			s.Require().Equal(false, ok)
		}
	}

	rm.clear()
	s.Require().Equal(0, rm.len())
}
