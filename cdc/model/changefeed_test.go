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

package model

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/pkg/config"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
)

type configSuite struct{}

var _ = check.Suite(&configSuite{})

func (s *configSuite) TestFillV1(c *check.C) {
	v1Config := `
{
    "sink-uri":"blackhole://",
    "opts":{

    },
    "start-ts":417136892416622595,
    "target-ts":0,
    "admin-job-type":0,
    "sort-engine":"memory",
    "sort-dir":".",
    "config":{
        "case-sensitive":true,
        "filter":{
            "do-tables":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl1"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl2"
                }
            ],
            "do-dbs":[
                "test1",
                "sys1"
            ],
            "ignore-tables":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl3"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl4"
                }
            ],
            "ignore-dbs":[
                "test",
                "sys"
            ],
            "ignore-txn-start-ts":[
                1,
                2
            ],
            "ddl-allow-list":"AQI="
        },
        "mounter":{
            "worker-num":64
        },
        "sink":{
            "dispatch-rules":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl3",
                    "rule":"ts"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl4",
                    "rule":"rowid"
                }
            ]
        },
        "cyclic-replication":{
            "enable":false,
            "replica-id":1,
            "filter-replica-ids":[
                2,
                3
            ],
            "id-buckets":4,
            "sync-ddl":true
        }
    }
}
`
	cfg := &ChangeFeedInfo{}
	err := cfg.Unmarshal([]byte(v1Config))
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.DeepEquals, &ChangeFeedInfo{
		SinkURI: "blackhole://",
		Opts:    map[string]string{},
		StartTs: 417136892416622595,
		Engine:  "memory",
		SortDir: ".",
		Config: &config.ReplicaConfig{
			CaseSensitive: true,
			Filter: &config.FilterConfig{
				MySQLReplicationRules: &filter.MySQLReplicationRules{
					DoTables: []*filter.Table{{
						Schema: "test",
						Name:   "tbl1",
					}, {
						Schema: "test",
						Name:   "tbl2",
					}},
					DoDBs: []string{"test1", "sys1"},
					IgnoreTables: []*filter.Table{{
						Schema: "test",
						Name:   "tbl3",
					}, {
						Schema: "test",
						Name:   "tbl4",
					}},
					IgnoreDBs: []string{"test", "sys"},
				},
				IgnoreTxnStartTs: []uint64{1, 2},
				DDLAllowlist:     []model.ActionType{1, 2},
			},
			Mounter: &config.MounterConfig{
				WorkerNum: 64,
			},
			Sink: &config.SinkConfig{
				DispatchRules: []*config.DispatchRule{
					{Matcher: []string{"test.tbl3"}, Dispatcher: "ts"},
					{Matcher: []string{"test.tbl4"}, Dispatcher: "rowid"},
				},
			},
			Cyclic: &config.CyclicConfig{
				ReplicaID:       1,
				FilterReplicaID: []uint64{2, 3},
				IDBuckets:       4,
				SyncDDL:         true,
			},
		},
	})
}

func (s *configSuite) TestVerifyAndFix(c *check.C) {
	info := &ChangeFeedInfo{
		SinkURI: "blackhole://",
		Opts:    map[string]string{},
		StartTs: 417257993615179777,
		Config:  &config.ReplicaConfig{},
	}

	err := info.VerifyAndFix()
	c.Assert(err, check.IsNil)
	c.Assert(info.Engine, check.Equals, SortInMemory)

	marshalConfig1, err := info.Config.Marshal()
	c.Assert(err, check.IsNil)
	defaultConfig := config.GetDefaultReplicaConfig()
	defaultConfig.CaseSensitive = false
	marshalConfig2, err := defaultConfig.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(marshalConfig1, check.Equals, marshalConfig2)
}

type changefeedSuite struct{}

var _ = check.Suite(&changefeedSuite{})

func (s *changefeedSuite) TestCheckErrorHistory(c *check.C) {
	now := time.Now()
	info := &ChangeFeedInfo{
		ErrorHis: []int64{},
	}
	for i := 0; i < 5; i++ {
		tm := now.Add(-errorHistoryGCInterval)
		info.ErrorHis = append(info.ErrorHis, tm.UnixNano()/1e6)
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < errorHistoryThreshold-1; i++ {
		info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
	needSave, canInit := info.CheckErrorHistory()
	c.Assert(needSave, check.IsTrue)
	c.Assert(canInit, check.IsTrue)
	c.Assert(info.ErrorHis, check.HasLen, errorHistoryThreshold-1)

	info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
	needSave, canInit = info.CheckErrorHistory()
	c.Assert(needSave, check.IsFalse)
	c.Assert(canInit, check.IsFalse)
}
