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

package config

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type replicaConfigSuite struct{}

var _ = check.Suite(&replicaConfigSuite{})

func (s *replicaConfigSuite) TestMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	b, err := conf.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(b, check.Equals, `{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}}`)
	conf2 := new(ReplicaConfig)
	err = conf2.Unmarshal([]byte(`{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}}`))
	c.Assert(err, check.IsNil)
	c.Assert(conf2, check.DeepEquals, conf)
}

func (s *replicaConfigSuite) TestClone(c *check.C) {
	defer testleak.AfterTest(c)()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf2 := conf.Clone()
	c.Assert(conf2, check.DeepEquals, conf)
	conf2.Mounter.WorkerNum = 4
	c.Assert(conf.Mounter.WorkerNum, check.Equals, 3)
}

func (s *replicaConfigSuite) TestOutDated(c *check.C) {
	defer testleak.AfterTest(c)()
	conf2 := new(ReplicaConfig)
	err := conf2.Unmarshal([]byte(`{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":3},"sink":{"dispatch-rules":[{"db-name":"a","tbl-name":"b","rule":"r1"},{"db-name":"a","tbl-name":"c","rule":"r2"},{"db-name":"a","tbl-name":"d","rule":"r2"}],"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}}`))
	c.Assert(err, check.IsNil)

	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, Dispatcher: "r1"},
		{Matcher: []string{"a.c"}, Dispatcher: "r2"},
		{Matcher: []string{"a.d"}, Dispatcher: "r2"},
	}
	c.Assert(conf2, check.DeepEquals, conf)
}
