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
	"github.com/pingcap/tiflow/pkg/util/testleak"
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
	c.Assert(b, check.Equals, `{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}}`)
	conf2 := new(ReplicaConfig)
	err = conf2.Unmarshal([]byte(`{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}}`))
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

type serverConfigSuite struct{}

var _ = check.Suite(&serverConfigSuite{})

func (s *serverConfigSuite) TestMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	rawConfig := `{"addr":"192.155.22.33:8887","advertise-addr":"","log-file":"","log-level":"info","log":{"file":{"max-size":300,"max-days":0,"max-backups":0}},"data-dir":"","gc-ttl":86400,"tz":"System","capture-session-ttl":10,"owner-flush-interval":200000000,"processor-flush-interval":100000000,"sorter":{"num-concurrent-worker":4,"chunk-size-limit":999,"max-memory-percentage":30,"max-memory-consumption":17179869184,"num-workerpool-goroutine":16,"sort-dir":"/tmp/sorter"},"security":{"ca-path":"","cert-path":"","key-path":"","cert-allowed-cn":null},"per-table-memory-quota":10485760,"kv-client":{"worker-concurrent":8,"worker-pool-size":0,"region-scan-limit":40}}`

	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	b, err := conf.Marshal()
	c.Assert(err, check.IsNil)

	c.Assert(b, check.Equals, rawConfig)
	conf2 := new(ServerConfig)
	err = conf2.Unmarshal([]byte(rawConfig))
	c.Assert(err, check.IsNil)
	c.Assert(conf2, check.DeepEquals, conf)
}

func (s *serverConfigSuite) TestClone(c *check.C) {
	defer testleak.AfterTest(c)()
	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	conf2 := conf.Clone()
	c.Assert(conf2, check.DeepEquals, conf)
	conf.Sorter.ChunkSizeLimit = 99
	c.Assert(conf.Sorter.ChunkSizeLimit, check.Equals, uint64(99))
}

func (s *serverConfigSuite) TestValidateAndAdjust(c *check.C) {
	defer testleak.AfterTest(c)()
	conf := new(ServerConfig)

	c.Assert(conf.ValidateAndAdjust(), check.ErrorMatches, ".*empty address")
	conf.Addr = "cdc:1234"
	c.Assert(conf.ValidateAndAdjust(), check.ErrorMatches, ".*empty GC TTL is not allowed")
	conf.GcTTL = 60
	c.Assert(conf.ValidateAndAdjust(), check.IsNil)
	c.Assert(conf.AdvertiseAddr, check.Equals, conf.Addr)
	conf.AdvertiseAddr = "advertise:1234"
	c.Assert(conf.ValidateAndAdjust(), check.IsNil)
	c.Assert(conf.Addr, check.Equals, "cdc:1234")
	c.Assert(conf.AdvertiseAddr, check.Equals, "advertise:1234")
	conf.AdvertiseAddr = "0.0.0.0:1234"
	c.Assert(conf.ValidateAndAdjust(), check.ErrorMatches, ".*must be specified.*")
	conf.Addr = "0.0.0.0:1234"
	c.Assert(conf.ValidateAndAdjust(), check.ErrorMatches, ".*must be specified.*")
	conf.AdvertiseAddr = "advertise"
	c.Assert(conf.ValidateAndAdjust(), check.ErrorMatches, ".*does not contain a port")
	conf.AdvertiseAddr = "advertise:1234"
	conf.PerTableMemoryQuota = 1
	c.Assert(conf.ValidateAndAdjust(), check.IsNil)
	c.Assert(uint64(1), check.Equals, conf.PerTableMemoryQuota)
	conf.PerTableMemoryQuota = 0
	c.Assert(conf.ValidateAndAdjust(), check.IsNil)
	c.Assert(GetDefaultServerConfig().PerTableMemoryQuota, check.Equals, conf.PerTableMemoryQuota)
}
