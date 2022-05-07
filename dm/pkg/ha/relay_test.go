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

package ha

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestGetRelayConfigEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		worker  = "dm-worker-1"
		source  = "mysql-replica-1"
		source2 = "mysql-replica-2"
	)
	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	c.Assert(err, IsNil)
	cfg.SourceID = source
	cfgs := []*config.SourceConfig{cfg}
	// no relay source and config
	cfgs1, rev1, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(cfgs1, HasLen, 0)

	rev2, err := PutRelayConfig(etcdTestCli, source, worker)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get relay source and config, but config is empty
	_, _, err = GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, ErrorMatches, ".*doesn't have related source config in etcd.*")

	rev3, err := PutSourceCfg(etcdTestCli, cfg)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)
	// get relay source and config
	cfgs2, rev4, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(cfgs2, DeepEquals, cfgs)

	// put another relay into etcd, check whether can PutRelayConfig return two configs
	rev5, err := PutRelayConfig(etcdTestCli, source2, worker)
	c.Assert(err, IsNil)
	c.Assert(rev5, Greater, rev4)
	cfg2 := cfg.Clone()
	cfg2.SourceID = source2
	rev6, err := PutSourceCfg(etcdTestCli, cfg2)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)
	// get source bound and config with two sources
	cfgs3, rev7, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev7, Equals, rev6)
	cfgs = append(cfgs, cfg2)
	c.Assert(cfgs3, DeepEquals, cfgs)

	rev8, err := DeleteRelayConfig(etcdTestCli, source, worker)
	c.Assert(err, IsNil)
	c.Assert(rev8, Greater, rev7)
	rev9, err := DeleteRelayConfig(etcdTestCli, source2, worker)
	c.Assert(err, IsNil)
	c.Assert(rev9, Greater, rev8)

	// though source config is saved in etcd, relay source is deleted so return nothing
	cfgs3, rev10, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev10, Equals, rev9)
	c.Assert(cfgs3, HasLen, 0)
}
