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
	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
)

func (t *testForEtcd) TestGetRelayConfigEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		worker = "dm-worker-1"
		source = "mysql-replica-1"
	)
	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	c.Assert(err, IsNil)
	cfg.SourceID = source
	// no relay source and config
	cfg1, rev1, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(cfg1, IsNil)

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
	cfg2, rev4, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(cfg2, DeepEquals, cfg)

	rev5, err := DeleteRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev5, Greater, rev4)

	// though source config is saved in etcd, relay source is deleted so return nothing
	cfg3, rev6, err := GetRelayConfig(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev6, Equals, rev5)
	c.Assert(cfg3, IsNil)
}
