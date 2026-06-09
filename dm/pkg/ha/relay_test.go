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
	"github.com/pingcap/tiflow/dm/config"
)

func (s *testForEtcd) TestGetRelayConfigEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		worker = "dm-worker-1"
		source = "mysql-replica-1"
	)
	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	s.Require().NoError(err)
	cfg.SourceID = source
	// no relay source and config
	cfg1, rev1, err := GetRelayConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Nil(cfg1)

	rev2, err := PutRelayConfig(etcdTestCli, source, worker)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get relay source and config, but config is empty
	_, _, err = GetRelayConfig(etcdTestCli, worker)
	s.Require().Error(err)
	s.Require().Regexp(".*doesn't have related source config in etcd.*", err.Error())

	rev3, err := PutSourceCfg(etcdTestCli, cfg)
	s.Require().NoError(err)
	s.Require().Greater(rev3, rev2)
	// get relay source and config
	cfg2, rev4, err := GetRelayConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	s.Require().Equal(cfg, cfg2)

	rev5, err := DeleteRelayConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Greater(rev5, rev4)

	// though source config is saved in etcd, relay source is deleted so return nothing
	cfg3, rev6, err := GetRelayConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Equal(rev5, rev6)
	s.Require().Nil(cfg3)
}
