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

package ha

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/dm/config"
)

func (s *testForEtcd) TestSourceBoundJSON() {
	b1 := NewSourceBound("mysql-replica-1", "dm-worker-1")

	j, err := b1.toJSON()
	s.Require().NoError(err)
	s.Require().Equal(`{"source":"mysql-replica-1","worker":"dm-worker-1"}`, j)
	s.Require().Equal(b1.String(), j)

	b2, err := sourceBoundFromJSON(j)
	s.Require().NoError(err)
	s.Require().Equal(b1, b2)
}

func (s *testForEtcd) TestSourceBoundEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		watchTimeout = 2 * time.Second
		worker1      = "dm-worker-1"
		worker2      = "dm-worker-2"
		bound1       = NewSourceBound("mysql-replica-1", worker1)
		bound2       = NewSourceBound("mysql-replica-2", worker2)
	)
	s.Require().False(bound1.IsDeleted)

	// no bound exists.
	sbm1, rev1, err := GetSourceBound(etcdTestCli, "")
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Len(sbm1, 0)

	// put two bounds.
	rev2, err := PutSourceBound(etcdTestCli, bound1)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)
	rev3, err := PutSourceBound(etcdTestCli, bound2)
	s.Require().NoError(err)
	s.Require().Greater(rev3, rev2)

	// watch the PUT operation for the bound1.
	boundCh := make(chan SourceBound, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceBound(ctx, etcdTestCli, worker1, rev2, boundCh, errCh)
	cancel()
	close(boundCh)
	close(errCh)
	s.Require().Equal(1, len(boundCh))
	bound1.Revision = rev2
	s.Require().Equal(bound1, <-boundCh)
	s.Require().Equal(0, len(errCh))

	// get bound1 back.
	sbm2, rev4, err := GetSourceBound(etcdTestCli, worker1)
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	s.Require().Len(sbm2, 1)
	s.Require().Equal(bound1, sbm2[worker1])

	// get bound1 and bound2 back.
	sbm2, rev4, err = GetSourceBound(etcdTestCli, "")
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	s.Require().Len(sbm2, 2)
	s.Require().Equal(bound1, sbm2[worker1])
	bound2.Revision = rev3
	s.Require().Equal(bound2, sbm2[worker2])

	// delete bound1.
	rev5, err := DeleteSourceBound(etcdTestCli, worker1)
	s.Require().NoError(err)
	s.Require().Greater(rev5, rev4)

	// delete bound2.
	rev6, err := DeleteSourceBound(etcdTestCli, worker2)
	s.Require().NoError(err)
	s.Require().Greater(rev6, rev5)

	// watch the DELETE operation for bound1.
	boundCh = make(chan SourceBound, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceBound(ctx, etcdTestCli, worker1, rev5, boundCh, errCh)
	cancel()
	close(boundCh)
	close(errCh)
	s.Require().Equal(1, len(boundCh))
	bo := <-boundCh
	s.Require().True(bo.IsDeleted)
	s.Require().Equal(rev5, bo.Revision)
	s.Require().Equal(0, len(errCh))

	// get again, bound1 not exists now.
	sbm3, rev7, err := GetSourceBound(etcdTestCli, worker1)
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(sbm3, 0)
}

func (s *testForEtcd) TestGetSourceBoundConfigEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		worker = "dm-worker-1"
		source = "mysql-replica-1"
		bound  = NewSourceBound(source, worker)
	)
	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	s.Require().NoError(err)
	cfg.SourceID = source
	// no source bound and config
	bound1, cfg1, rev1, err := GetSourceBoundConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().True(bound1.IsEmpty())
	s.Require().Nil(cfg1)

	rev2, err := PutSourceBound(etcdTestCli, bound)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)
	// get source bound and config, but config is empty
	// nolint:dogsled
	_, _, _, err = GetSourceBoundConfig(etcdTestCli, worker)
	s.Require().Error(err)
	s.Require().Regexp(".*doesn't have related source config in etcd.*", err.Error())

	rev3, err := PutSourceCfg(etcdTestCli, cfg)
	s.Require().NoError(err)
	s.Require().Greater(rev3, rev2)
	// get source bound and config
	bound2, cfg2, rev4, err := GetSourceBoundConfig(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	bound.Revision = rev2
	s.Require().Equal(bound, bound2)
	s.Require().Equal(cfg, cfg2)
}
