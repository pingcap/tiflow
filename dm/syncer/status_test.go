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

package syncer

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/dm/syncer/shardddl"
	"go.uber.org/zap"
)

var _ = check.Suite(&statusSuite{})

type statusSuite struct{}

func (t *statusSuite) TestStatusRace(c *check.C) {
	s := &Syncer{}

	l := log.With(zap.String("unit test", "TestStatueRace"))
	s.tctx = tcontext.Background().WithLogger(l)
	s.cfg = &config.SubTaskConfig{}
	s.checkpoint = &mockCheckpoint{}
	s.pessimist = shardddl.NewPessimist(&l, nil, "", "")
	s.optimist = shardddl.NewOptimist(&l, nil, "", "")
	s.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source")

	sourceStatus := &binlog.SourceStatus{
		Location: binlog.Location{
			Position: mysql.Position{
				Name: "mysql-bin.000123",
				Pos:  223,
			},
		},
		Binlogs: binlog.FileSizes(nil),
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ret := s.Status(sourceStatus)
			status := ret.(*pb.SyncStatus)
			c.Assert(status.MasterBinlog, check.Equals, "(mysql-bin.000123, 223)")
			c.Assert(status.SyncerBinlog, check.Equals, "(mysql-bin.000123, 123)")
		}()
	}
	wg.Wait()
}

type mockCheckpoint struct {
	CheckPoint
}

func (*mockCheckpoint) FlushedGlobalPoint() binlog.Location {
	return binlog.Location{
		Position: mysql.Position{
			Name: "mysql-bin.000123",
			Pos:  123,
		},
	}
}

func (*mockCheckpoint) SaveTablePoint(_ *filter.Table, _ binlog.Location, _ *model.TableInfo) {}
