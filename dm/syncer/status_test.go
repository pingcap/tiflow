package syncer

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/dm/config"
	"github.com/pingcap/ticdc/dm/dm/pb"
	"github.com/pingcap/ticdc/dm/pkg/binlog"
	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/syncer/shardddl"
)

var _ = Suite(&statusSuite{})

type statusSuite struct{}

func (t *statusSuite) TestStatusRace(c *C) {
	s := &Syncer{}

	l := log.With(zap.String("unit test", "TestStatueRace"))
	s.tctx = tcontext.Background().WithLogger(l)
	s.cfg = &config.SubTaskConfig{}
	s.checkpoint = &mockCheckpoint{}
	s.pessimist = shardddl.NewPessimist(&l, nil, "", "")

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
			c.Assert(status.MasterBinlog, Equals, "(mysql-bin.000123, 223)")
			c.Assert(status.SyncerBinlog, Equals, "(mysql-bin.000123, 123)")
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
