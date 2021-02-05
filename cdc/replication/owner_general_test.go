package replication

import (
	"context"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap/zapcore"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type ownerSuite struct {
}

var _ = check.Suite(&ownerSuite{})

func (s *ownerSuite) TestOwnerInitialization(c *check.C) {
	defer testleak.AfterTest(c)()

	info := &model.ChangeFeedInfo{
		SinkURI:           "blackhole:///",
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           2000,
		TargetTs:          0,
		Config:            config.GetDefaultReplicaConfig(),
		Engine:            model.SortUnified,
		SortDir:           ".",
		State:             model.StateNormal,
	}

	log.SetLevel(zapcore.DebugLevel)
	harness := newOwnerTestHarness(c.MkDir())
	owner := harness.CreateOwner(1000)

	ctx, cancel := context.WithTimeout(context.Background(), 20 * time.Second)
	defer cancel()

	go func() {
		etcdCli := harness.newClient()
		cdcClient := kv.NewCDCEtcdClient(ctx, etcdCli.Unwrap())
		err := cdcClient.CreateChangefeedInfo(ctx, info, "test-1")
		c.Assert(err, check.IsNil)
	}()

	err := owner.Run(ctx)
	c.Assert(err, check.ErrorMatches, ".*canceled.*")
}
