package kv

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

func Test(t *testing.T) { check.TestingT(t) }

type clientSuite struct {
}

var _ = check.Suite(&clientSuite{})

func (s *clientSuite) TestNewClose(c *check.C) {
	cluster := mocktikv.NewCluster()
	pdCli := mocktikv.NewPDClient(cluster)

	cli, err := NewCDCClient(pdCli)
	c.Assert(err, check.IsNil)

	err = cli.Close()
	c.Assert(err, check.IsNil)
}
