package mock

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/cdc/kv"
)

func Test(t *testing.T) { check.TestingT(t) }

type mockTiDBsuite struct {
}

var _ = check.Suite(&mockTiDBsuite{})

func (s *mockTiDBsuite) TestCanGetKVEntrys(c *check.C) {
	puller, err := NewMockPuller()
	c.Assert(err, check.IsNil)

	var entrys []*kv.RawKVEntry
	entrys = puller.MustExec(c, "create table test.test(id varchar(255) primary key, a int)")
	c.Assert(len(entrys), check.Greater, 0)
	c.Log(len(entrys))

	entrys = puller.MustExec(c, "insert into test.test(id, a) values(?, ?)", 1, 1)
	c.Assert(len(entrys), check.Greater, 0)
	c.Logf("%+v", entrys)

	entrys = puller.MustExec(c, "delete from test.test")
	c.Assert(len(entrys), check.Greater, 0)
	c.Logf("%+v", entrys)
}
