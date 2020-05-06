package codec

import (
	"testing"

	"github.com/pingcap/ticdc/cdc/model"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	rowCases        []*model.RowChangedEvent
	ddlCases        []*model.DDLEvent
	resolvedTsCases []uint64
}

var _ = check.Suite(&batchSuite{})

func (s *batchSuite) test(c *check.C) {
	//TODO add tests for batch encoder
}
