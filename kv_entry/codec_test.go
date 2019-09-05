package kv_entry

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/tablecodec"
)

type codecSuite struct {
}

var _ = check.Suite(&codecSuite{})

func (s *codecSuite) TestDecodeRecordKey(c *check.C) {
	recordPrefix := tablecodec.GenTableRecordPrefix(12345)
	key := tablecodec.EncodeRecordKey(recordPrefix, 67890)
	key, tableId, err := decodeTableId(key)
	c.Assert(err, check.IsNil)
	c.Assert(tableId, check.Equals, int64(12345))
	key, recordId, err := decodeRecordId(key)
	c.Assert(err, check.IsNil)
	c.Assert(recordId, check.Equals, int64(67890))
	c.Assert(len(key), check.Equals, 0)
}
