package model

import (
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

type columnSuite struct{}

var _ = check.Suite(&columnSuite{})

func (s *columnSuite) TestFormatCol(c *check.C) {
	row := &MqMessageRow{Update: map[string]*Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.Encode()
	c.Assert(err, check.IsNil)
	row2 := new(MqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)

	row = &MqMessageRow{Update: map[string]*Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.Encode()
	c.Assert(err, check.IsNil)
	row2 = new(MqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)
}
