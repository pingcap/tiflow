package model

import "github.com/pingcap/check"

type batchSuite struct{}

var _ = check.Suite(&batchSuite{})

func (s *taskStatusSuite) TestBatchEncoderAndDecoder(c *check.C) {
	testCases := []struct {
		key   []byte
		value []byte
	}{{key: []byte("123"), value: []byte("123")},
		{key: []byte("321"), value: []byte("123")},
		{key: []byte("abc"), value: []byte("alksdj")},
		{key: []byte("dfg"), value: []byte("a,mnv")},
		{key: []byte("qwer"), value: []byte("ijweior")},
		{key: []byte("aslkdjf"), value: []byte("asndf")}}
	encoder := NewBatchEncoder()
	var length int
	for _, tc := range testCases {
		encoder.Append(tc.key, tc.value)
		length += len(tc.key) + len(tc.value) + 16
		c.Assert(encoder.Len(), check.Equals, length)
	}
	decoder := NewBatchDecoder()
	err := decoder.Set(encoder.Read())
	c.Assert(err, check.IsNil)
	for _, tc := range testCases {
		key, value, exist := decoder.Next()
		c.Assert(exist, check.IsTrue)
		c.Assert(key, check.BytesEquals, tc.key)
		c.Assert(value, check.BytesEquals, tc.value)
	}
	c.Assert(decoder.HasNext(), check.Equals, false)

}
