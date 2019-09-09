// Copyright 2019 PingCAP, Inc.
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

package entry

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/tablecodec"
	"testing"
)

func Test(t *testing.T) { check.TestingT(t) }

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
