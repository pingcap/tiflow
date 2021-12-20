// Copyright 2020 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type codecSuite struct {
}

var _ = check.Suite(&codecSuite{})

func (s *codecSuite) TestDecodeRecordKey(c *check.C) {
	defer testleak.AfterTest(c)()
	recordPrefix := tablecodec.GenTableRecordPrefix(12345)
	key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(67890))
	key, tableID, err := decodeTableID(key)
	c.Assert(err, check.IsNil)
	c.Assert(tableID, check.Equals, int64(12345))
	key, recordID, err := decodeRecordID(key)
	c.Assert(err, check.IsNil)
	c.Assert(recordID, check.Equals, int64(67890))
	c.Assert(len(key), check.Equals, 0)
}

type decodeMetaKeySuite struct {
}

var _ = check.Suite(&decodeMetaKeySuite{})

func (s *decodeMetaKeySuite) TestDecodeListData(c *check.C) {
	defer testleak.AfterTest(c)()
	key := []byte("hello")
	var index int64 = 3

	meta, err := decodeMetaKey(buildMetaKey(key, index))
	c.Assert(err, check.IsNil)
	c.Assert(meta.getType(), check.Equals, ListData)
	list := meta.(metaListData)
	c.Assert(list.key, check.Equals, string(key))
	c.Assert(list.index, check.Equals, index)
}

func buildMetaKey(key []byte, index int64) []byte {
	ek := make([]byte, 0, len(metaPrefix)+len(key)+36)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(ListData))
	return codec.EncodeInt(ek, index)
}
