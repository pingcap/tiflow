// Copyright 2021 PingCAP, Inc.
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

package craft

import (
	"math/rand"
	"testing"
	"time"

<<<<<<< HEAD:cdc/sink/codec/craft/codec_test.go
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
=======
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
>>>>>>> c3a120488 (pdutil(ticdc): split `tidb_ddl_job` table (#6673)):cdc/sink/mq/codec/craft/codec_test.go
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

<<<<<<< HEAD:cdc/sink/codec/craft/codec_test.go
var _ = check.Suite(&codecSuite{allocator: NewSliceAllocator(64)})

func Test(t *testing.T) { check.TestingT(t) }

type codecSuite struct {
	allocator *SliceAllocator
}

func (s *codecSuite) TestSizeTable(c *check.C) {
	defer testleak.AfterTest(c)()
=======
func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func TestSizeTable(t *testing.T) {
	t.Parallel()

>>>>>>> c3a120488 (pdutil(ticdc): split `tidb_ddl_job` table (#6673)):cdc/sink/mq/codec/craft/codec_test.go
	tables := [][]int64{
		{
			1, 3, 5, 7, 9,
		},
		{
			2, 4, 6, 8, 10,
		},
	}
	bits := make([]byte, 16)
	rand.Read(bits)
	bits = encodeSizeTables(bits, tables)

	size, decoded, err := decodeSizeTables(bits, s.allocator)
	c.Check(err, check.IsNil)
	c.Check(decoded, check.DeepEquals, tables)
	c.Check(size, check.Equals, len(bits)-16)
}

<<<<<<< HEAD:cdc/sink/codec/craft/codec_test.go
func (s *codecSuite) TestUvarintReverse(c *check.C) {
	defer testleak.AfterTest(c)()
=======
func TestUvarintReverse(t *testing.T) {
	t.Parallel()
>>>>>>> c3a120488 (pdutil(ticdc): split `tidb_ddl_job` table (#6673)):cdc/sink/mq/codec/craft/codec_test.go

	var i uint64 = 0

	for i < 0x8000000000000000 {
		bits := make([]byte, 16)
		rand.Read(bits)
		bits, bytes1 := encodeUvarintReversed(bits, i)
		bytes2, u64, err := decodeUvarintReversed(bits)
		c.Check(err, check.IsNil)
		c.Check(u64, check.Equals, i)
		c.Check(bytes1, check.Equals, len(bits)-16)
		c.Check(bytes1, check.Equals, bytes2)
		if i == 0 {
			i = 1
		} else {
			i <<= 1
		}
	}
}

func newNullableString(a string) *string {
	return &a
}

<<<<<<< HEAD:cdc/sink/codec/craft/codec_test.go
func (s *codecSuite) TestEncodeChunk(c *check.C) {
	defer testleak.AfterTest(c)()
=======
func TestEncodeChunk(t *testing.T) {
	t.Parallel()

>>>>>>> c3a120488 (pdutil(ticdc): split `tidb_ddl_job` table (#6673)):cdc/sink/mq/codec/craft/codec_test.go
	stringChunk := []string{"a", "b", "c"}
	nullableStringChunk := []*string{newNullableString("a"), newNullableString("b"), newNullableString("c")}
	int64Chunk := []int64{1, 2, 3}

	bits := encodeStringChunk(nil, stringChunk)
	bits, decodedStringChunk, err := decodeStringChunk(bits, 3, s.allocator)
	c.Check(err, check.IsNil)
	c.Check(len(bits), check.Equals, 0)
	c.Check(decodedStringChunk, check.DeepEquals, stringChunk)

	bits = encodeNullableStringChunk(nil, nullableStringChunk)
	bits, decodedNullableStringChunk, err := decodeNullableStringChunk(bits, 3, s.allocator)
	c.Check(err, check.IsNil)
	c.Check(len(bits), check.Equals, 0)
	c.Check(decodedNullableStringChunk, check.DeepEquals, nullableStringChunk)

	bits = encodeVarintChunk(nil, int64Chunk)
	bits, decodedVarintChunk, err := decodeVarintChunk(bits, 3, s.allocator)
	c.Check(err, check.IsNil)
	c.Check(len(bits), check.Equals, 0)
	c.Check(decodedVarintChunk, check.DeepEquals, int64Chunk)
}
