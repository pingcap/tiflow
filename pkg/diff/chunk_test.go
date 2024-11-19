// Copyright 2024 PingCAP, Inc.
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

package diff

import (
	"github.com/pingcap/check"
)

var _ = check.Suite(&testChunkSuite{})

type testChunkSuite struct{}

func (*testChunkSuite) TestChunkUpdate(c *check.C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []string
	}{
		{
			[]string{"a", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"5", "5", "3", "6", "6", "4"},
		}, {
			[]string{"b", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"1", "1", "5", "2", "2", "6"},
		}, {
			[]string{"c", "7", "8"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
			[]string{"1", "1", "3", "1", "3", "7", "2", "2", "4", "2", "4", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.copyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2])
		conditions, args := newChunk.toString("")
		c.Assert(conditions, check.Equals, cs.expectStr)
		c.Assert(args, check.DeepEquals, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.toString("")
	c.Assert(conditions, check.Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))")
	expectArgs := []string{"1", "1", "3", "2", "2", "4"}
	c.Assert(args, check.DeepEquals, expectArgs)
}

func (*testChunkSuite) TestChunkToString(c *check.C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args := chunk.toString("")
	c.Assert(conditions, check.Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	expectArgs := []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, check.Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("latin1")
	c.Assert(conditions, check.Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' <= ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, check.Equals, expectArgs[i])
	}
}
