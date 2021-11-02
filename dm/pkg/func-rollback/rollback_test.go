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

package rollback

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testRollbackSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testRollbackSuite struct{}

func (t *testRollbackSuite) TestRollback(c *C) {
	var (
		h        = NewRollbackHolder("test")
		total    = 5
		count    = total
		obtained = make([]int, 0, total)
		expected = make([]int, 0, total)
		rf       = func() {
			count--
			obtained = append(obtained, count)
		}
	)
	for i := total - 1; i >= 0; i-- {
		expected = append(expected, i) // [4, 3, 2, 1, 0]
	}

	for i := 0; i < total; i++ {
		h.Add(FuncRollback{Name: fmt.Sprintf("test-%d", i), Fn: rf})
	}
	h.RollbackReverseOrder()
	c.Assert(obtained, DeepEquals, expected)
}
