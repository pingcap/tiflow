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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"io"

	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

type testCase struct {
	input  []byte
	output map[byte][]byte
	err    error
}

func (t *testUtilSuite) TestStatusVarsToKV(c *C) {
	testCases := []testCase{
		// only Q_FLAGS2_CODE
		{
			[]byte{0, 0, 0, 0, 0},
			map[byte][]byte{
				0: {0, 0, 0, 0},
			},
			nil,
		},
		// only Q_CHARSET_CODE
		{
			[]byte{4, 33, 0, 33, 0, 8, 0},
			map[byte][]byte{
				4: {33, 0, 33, 0, 8, 0},
			},
			nil,
		},
		// copied from a integration test
		{
			[]byte{0, 0, 0, 0, 0, 1, 4, 0, 8, 0, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 33, 0, 33, 0, 8, 0, 12, 1, 97, 108, 108, 95, 109, 111, 100, 101, 0},
			map[byte][]byte{
				0:  {0, 0, 0, 0},
				1:  {4, 0, 8, 0, 0, 0, 0, 0},
				6:  {3, 115, 116, 100},
				4:  {33, 0, 33, 0, 8, 0},
				12: {1, 97, 108, 108, 95, 109, 111, 100, 101, 0},
			},
			nil,
		},
		// wrong input
		{
			[]byte{0, 0, 0, 0, 0, 1},
			map[byte][]byte{
				0: {0, 0, 0, 0},
			},
			terror.ErrBinlogStatusVarsParse.Delegate(io.EOF, []byte{0, 0, 0, 0, 0, 1}, 6),
		},
		// undocumented status_vars
		{
			[]byte{0, 0, 0, 0, 0, 1, 32, 0, 160, 85, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 45, 0, 45, 0, 8, 0, 12, 1, 111, 110, 108, 105, 110, 101, 95, 100, 100, 108, 0, 16, 0},
			map[byte][]byte{
				0:  {0, 0, 0, 0},
				1:  {32, 0, 160, 85, 0, 0, 0, 0},
				6:  {3, 115, 116, 100},
				4:  {45, 0, 45, 0, 8, 0},
				12: {1, 111, 110, 108, 105, 110, 101, 95, 100, 100, 108, 0},
				16: {0},
			},
			nil,
		},
		// OVER_MAX_DBS_IN_EVENT_MTS in Q_UPDATED_DB_NAMES
		{
			[]byte{0, 0, 0, 0, 0, 1, 0, 0, 160, 85, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 45, 0, 45, 0, 33, 0, 12, 254},
			map[byte][]byte{
				0:  {0, 0, 0, 0},
				1:  {0, 0, 160, 85, 0, 0, 0, 0},
				6:  {3, 115, 116, 100},
				4:  {45, 0, 45, 0, 33, 0},
				12: {254},
			},
			nil,
		},
		{
			[]byte{0, 0, 0, 0, 0, 1, 0, 0, 32, 80, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 33, 0, 33, 0, 33, 0, 11, 4, 114, 111, 111, 116, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 12, 2, 109, 121, 115, 113, 108, 0, 97, 117, 116, 104, 111, 114, 105, 122, 101, 0},
			map[byte][]byte{
				0:  {0, 0, 0, 0},
				1:  {0, 0, 32, 80, 0, 0, 0, 0},
				6:  {3, 115, 116, 100},
				4:  {33, 0, 33, 0, 33, 0},
				11: {4, 114, 111, 111, 116, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116},
				12: {2, 109, 121, 115, 113, 108, 0, 97, 117, 116, 104, 111, 114, 105, 122, 101, 0},
			},
			nil,
		},
		{
			[]byte{0, 0, 0, 0, 0, 1, 0, 0, 40, 0, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 33, 0, 33, 0, 83, 0, 5, 6, 83, 89, 83, 84, 69, 77, 128, 19, 29, 12},
			map[byte][]byte{
				0:   {0, 0, 0, 0},
				1:   {0, 0, 40, 0, 0, 0, 0, 0},
				6:   {3, 115, 116, 100},
				4:   {33, 0, 33, 0, 83, 0},
				5:   {6, 83, 89, 83, 84, 69, 77}, // "SYSTEM" of length 6
				128: {19, 29, 12},                // Q_HRNOW from MariaDB
			},
			nil,
		},
	}

	for _, t := range testCases {
		vars, err := statusVarsToKV(t.input)
		if t.err != nil {
			c.Assert(err.Error(), Equals, t.err.Error())
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(vars, DeepEquals, t.output)
	}
}
