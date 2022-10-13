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
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	input  []byte
	output map[byte][]byte
	err    error
}

type testCaseTimezone struct {
	input  []byte
	output string
	err    error
}

func TestStatusVarsToKV(t *testing.T) {
	t.Parallel()
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

	for _, test := range testCases {
		vars, err := statusVarsToKV(test.input)
		if test.err != nil {
			require.Equal(t, test.err.Error(), err.Error())
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, test.output, vars)
	}
}

func TestGetTimezoneByStatusVars(t *testing.T) {
	t.Parallel()
	testCases := []testCaseTimezone{
		//+08:00
		{
			[]byte{0, 0, 0, 0, 0, 1, 32, 0, 160, 69, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 8, 0, 8, 0, 46, 0, 5, 6, 43, 48, 56, 58, 48, 48, 12, 1, 109, 97, 110, 121, 95, 116, 97, 98, 108, 101, 115, 95, 116, 101, 115, 116, 0},
			"+08:00",
			nil,
		},
		//-06:00
		{
			[]byte{0, 0, 0, 0, 0, 1, 32, 0, 160, 69, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 8, 0, 8, 0, 46, 0, 5, 6, 45, 48, 54, 58, 48, 48, 12, 1, 109, 97, 110, 121, 95, 116, 97, 98, 108, 101, 115, 95, 116, 101, 115, 116, 0},
			"-06:00",
			nil,
		},
		// SYSTEM
		{
			[]byte{0, 0, 0, 0, 0, 1, 32, 0, 160, 69, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 8, 0, 8, 0, 46, 0, 5, 6, 83, 89, 83, 84, 69, 77, 12, 1, 109, 97, 110, 121, 95, 116, 97, 98, 108, 101, 115, 95, 116, 101, 115, 116, 0},
			"+0:00",
			nil,
		},
	}

	for _, test := range testCases {
		// to simulate Syncer.upstreamTZ
		upstreamTZStr := "+0:00"
		vars, err := GetTimezoneByStatusVars(test.input, upstreamTZStr)
		if test.err != nil {
			require.Equal(t, test.err.Error(), err.Error())
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, test.output, vars)
	}
}
