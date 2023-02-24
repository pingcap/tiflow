// Copyright 2022 PingCAP, Inc.
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

package model

import (
	"fmt"
	"regexp"
	"testing"

	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

// This test case aims to check MasterMetaExt implements
// Scan() correctly so that it can work with the SQL driver.
func TestMasterMetaExtScan(t *testing.T) {
	t.Parallel()

	expectNoError := func(t *testing.T, err error) {
		require.NoError(t, err)
	}

	cases := []struct {
		input    any
		checkErr func(*testing.T, error)
		result   *MasterMetaExt
	}{
		{
			input:    nil,
			checkErr: expectNoError,
			result:   &MasterMetaExt{},
		},
		{
			input:    "",
			checkErr: expectNoError,
			result:   &MasterMetaExt{},
		},
		{
			input:    []byte(""),
			checkErr: expectNoError,
			result:   &MasterMetaExt{},
		},
		{
			input: 123,
			checkErr: func(t *testing.T, err error) {
				require.Regexp(
					t,
					regexp.QuoteMeta("failed to scan MasterMetaExt. Expected string or []byte, got int"),
					err)
			},
			result: &MasterMetaExt{},
		},
		{
			input:    `{"selectors":[{"label":"test","target":"test-val","op":"eq"}]}`,
			checkErr: expectNoError,
			result: &MasterMetaExt{
				Selectors: []*label.Selector{
					{
						Key:    "test",
						Target: "test-val",
						Op:     label.OpEq,
					},
				},
			},
		},
		{
			input:    []byte(`{"selectors":[{"label":"test","target":"test-val","op":"eq"}]}`),
			checkErr: expectNoError,
			result: &MasterMetaExt{
				Selectors: []*label.Selector{
					{
						Key:    "test",
						Target: "test-val",
						Op:     label.OpEq,
					},
				},
			},
		},
		{
			input: []byte(`{"selecto`),
			checkErr: func(t *testing.T, err error) {
				require.Regexp(t, "failed to unmarshal MasterMetaExt", err)
			},
			result: &MasterMetaExt{},
		},
	}

	for i := range cases {
		i := i
		t.Run(fmt.Sprintf("subcase-%d", i), func(t *testing.T) {
			t.Parallel()
			var ext MasterMetaExt
			c := cases[i]
			c.checkErr(t, ext.Scan(c.input))
			require.Equal(t, c.result, &ext)
		})
	}
}

func TestMasterMetaExtValue(t *testing.T) {
	t.Parallel()

	ext := &MasterMetaExt{Selectors: []*label.Selector{
		{
			Key:    "test",
			Target: "test-val",
			Op:     label.OpEq,
		},
	}}
	val, err := ext.Value()
	require.NoError(t, err)
	require.IsType(t, "" /* string */, val)
	require.Equal(t, `{"selectors":[{"label":"test","target":"test-val","op":"eq"}]}`, val)
}

func TestMasterStateIsTerminated(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		code         MasterState
		isTerminated bool
	}{
		{MasterStateUninit, false},
		{MasterStateInit, false},
		{MasterStateFinished, true},
		{MasterStateStopped, true},
		{MasterStateFailed, true},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.isTerminated, tc.code.IsTerminatedState())
	}
}

func TestOrmKeyValues(t *testing.T) {
	t.Parallel()
	meta := &MasterMeta{
		ProjectID: "p-id",
		ID:        "job-id",
		Type:      1,
		NodeID:    "node-id",
		Epoch:     1,
		State:     1,
		Addr:      "127.0.0.1",
		Config:    []byte{0x11, 0x22},
		ErrorMsg:  "error message",
		Detail:    []byte("job detail"),
	}
	require.Equal(t, ormModel.KeyValueMap{
		"node_id": meta.NodeID,
		"address": meta.Addr,
		"epoch":   meta.Epoch,
	}, meta.RefreshValues())

	require.Equal(t, ormModel.KeyValueMap{
		"state": meta.State,
	}, meta.UpdateStateValues())

	require.Equal(t, ormModel.KeyValueMap{
		"error_message": meta.ErrorMsg,
	}, meta.UpdateErrorValues())

	require.Equal(t, ormModel.KeyValueMap{
		"state":         meta.State,
		"error_message": meta.ErrorMsg,
		"detail":        meta.Detail,
	}, meta.ExitValues())
}
