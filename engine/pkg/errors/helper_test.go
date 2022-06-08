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

package errors

import (
	std_errors "errors"
	"testing"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/stretchr/testify/require"
)

func TestToPBError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		err     error
		pbError *pb.Error
	}{
		{
			nil,
			nil,
		},
		{
			std_errors.New("non rfc error"),
			&pb.Error{Code: pb.ErrorCode_UnknownError},
		},
		{
			ErrUnknownExecutorID.FastGenByArgs(100),
			&pb.Error{Code: pb.ErrorCode_UnknownExecutor},
		},
		{
			ErrTombstoneExecutor.FastGenByArgs(101),
			&pb.Error{Code: pb.ErrorCode_TombstoneExecutor},
		},
		{
			ErrSubJobFailed.FastGenByArgs(102, 103),
			&pb.Error{Code: pb.ErrorCode_SubJobSubmitFailed},
		},
		{
			ErrClusterResourceNotEnough.FastGenByArgs(),
			&pb.Error{Code: pb.ErrorCode_NotEnoughResource},
		},
		{
			ErrBuildJobFailed.FastGenByArgs(),
			&pb.Error{Code: pb.ErrorCode_SubJobBuildFailed},
		},
		{
			ErrHeartbeat.FastGenByArgs("logic"),
			&pb.Error{Code: pb.ErrorCode_UnknownError},
		},
	}
	for _, tc := range testCases {
		if tc.err != nil {
			tc.pbError.Message = tc.err.Error()
		}
		require.Equal(t, tc.pbError, ToPBError(tc.err))
	}
}

func TestWrapError(t *testing.T) {
	t.Parallel()
	var (
		err       = errors.New("test")
		testCases = []struct {
			rfcError *errors.Error
			err      error
			isNil    bool
			expected string
			args     []interface{}
		}{
			{ErrBuildJobFailed, nil, true, "", []interface{}{}},
			{ErrBuildJobFailed, err, false, "[DFLOW:ErrBuildJobFailed]build job failed: test", []interface{}{}},
			{ErrSubJobFailed, err, false, "[DFLOW:ErrSubJobFailed]executor e-1 job 2: test", []interface{}{"e-1", 2}},
		}
	)
	for _, tc := range testCases {
		we := Wrap(tc.rfcError, tc.err, tc.args...)
		if tc.isNil {
			require.Nil(t, we)
		} else {
			require.NotNil(t, we)
			require.Equal(t, tc.expected, we.Error())
		}
	}
}
