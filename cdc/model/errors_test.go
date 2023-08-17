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
	"testing"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsChangefeedNotRetryError(t *testing.T) {
	cases := []struct {
		err    RunningError
		result bool
	}{
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrAPIGetPDClientFailed.RFCCode()),
				Message: cerror.ErrAPIGetPDClientFailed.Error(),
			},
			false,
		},
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrExpressionColumnNotFound.RFCCode()),
				Message: cerror.ErrExpressionColumnNotFound.Error(),
			},
			true,
		},
		{
			RunningError{
				Addr:    "",
				Code:    string(cerror.ErrExpressionParseFailed.RFCCode()),
				Message: cerror.ErrExpressionParseFailed.Error(),
			},
			true,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.result, c.err.ShouldFailChangefeed())
	}
}
