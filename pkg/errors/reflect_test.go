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

package errors

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestToDMError(t *testing.T) {
	t.Parallel()

	var (
		minCode = 10000
		maxCode = 50000
	)

	for code := minCode; code < maxCode; code++ {
		// DM standard error
		baseError, ok := terror.ErrorFromCode(terror.ErrCode(code))
		if ok {
			err := baseError.Generate()
			reflectErr := ToDMError(err)
			require.Error(t, reflectErr)
			require.True(t, err.(*terror.Error).Equal(reflectErr))
		}
	}

	for code := minCode; code < maxCode; code++ {
		// error from format DM standard error
		baseError, ok := terror.ErrorFromCode(terror.ErrCode(code))
		if ok {
			raw := baseError.Generate()
			err := errors.New(raw.Error())
			reflectErr := ToDMError(err)
			require.Error(t, reflectErr)
			require.True(t, baseError.Equal(reflectErr))
		}
	}

	// Non DM error
	err := errors.New("normal error")
	require.Equal(t, err, ToDMError(err))
}
