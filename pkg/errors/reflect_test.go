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
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestToDMError(t *testing.T) {
	t.Parallel()

	// DM standard error
	for _, baseError := range terror.CodeToErrorMap {
		err := baseError.Generate()
		reflectErr := ToDMError(err)
		require.Error(t, reflectErr)
		require.True(t, err.(*terror.Error).Equal(reflectErr))
	}

	// error from format DM standard error
	for _, baseError := range terror.CodeToErrorMap {
		raw := baseError.Generate()
		err := errors.New(raw.Error())
		reflectErr := ToDMError(err)
		require.Error(t, reflectErr)
		require.True(t, raw.(*terror.Error).Equal(reflectErr))
	}

	// Non DM error
	err := perrors.New("normal error")
	require.Equal(t, err, ToDMError(err))
}
