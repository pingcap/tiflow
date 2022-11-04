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
// See the License for the specific language

package errors

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestFailFastWrap(t *testing.T) {
	t.Parallel()

	// Note: this error is only used for testing.
	// Feel free to replace it with another one.
	testErr := errors.ErrTooManyStatusUpdates.GenWithStackByArgs()

	err := FailFast(testErr)
	require.True(t, errors.Is(err, errors.ErrTooManyStatusUpdates))
	require.Regexp(t, "ErrTooManyStatusUpdates", err)

	require.True(t, IsFailFastError(err))
}

func TestIsFailFastErrorFalse(t *testing.T) {
	t.Parallel()

	anyErr := errors.New("test")
	require.False(t, IsFailFastError(anyErr))
}

func TestFailFastWrapNil(t *testing.T) {
	t.Parallel()

	require.NoError(t, FailFast(nil))
}
