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

package orm

import (
	"errors"
	"testing"

	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsNotFoundError(t *testing.T) {
	b := IsNotFoundError(cerrors.ErrMetaEntryNotFound.GenWithStackByArgs("error"))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaEntryNotFound.GenWithStack("err:%s", "error"))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaEntryNotFound.Wrap(errors.New("error")))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaNewClientFail.Wrap(errors.New("error")))
	require.False(t, b)

	b = IsNotFoundError(errors.New("error"))
	require.False(t, b)
}
