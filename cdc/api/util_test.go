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

package api

import (
	"testing"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsHTTPBadRequestError(t *testing.T) {
	t.Parallel()
	err := cerror.ErrAPIInvalidParam.GenWithStack("aa")
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrAPIInvalidParam.Wrap(errors.New("aa"))
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrPDEtcdAPIError.GenWithStack("aa")
	require.Equal(t, false, IsHTTPBadRequestError(err))
	err = nil
	require.Equal(t, false, IsHTTPBadRequestError(err))
}
