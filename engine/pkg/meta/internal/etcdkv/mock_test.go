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

package etcdkv

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestRetryMockBackendEtcd(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/MockEtcdAddressAlreadyUse", "return(true)")
	_, _, err := RetryMockBackendEtcd()
	require.Error(t, err)
	require.Regexp(t, "address already in use", err)
	_ = failpoint.Disable("github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/MockEtcdAddressAlreadyUse")

	svr, _, err := RetryMockBackendEtcd()
	require.NoError(t, err)
	require.NotNil(t, svr)
	CloseEmbededEtcd(svr)
}
