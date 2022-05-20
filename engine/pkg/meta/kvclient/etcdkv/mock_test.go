package etcdkv

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestRetryMockBackendEtcd(t *testing.T) {
	_ = failpoint.Enable("github.com/hanfei1991/microcosm/pkg/meta/kvclient/etcdkv/MockEtcdAddressAlreadyUse", "return(true)")
	_, _, err := RetryMockBackendEtcd()
	require.Error(t, err)
	require.Regexp(t, "address already in use", err)
	_ = failpoint.Disable("github.com/hanfei1991/microcosm/pkg/meta/kvclient/etcdkv/MockEtcdAddressAlreadyUse")

	svr, _, err := RetryMockBackendEtcd()
	require.NoError(t, err)
	require.NotNil(t, svr)
	CloseEmbededEtcd(svr)
}
