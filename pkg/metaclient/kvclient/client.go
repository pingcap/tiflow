package kvclient

import (
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/hanfei1991/microcosm/pkg/metaclient/kvclient/etcdkv"
	"github.com/hanfei1991/microcosm/pkg/metaclient/namespace"
)

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+etcdClientCloser -> etcdKVClient
type etcdKVClient struct {
	metaclient.Closer
	metaclient.KV
	tenantID string
}

func NewEtcdKVClient(config *metaclient.Config, tenantID string) (metaclient.KVClient, error) {
	impl, err := etcdkv.NewEtcdImpl(config)
	if err != nil {
		return nil, err
	}

	pfKV := namespace.NewPrefixKV(impl, namespace.MakeNamespacePrefix(tenantID))
	return &etcdKVClient{
		Closer:   etcdkv.NewEtcdClientCloser(impl),
		KV:       pfKV,
		tenantID: tenantID,
	}, nil
}
