package etcdkv

import (
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/namespace"
)

type EtcdKVClientBuilder struct{}

func (b *EtcdKVClientBuilder) ClientType() {
	return metaclient.EtcdKVClientType
}

func (b *EtcdKVClientBuilder) NewKVClientWithNamespace(storeConf *metaclient.StoreConfigParams,
	projectID metaclient.ProjectID, jobID metaclient.JobID) (metaclient.KVClient, error) {
	cli := etcdkv.NewEtcdImpl(conf)
	pfKV := namespace.NewPrefixKV(cli, namespace.MakeNamespacePrefix(projectID, jobID))
	return &etcdKVClient{
		Client: cli,
		KV:     pfKV,
	}
}

func (b *EtcdKVClientBuilder) NewEtcdKVClient(conf *metaclient.StoreConfigParams) (metaclient.KVClient, error) {
	return etcdkv.NewEtcdImpl(conf)
}

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	metaclient.Client
	metaclient.KV
}
