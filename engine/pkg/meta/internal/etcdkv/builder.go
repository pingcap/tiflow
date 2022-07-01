package etcdkv

import (
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv/namespace"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

func init() {
	internal.MustRegister(&etcdKVClientBuilder{})
}

type etcdKVClientBuilder struct{}

func (b *etcdKVClientBuilder) ClientType() {
	return metaclient.EtcdKVClientType
}

func (b *etcdKVClientBuilder) NewKVClientWithNamespace(storeConf *metaclient.StoreConfigParams,
	projectID metaclient.ProjectID, jobID metaclient.JobID) (metaclient.KVClient, error) {
	cli := etcdkv.NewEtcdImpl(conf)
	pfKV := namespace.NewPrefixKV(cli, namespace.MakeNamespacePrefix(projectID, jobID))
	return &etcdKVClient{
		Client: cli,
		KV:     pfKV,
	}
}

func (b *etcdKVClientBuilder) NewKVClient(conf *metaclient.StoreConfigParams) (metaclient.KVClient, error) {
	return etcdkv.NewEtcdImpl(conf)
}

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	metaclient.Client
	metaclient.KV
}
