package sqlkv

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv/namespace"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

func init() {
	internal.MustRegister(&sqlKVClientBuilder{})
}

type sqlKVClientBuilder struct{}

func (b *sqlKVClientBuilder) ClientType() {
	return metaclient.SQLKVClientType
}

func (b *sqlKVClientBuilder) NewKVClientWithNamespace(storeConf *metaclient.StoreConfigParams,
	projectID metaclient.ProjectID, jobID metaclient.JobID) (metaclient.KVClient, error) {
	return namespace.NewClient(storeConf, projectID, jobID)
}

func (b *sqlKVClientBuilder) NewKVClient(storeConf *metaclient.StoreConfigParams) (metaclient.KVClient, error) {
	ormDB, err := NewOrmDB(storeConf)
	if err != nil {
		return nil, err
	}

	return sqlkv.NewSQLImpl(ormDB)
}
