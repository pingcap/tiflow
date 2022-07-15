package mock

import (
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// NewMockClientConn new a mock client connection
func NewMockClientConn() metaModel.ClientConn {
	return &mockClientConn{}
}

type mockClientConn struct{}

func (c *mockClientConn) Initialize(conf *metaModel.StoreConfig) error {
	return nil
}

func (c *mockClientConn) ClientType() metaModel.ClientType {
	return metaModel.MockKVClientType
}

func (c *mockClientConn) GetConn() (interface{}, error) {
	return nil, nil
}

func (c *mockClientConn) Close() error {
	return nil
}
