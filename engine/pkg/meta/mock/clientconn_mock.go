package mock

import (
	"log"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"gorm.io/gorm"
)

// NewMockClientConn new a mock client connection
func NewMockClientConn() metaModel.ClientConn {
	return &mockClientConn{}
}

type mockClientConn struct{}

func (c *mockClientConn) ClientType() metaModel.ClientType {
	return metaModel.MockKVClientType
}

func (c *mockClientConn) GetConn() (interface{}, error) {
	log.Panic("SHOULD not call GetConn for mock client conn")
	return nil, nil
}

func (c *mockClientConn) Close() error {
	return nil
}

// NewGormClientConn new a sqlite client connection
func NewGormClientConn(db *gorm.DB) metaModel.ClientConn {
	return &gormClientConn{
		db: db,
	}
}

type gormClientConn struct {
	db *gorm.DB
}

func (c *gormClientConn) ClientType() metaModel.ClientType {
	// SHOULD NOT use this ClientConn to generate kv client
	return metaModel.UnknownKVClientType
}

func (c *gormClientConn) GetConn() (interface{}, error) {
	if c.db != nil {
		return c.db, nil
	}

	return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("inner db is nil")
}

func (c *gormClientConn) Close() error {
	return nil
}
