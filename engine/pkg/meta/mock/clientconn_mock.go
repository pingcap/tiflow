package mock

import (
	"database/sql"
	"fmt"
	"log"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
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

// NewGormClientConn new a client connection with an gorm.DB inside
// Currently, we only use this connection for sqlite backend
func NewMockClientConnForSqlite(dbFile string) (metaModel.ClientConn, error) {
	// ref:https://www.sqlite.org/inmemorydb.html
	// using dsn(file:%s?mode=memory&cache=shared) format here to
	// 1. Create different DB for different TestXXX()
	// 2. Enable DB shared for different connection
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", dbFile)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, errors.ErrMetaOpFail.wrap(err)
	}

	return &gormClientConn{
		db: db,
	}
}

type gormClientConn struct {
	db *sql.DB
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
