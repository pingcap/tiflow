package mock

import (
	"database/sql"
	"log"

	// register sqlite driver
	_ "github.com/mattn/go-sqlite3"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// NewMockClientConn news a connection for mock kvclient
// Only for test
func NewMockClientConn() metaModel.ClientConn {
	return &mockClientConn{}
}

type mockClientConn struct{}

func (c *mockClientConn) StoreType() metaModel.StoreType {
	return metaModel.StoreTypeMockKV
}

func (c *mockClientConn) GetConn() (interface{}, error) {
	log.Panic("SHOULD not call GetConn for mock client conn")
	return nil, nil
}

func (c *mockClientConn) Close() error {
	return nil
}

//////////////////////////////////////////////////////////////////////////

// NewClientConnForSQLite news a connection of sqlite
// Only for test
func NewClientConnForSQLite(dsn string) (metaModel.ClientConn, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)

	return &sqliteClientConn{
		db: db,
	}, nil
}

type sqliteClientConn struct {
	db *sql.DB
}

func (c *sqliteClientConn) StoreType() metaModel.StoreType {
	return metaModel.StoreTypeSQLite
}

func (c *sqliteClientConn) GetConn() (interface{}, error) {
	if c.db != nil {
		return c.db, nil
	}

	return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("inner db is nil")
}

func (c *sqliteClientConn) Close() error {
	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////////

// NewClientConnWithDB news a connection with an sql.DB inside
// Only for test
func NewClientConnWithDB(db *sql.DB) metaModel.ClientConn {
	return &dbClientConn{
		db: db,
	}
}

type dbClientConn struct {
	db *sql.DB
}

func (c *dbClientConn) StoreType() metaModel.StoreType {
	return metaModel.StoreTypeMySQL
}

func (c *dbClientConn) GetConn() (interface{}, error) {
	if c.db != nil {
		return c.db, nil
	}

	return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("inner db is nil")
}

func (c *dbClientConn) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
