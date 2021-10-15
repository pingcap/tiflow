package conn

import (
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

type mockDBProvider struct {
	verDB *sql.DB // verDB user for show version.
	db    *sql.DB
}

// Apply will build BaseDB with DBConfig.
func (d *mockDBProvider) Apply(config config.DBConfig) (*BaseDB, error) {
	if d.verDB != nil {
		if err := d.verDB.Ping(); err == nil {
			// nolint:nilerr
			return NewBaseDB(d.verDB, func() {}), nil
		}
	}
	return NewBaseDB(d.db, func() {}), nil
}

// InitMockDB return a mocked db for unit test.
func InitMockDB(c *check.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.db = db
	} else {
		DefaultDBProvider = &mockDBProvider{db: db}
	}
	return mock
}

// InitVersionDB return a mocked db for unit test's show version.
func InitVersionDB(c *check.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.verDB = db
	} else {
		DefaultDBProvider = &mockDBProvider{verDB: db}
	}
	return mock
}
