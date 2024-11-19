// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"database/sql"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// IDBConnectionFactory is an interface designed specifically to facilitate unit testing
// in scenarios where connections to downstream databases are required.
//
// In the process of creating a downstream database connection based on a Sink URI,
// a temporary connection is first established. This temporary connection is used
// to query information from the downstream database, such as version, charset, etc.
// After retrieving this information, the temporary connection is closed.
// The retrieved data is then used to populate additional parameters into the Sink URI,
// and a more refined Sink URI is used to establish the final, standard connection that
// the Sink will use for subsequent operations.
//
// During normal system operation, it's perfectly acceptable to create both of these
// connections in the same manner. However, in the context of unit testing, where
// it's not feasible to start an actual downstream database, we need to mock these
// connections. Since both connections will send SQL requests, the unit tests require
// mocking two different connections to handle each phase of the connection creation process.
//
// This interface addresses this issue by providing two distinct methods:
// CreateTemporaryConnection, for creating the temporary connection, and
// CreateStandardConnection, for creating the standard, persistent connection.
// By using these separate methods, the interface allows for greater flexibility
// in mocking and testing, ensuring that the two connections can be created differently
// as needed during unit tests.
type IDBConnectionFactory interface {
	CreateTemporaryConnection(ctx context.Context, dsnStr string) (*sql.DB, error)
	CreateStandardConnection(ctx context.Context, dsnStr string) (*sql.DB, error)
}

// DBConnectionFactory is an implementation of the IDBConnectionFactory interface,
// designed for use in normal system operations where only a single method of creating
// database connections is required.
//
// In regular workflows, both the temporary connection (used for querying information
// like version and charset) and the standard connection (used for subsequent operations)
// can be created in the same manner. Therefore, DBConnectionFactory provides a unified
// approach by implementing the IDBConnectionFactory interface and using the same
// CreateMySQLDBConn function for both CreateTemporaryConnection and CreateStandardConnection.
//
// This struct simplifies the process for scenarios where unit testing is not a concern.
// As long as you are not writing unit tests and do not need to mock different types
// of connections, using DBConnectionFactory is perfectly suitable for establishing
// the necessary database connections.
type DBConnectionFactory struct{}

// CreateTemporaryConnection creates a temporary database connection used to query
// essential information from the downstream database, such as version and charset.
// This connection is intended to be short-lived and will be closed after the necessary
// information is retrieved.
func (d *DBConnectionFactory) CreateTemporaryConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	return CreateMySQLDBConn(ctx, dsnStr)
}

// CreateStandardConnection creates the standard database connection that will be
// used by the system for ongoing operations. This connection is based on the refined
// Sink URI containing the necessary parameters gathered from the temporary connection.
func (d *DBConnectionFactory) CreateStandardConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	return CreateMySQLDBConn(ctx, dsnStr)
}

// DBConnectionFactoryForTest is a utility implementation of the IDBConnectionFactory
// interface designed to simplify the process of writing unit tests that require
// different methods for creating database connections.
//
// Instead of implementing the IDBConnectionFactory interface from scratch in every
// unit test, DBConnectionFactoryForTest allows developers to easily set up custom
// connection creation methods. The SetTemporaryConnectionFactory and
// SetStandardConnectionFactory methods allow you to define how the temporary and
// standard connections should be created during testing.
//
// Once these methods are set, the system will automatically invoke CreateTemporaryConnection
// and CreateStandardConnection to establish the respective connections during the
// connection creation process. This approach provides flexibility and convenience
// in unit testing scenarios, where different connection behaviors need to be mocked
// or tested separately.
type DBConnectionFactoryForTest struct {
	temp     ConnectionFactory
	standard ConnectionFactory
}

// NewDBConnectionFactoryForTest creates a new instance of DBConnectionFactoryForTest
// with a predefined temporary connection creation method. This method is initialized
// to use a mock database connection, which is commonly required in the current unit tests.
// By setting up the temporary connection creation logic directly in this constructor,
// it eliminates the need to repeatedly call SetTemporaryConnectionFactory to set up
// the temporary connection factory in each test. This streamlines the setup process
// for unit tests that require a mock temporary connection.
func NewDBConnectionFactoryForTest() *DBConnectionFactoryForTest {
	dbConnFactory := &DBConnectionFactoryForTest{}
	dbConnFactory.SetTemporaryConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		return MockTestDB()
	})
	return dbConnFactory
}

// SetTemporaryConnectionFactory sets the connection factory that will be used to
// create the temporary connection during testing. This allows for custom behavior
// during unit tests when different connection logic is required.
func (d *DBConnectionFactoryForTest) SetTemporaryConnectionFactory(f ConnectionFactory) {
	d.temp = f
}

// SetStandardConnectionFactory sets the connection factory that will be used to
// create the standard connection during testing. This provides the ability to mock
// the connection behavior specifically for the standard connection phase.
func (d *DBConnectionFactoryForTest) SetStandardConnectionFactory(f ConnectionFactory) {
	d.standard = f
}

// CreateTemporaryConnection creates a temporary database connection during testing
// using the connection factory set by SetTemporaryConnectionFactory. If no factory
// has been set, it returns an error. This method allows for customized connection
// logic during the temporary connection phase in unit tests.
func (d *DBConnectionFactoryForTest) CreateTemporaryConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	if d.temp == nil {
		return nil, cerror.ErrCodeNilFunction.GenWithStackByArgs()
	}
	return d.temp(ctx, dsnStr)
}

// CreateStandardConnection creates the standard database connection during testing
// using the connection factory set by SetStandardConnectionFactory. If no factory
// has been set, it returns an error. This method supports custom connection behavior
// during the standard connection phase in unit tests.
func (d *DBConnectionFactoryForTest) CreateStandardConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	if d.standard == nil {
		return nil, cerror.ErrCodeNilFunction.GenWithStackByArgs()
	}
	return d.standard(ctx, dsnStr)
}

// ConnectionFactory is a function type that takes a context and a DSN (Data Source Name) string
// as input parameters, and returns a pointer to an sql.DB object and an error. This function type
// is typically used to create and configure database connections.
type ConnectionFactory func(ctx context.Context, dsnStr string) (*sql.DB, error)

// CreateMySQLDBConn establishes a connection to a MySQL database and pings it to verify the connection.
// It takes a context for managing cancellation and timeouts, and a DSN (Data Source Name) string as input.
// The function returns a pointer to an sql.DB object, representing the connection, or an error if the connection fails.
func CreateMySQLDBConn(ctx context.Context, dsnStr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	// Ping the database to verify that the connection is established and functioning.
	err = db.PingContext(ctx)
	if err != nil {
		// If pinging fails, attempt to close the connection to release resources.
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	return db, nil
}
