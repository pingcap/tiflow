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

	cerror "github.com/pingcap/tiflow/pkg/errors"
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

func (d *DBConnectionFactory) CreateTemporaryConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	return CreateMySQLDBConn(ctx, dsnStr)
}

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

func (d *DBConnectionFactoryForTest) SetTemporaryConnectionFactory(f ConnectionFactory) {
	d.temp = f
}

func (d *DBConnectionFactoryForTest) SetStandardConnectionFactory(f ConnectionFactory) {
	d.standard = f
}

func (d *DBConnectionFactoryForTest) CreateTemporaryConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	if d.temp == nil {
		return nil, cerror.ErrCodeNilFunction.GenWithStackByArgs()
	}
	return d.temp(ctx, dsnStr)
}

func (d *DBConnectionFactoryForTest) CreateStandardConnection(ctx context.Context, dsnStr string) (*sql.DB, error) {
	if d.standard == nil {
		return nil, cerror.ErrCodeNilFunction.GenWithStackByArgs()
	}
	return d.standard(ctx, dsnStr)
}

type ConnectionFactory func(ctx context.Context, dsnStr string) (*sql.DB, error)
