// Copyright 2024 PingCAP, Inc.
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
	"net/url"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MySQLDBConnector manages the database connection, handling reconnections
// in case of connection failures when calling SwitchToAvailableMySQLDB method.
// To execute SQL queries or interact with the database, use the CurrentDB field of
// MySQLDBConnector to access the underlying *sql.DB instance, i.e., MySQLDBConnector.CurrentDB.
type MySQLDBConnector struct {
	// Current connnection to a MySQL-compatible DB.
	CurrentDB *sql.DB

	// The list of Data Source Names (DSNs), which are used as inputs for sql.Open().
	// The DSN format for passing to the database driver:
	// [username[:password]@][net[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsnList []string

	// The index for the next connection attempt,
	// used to try to find an available DB server in Round-Robin mode.
	nextTry int

	// dbConnFactory is a Factory function for MySQLDBConnector that creates database connections.
	dbConnFactory IDBConnectionFactory

	// configureDBWhenSwitch stores the function that will be automatically invoked
	// to configure a new MySQL connection after switching to it using the
	// SwitchToAvailableMySQLDB function. This function will be set by the
	// ConfigureDBWhenSwitch method and can be applied to both newly established
	// connections and the current active connection if required.
	configureDBWhenSwitch func()
}

// New a MySQLDBConnector for creating DB connection.
func NewMySQLDBConnector(ctx context.Context, cfg *Config, sinkURI *url.URL) (*MySQLDBConnector, error) {
	return NewMySQLDBConnectorWithFactory(ctx, cfg, sinkURI, &DBConnectionFactory{})
}

// New a MySQLDBConnector by the given factory function for creating DB connection.
// The sinkURI format:
// [scheme]://[user[:password]@][host[:port]][,host[:port]][,host[:port]][/path][?param1=value1&paramN=valueN]
// User must ensure that each address ([host[:port]]) in the sinkURI (if there are multiple addresses)
// is valid, otherwise returns an error.
func NewMySQLDBConnectorWithFactory(ctx context.Context, cfg *Config, sinkURI *url.URL, dbConnFactory IDBConnectionFactory) (*MySQLDBConnector, error) {
	if dbConnFactory == nil {
		dbConnFactory = &DBConnectionFactory{}
	}

	// generateDSNs function parses multiple addresses from the URL (if any)
	// and generates a DSN (Data Source Name) for each one.
	// For each DSN, the function attempts to create a connection and perform a Ping
	// to verify its availability.
	// If any of the DSNs are unavailable, the function immediately returns an error.
	dsnList, err := generateDSNs(ctx, sinkURI, cfg, dbConnFactory.CreateTemporaryConnection)
	if err != nil {
		return nil, err
	}

	connector := &MySQLDBConnector{dsnList: dsnList, nextTry: 0, dbConnFactory: dbConnFactory}

	err = connector.SwitchToAvailableMySQLDB(ctx)
	if err != nil {
		return nil, err
	}

	return connector, nil
}

// SwitchToAvailableMySQLDB attempts to switch to an available MySQL-compatible database connection
// if the current connection is invalid, and it updates MySQLDBConnector.CurrentDB for user to use it.
// If there is only one DSN in MySQLDBConnector, switching is not possible.
func (c *MySQLDBConnector) SwitchToAvailableMySQLDB(ctx context.Context) error {
	// If a connection has already been established and there is only one DSN (Data Source Name) available,
	// it is not possible to connect a different DSN. Therefore, simply return from the function.
	if len(c.dsnList) == 1 && c.CurrentDB != nil {
		return nil
	}

	if c.CurrentDB != nil {
		// Check if the current connection is available; return immediately if it is.
		err := c.CurrentDB.PingContext(ctx)
		if err == nil {
			return nil
		}

		// The current connection has become invalid. Close this connection to free up resources,
		// then attempt to establish a new connection using a different DSN.
		closeErr := c.CurrentDB.Close()
		if closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
	}

	var err error
	for i := 0; i < len(c.dsnList); i++ {
		if c.dsnList[c.nextTry] == "" {
			continue
		}

		db, err := c.dbConnFactory.CreateStandardConnection(ctx, c.dsnList[c.nextTry])
		c.nextTry = (c.nextTry + 1) % len(c.dsnList)
		if err == nil {
			c.CurrentDB = db
			if c.configureDBWhenSwitch != nil {
				c.configureDBWhenSwitch()
			}
			return nil
		}
	}

	return err
}

// ConfigureDBWhenSwitch allows for automatic configuration of a new MySQL connection
// when switching to an available connection using the SwitchToAvailableMySQLDB function.
// By providing the function `f` as an argument, it ensures that any newly established
// connection is automatically configured after the switch. Additionally, if the existing
// MySQLDBConnector.CurrentDB also requires configuration, you can set
// `needConfigureCurrentDB` to true, and this function will automatically apply
// the configuration function `f` to it as well.
func (c *MySQLDBConnector) ConfigureDBWhenSwitch(f func(), needConfigureCurrentDB bool) {
	if f == nil {
		return
	}
	c.configureDBWhenSwitch = f
	if needConfigureCurrentDB {
		c.configureDBWhenSwitch()
	}
}
