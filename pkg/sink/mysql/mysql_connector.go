package mysql

import (
	"context"
	"database/sql"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type MySQLDBConnector struct {
	// The current DB connnection.
	CurrentDB *sql.DB

	// The list of Data Source Names (DSNs), which are used as inputs for sql.Open().
	// The DSN format for passing to the database driver:
	// [username[:password]@][net[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsnList []string

	// The index for the next connection attempt,
	// used to try to find an available DB server in Round-Robin mode.
	nextTry int
}

// The sinkURI format:
// [scheme]://[user[:password]@][host[:port]][,host[:port]][,host[:port]][/path][?param1=value1&paramN=valueN]
// User must ensure that each address ([host[:port]]) in the sinkURI (if there are multiple addresses)
// is valid.
func NewMySQLDBConnector(ctx context.Context, cfg *Config, sinkURI *url.URL) (*MySQLDBConnector, error) {
	if sinkURI.Scheme != "mysql" && sinkURI.Scheme != "tidb" {
		return nil, errors.New("")
	}

	// GenerateDSN function parses multiple addresses from the URL (if any)
	// and generates a DSN (Data Source Name) for each one.
	// For each DSN, the function attempts to create a connection and perform a Ping
	// to verify its availability.
	// If any of the DSNs are unavailable, the function immediately returns an error.
	dsn, err := GenerateDSN(ctx, sinkURI, cfg)
	if err != nil {
		return nil, err
	}

	connector := &MySQLDBConnector{dsnList: dsn, nextTry: 0}

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

		db, err := CreateMySQLDBConn(ctx, c.dsnList[c.nextTry])
		c.nextTry = (c.nextTry + 1) % len(c.dsnList)
		if err == nil {
			c.CurrentDB = db
			return nil
		}
	}

	return err
}

// TODO: 设置每次切换的配置
