// Copyright 2019 PingCAP, Inc.
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

package conn

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

var customID int64

var netTimeout = DefaultDBTimeout

// DBProvider providers BaseDB instance.
type DBProvider interface {
	Apply(config ScopedDBConfig) (*BaseDB, error)
}

// DefaultDBProviderImpl is default DBProvider implement.
type DefaultDBProviderImpl struct{}

// DefaultDBProvider is global instance of DBProvider.
var DefaultDBProvider DBProvider

func init() {
	DefaultDBProvider = &DefaultDBProviderImpl{}
}

type ScopedDBConfig struct {
	*dbconfig.DBConfig
	Scope terror.ErrScope
}

func UpstreamDBConfig(cfg *dbconfig.DBConfig) ScopedDBConfig {
	return ScopedDBConfig{
		DBConfig: cfg,
		Scope:    terror.ScopeUpstream,
	}
}

func DownstreamDBConfig(cfg *dbconfig.DBConfig) ScopedDBConfig {
	return ScopedDBConfig{
		DBConfig: cfg,
		Scope:    terror.ScopeDownstream,
	}
}

func GetUpstreamDB(cfg *dbconfig.DBConfig) (*BaseDB, error) {
	return DefaultDBProvider.Apply(UpstreamDBConfig(cfg))
}

func GetDownstreamDB(cfg *dbconfig.DBConfig) (*BaseDB, error) {
	return DefaultDBProvider.Apply(DownstreamDBConfig(cfg))
}

// Apply will build BaseDB with DBConfig.
func (d *DefaultDBProviderImpl) Apply(config ScopedDBConfig) (*BaseDB, error) {
	// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
	// https://github.com/go-sql-driver/mysql#maxallowedpacket
	hostPort := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	net := "tcp"
	if config.Net != "" {
		net = config.Net
	}
	dsn := fmt.Sprintf("%s:%s@%s(%s)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=0",
		config.User, config.Password, net, hostPort)

	doFuncInClose := func() {}
	if config.Security != nil {
		if loadErr := config.Security.LoadTLSContent(); loadErr != nil {
			return nil, terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err := util.NewTLSConfig(
			util.WithCAContent(config.Security.SSLCABytes),
			util.WithCertAndKeyContent(config.Security.SSLCertBytes, config.Security.SSLKeyBytes),
			util.WithVerifyCommonName(config.Security.CertAllowedCN),
		)
		if err != nil {
			return nil, terror.ErrConnInvalidTLSConfig.Delegate(err)
		}

		if tlsConfig != nil {
			name := "dm" + strconv.FormatInt(atomic.AddInt64(&customID, 1), 10)
			err = mysql.RegisterTLSConfig(name, tlsConfig)
			if err != nil {
				return nil, terror.ErrConnRegistryTLSConfig.Delegate(err)
			}
			dsn += "&tls=" + name

			doFuncInClose = func() {
				mysql.DeregisterTLSConfig(name)
			}
		}
	}

	var maxIdleConns int
	rawCfg := config.RawDBCfg
	if rawCfg != nil {
		if rawCfg.ReadTimeout != "" {
			dsn += fmt.Sprintf("&readTimeout=%s", rawCfg.ReadTimeout)
		}
		if rawCfg.WriteTimeout != "" {
			dsn += fmt.Sprintf("&writeTimeout=%s", rawCfg.WriteTimeout)
		}
		maxIdleConns = rawCfg.MaxIdleConns
	}

	var setFK bool
	for key, val := range config.Session {
		// for num such as 1/"1", format as key='1'
		// for string, format as key='string'
		// both are valid for mysql and tidb
		if strings.ToLower(key) == "foreign_key_checks" {
			setFK = true
		}
		dsn += fmt.Sprintf("&%s='%s'", key, url.QueryEscape(val))
	}

	if !setFK {
		dsn += "&foreign_key_checks=0"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, config.Scope, terror.ErrDBDriverError)
	}

	ctx, cancel := context.WithTimeout(context.Background(), netTimeout)
	defer cancel()
	err = db.PingContext(ctx)
	failpoint.Inject("failDBPing", func(_ failpoint.Value) {
		err = errors.New("injected error")
	})
	if err != nil {
		db.Close()
		doFuncInClose()
		return nil, terror.DBErrorAdapt(err, config.Scope, terror.ErrDBDriverError)
	}

	db.SetMaxIdleConns(maxIdleConns)

	return NewBaseDB(db, config.Scope, doFuncInClose), nil
}

// BaseDB wraps *sql.DB, control the BaseConn.
type BaseDB struct {
	DB *sql.DB

	mu sync.Mutex // protects following fields
	// hold all db connections generated from this BaseDB
	conns map[*BaseConn]struct{}

	Retry retry.Strategy

	Scope terror.ErrScope
	// this function will do when close the BaseDB
	doFuncInClose []func()

	// only use in unit test
	doNotClose bool
}

// NewBaseDB returns *BaseDB object for test.
func NewBaseDB(db *sql.DB, scope terror.ErrScope, doFuncInClose ...func()) *BaseDB {
	conns := make(map[*BaseConn]struct{})
	return &BaseDB{
		DB:            db,
		conns:         conns,
		Retry:         &retry.FiniteRetryStrategy{},
		Scope:         scope,
		doFuncInClose: doFuncInClose,
	}
}

// NewBaseDBForTest returns *BaseDB object for test.
func NewBaseDBForTest(db *sql.DB, doFuncInClose ...func()) *BaseDB {
	conns := make(map[*BaseConn]struct{})
	return &BaseDB{
		DB:            db,
		conns:         conns,
		Retry:         &retry.FiniteRetryStrategy{},
		Scope:         terror.ScopeNotSet,
		doFuncInClose: doFuncInClose,
	}
}

// NewMockDB returns *BaseDB object for mock.
func NewMockDB(db *sql.DB, doFuncInClose ...func()) *BaseDB {
	baseDB := NewBaseDBForTest(db, doFuncInClose...)
	baseDB.doNotClose = true
	return baseDB
}

// GetBaseConn retrieves *BaseConn which has own retryStrategy.
func (d *BaseDB) GetBaseConn(ctx context.Context) (*BaseConn, error) {
	ctx, cancel := context.WithTimeout(ctx, netTimeout)
	defer cancel()
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, d.Scope, terror.ErrDBDriverError)
	}
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, d.Scope, terror.ErrDBDriverError)
	}
	baseConn := NewBaseConn(conn, d.Scope, d.Retry)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[baseConn] = struct{}{}
	return baseConn, nil
}

// TODO: retry can be done inside the BaseDB.
func (d *BaseDB) ExecContext(tctx *tcontext.Context, query string, args ...interface{}) (sql.Result, error) {
	if tctx.L().Core().Enabled(zap.DebugLevel) {
		tctx.L().Debug("exec context",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)))
	}
	return d.DB.ExecContext(tctx.Ctx, query, args...)
}

// TODO: retry can be done inside the BaseDB.
func (d *BaseDB) QueryContext(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if tctx.L().Core().Enabled(zap.DebugLevel) {
		tctx.L().Debug("query context",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)))
	}
	return d.DB.QueryContext(tctx.Ctx, query, args...)
}

func (d *BaseDB) DoTxWithRetry(tctx *tcontext.Context, queries []string, args [][]interface{}, retryer retry.Retryer) error {
	workFunc := func(tctx *tcontext.Context) (interface{}, error) {
		var (
			err error
			tx  *sql.Tx
		)
		tx, err = d.DB.BeginTx(tctx.Ctx, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					tctx.L().Warn("failed to rollback", zap.Error(errors.Trace(rollbackErr)))
				}
			} else {
				err = tx.Commit()
			}
		}()
		for i := range queries {
			q := queries[i]
			if tctx.L().Core().Enabled(zap.DebugLevel) {
				tctx.L().Debug("exec in tx",
					zap.String("query", utils.TruncateString(q, -1)),
					zap.String("argument", utils.TruncateInterface(args[i], -1)))
			}
			if _, err = tx.ExecContext(tctx.Ctx, q, args[i]...); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return nil, errors.Trace(err)
	}

	_, _, err := retryer.Apply(tctx, workFunc)
	return err
}

// CloseConn release BaseConn resource from BaseDB, and returns the connection to the connection pool,
// has the same meaning of sql.Conn.Close.
func (d *BaseDB) CloseConn(conn *BaseConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, conn)
	return conn.close()
}

// CloseConnWithoutErr release BaseConn resource from BaseDB, and returns the connection to the connection pool,
// has the same meaning of sql.Conn.Close, and log warning on error.
func (d *BaseDB) CloseConnWithoutErr(conn *BaseConn) {
	if err := d.CloseConn(conn); err != nil {
		log.L().Warn("close db connection failed", zap.Error(err))
	}
}

// ForceCloseConn release BaseConn resource from BaseDB, and close BaseConn completely(not return to the connection pool).
func (d *BaseDB) ForceCloseConn(conn *BaseConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, conn)
	return conn.forceClose()
}

// ForceCloseConnWithoutErr close the connection completely(not return to the conn pool),
// and output a warning log if meets an error.
func (d *BaseDB) ForceCloseConnWithoutErr(conn *BaseConn) {
	if err1 := d.ForceCloseConn(conn); err1 != nil {
		log.L().Warn("close db connection failed", zap.Error(err1))
	}
}

// Close release *BaseDB resource.
func (d *BaseDB) Close() error {
	if d == nil || d.DB == nil || d.doNotClose {
		return nil
	}
	var err error
	d.mu.Lock()
	defer d.mu.Unlock()
	for conn := range d.conns {
		terr := conn.forceClose()
		if err == nil {
			err = terr
		}
	}
	terr := d.DB.Close()
	for _, f := range d.doFuncInClose {
		f()
	}

	if err == nil {
		return terr
	}

	return err
}
