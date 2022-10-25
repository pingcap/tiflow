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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var customID int64

var netTimeout = utils.DefaultDBTimeout

// DBProvider providers BaseDB instance.
type DBProvider interface {
	Apply(config *config.DBConfig) (*BaseDB, error)
}

// DefaultDBProviderImpl is default DBProvider implement.
type DefaultDBProviderImpl struct{}

// DefaultDBProvider is global instance of DBProvider.
var DefaultDBProvider DBProvider

func init() {
	DefaultDBProvider = &DefaultDBProviderImpl{}
}

// Apply will build BaseDB with DBConfig.
func (d *DefaultDBProviderImpl) Apply(config *config.DBConfig) (*BaseDB, error) {
	// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
	// https://github.com/go-sql-driver/mysql#maxallowedpacket
	hostPort := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=0",
		config.User, config.Password, hostPort)

	doFuncInClose := func() {}
	if config.Security != nil {
		if loadErr := config.Security.LoadTLSContent(); loadErr != nil {
			return nil, terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err := util.NewTLSConfig(
			util.WithCAContent(config.Security.SSLCABytes),
			util.WithCertAndKeyContent(config.Security.SSLCertBytes, config.Security.SSLKEYBytes),
			util.WithVerifyCommonName(config.Security.CertAllowedCN),
		)
		if err != nil {
			return nil, terror.ErrConnInvalidTLSConfig.Delegate(err)
		}

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

	for key, val := range config.Session {
		// for num such as 1/"1", format as key='1'
		// for string, format as key='string'
		// both are valid for mysql and tidb
		dsn += fmt.Sprintf("&%s='%s'", key, url.QueryEscape(val))
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
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
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	db.SetMaxIdleConns(maxIdleConns)

	return NewBaseDB(db, doFuncInClose), nil
}

// BaseDB wraps *sql.DB, control the BaseConn.
type BaseDB struct {
	DB *sql.DB

	mu sync.Mutex // protects following fields
	// hold all db connections generated from this BaseDB
	conns map[*BaseConn]struct{}

	Retry retry.Strategy

	// this function will do when close the BaseDB
	doFuncInClose []func()
}

// NewBaseDB returns *BaseDB object.
func NewBaseDB(db *sql.DB, doFuncInClose ...func()) *BaseDB {
	conns := make(map[*BaseConn]struct{})
	return &BaseDB{DB: db, conns: conns, Retry: &retry.FiniteRetryStrategy{}, doFuncInClose: doFuncInClose}
}

// GetBaseConn retrieves *BaseConn which has own retryStrategy.
func (d *BaseDB) GetBaseConn(ctx context.Context) (*BaseConn, error) {
	ctx, cancel := context.WithTimeout(ctx, netTimeout)
	defer cancel()
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	baseConn := NewBaseConn(conn, d.Retry)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[baseConn] = struct{}{}
	return baseConn, nil
}

func (d *BaseDB) ExecContext(tctx *tcontext.Context, query string, args ...interface{}) (sql.Result, error) {
	if tctx.L().Core().Enabled(zap.DebugLevel) {
		tctx.L().Debug("exec context",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)))
	}
	return d.DB.ExecContext(tctx.Ctx, query, args...)
}

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
			return nil, perrors.Trace(err)
		}
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					tctx.L().Warn("failed to rollback", zap.Error(perrors.Trace(rollbackErr)))
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
				return nil, perrors.Trace(err)
			}
		}
		return nil, perrors.Trace(err)
	}

	_, _, err := retryer.Apply(tctx, workFunc)
	return err
}

// CloseBaseConn release BaseConn resource from BaseDB, and close BaseConn.
func (d *BaseDB) CloseBaseConn(conn *BaseConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, conn)
	return conn.close()
}

// CloseBaseConnWithoutErr close the base connect and output a warn log if meets an error.
func CloseBaseConnWithoutErr(d *BaseDB, conn *BaseConn) {
	if err1 := d.CloseBaseConn(conn); err1 != nil {
		log.L().Warn("close db connection failed", zap.Error(err1))
	}
}

// Close release *BaseDB resource.
func (d *BaseDB) Close() error {
	if d == nil || d.DB == nil {
		return nil
	}
	var err error
	d.mu.Lock()
	defer d.mu.Unlock()
	for conn := range d.conns {
		terr := conn.close()
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
