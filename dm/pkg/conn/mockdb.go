// Copyright 2021 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"
	tidbConfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/tikv/client-go/v2/testutils"
)

type mockDBProvider struct {
	verDB *sql.DB // verDB user for show version.
	db    *sql.DB
	// customDB defines a db that will never be close
	// TODO: we should use customDB for all mock.
	customDB *sql.DB
}

// Apply will build BaseDB with DBConfig.
func (d *mockDBProvider) Apply(config ScopedDBConfig) (*BaseDB, error) {
	if d.verDB != nil {
		if err := d.verDB.Ping(); err == nil {
			// nolint:nilerr
			return NewBaseDBForTest(d.verDB), nil
		}
	}
	if d.customDB != nil {
		if err := d.customDB.Ping(); err == nil {
			// nolint:nilerr
			return NewMockDB(d.customDB), nil
		}
	}
	return NewBaseDBForTest(d.db), nil
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

// MockDefaultDBProvider return a mocked db for unit test.
func MockDefaultDBProvider() (sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.db = db
	} else {
		DefaultDBProvider = &mockDBProvider{db: db}
	}
	return mock, nil
}

// InitVersionDB return a mocked db for unit test's show version.
func InitVersionDB() sqlmock.Sqlmock {
	// nolint:errcheck
	db, mock, _ := sqlmock.New()
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.verDB = db
	} else {
		DefaultDBProvider = &mockDBProvider{verDB: db}
	}
	return mock
}

func InitMockDBFull() (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.db = db
	} else {
		DefaultDBProvider = &mockDBProvider{db: db}
	}
	return db, mock, err
}

func InitMockDBNotClose() (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}
	if mdbp, ok := DefaultDBProvider.(*mockDBProvider); ok {
		mdbp.customDB = db
	} else {
		DefaultDBProvider = &mockDBProvider{customDB: db}
	}
	return db, mock, err
}

// TODO: export Config in https://github.com/pingcap/tidb/blob/a8fa29b56d633b1ec843e21cb89131dd4fd601db/br/pkg/mock/mock_cluster.go#L35
// Cluster is mock tidb cluster.
type Cluster struct {
	*server.Server
	testutils.Cluster
	kv.Storage
	*server.TiDBDriver
	*domain.Domain
	Port int
}

// NewCluster create a new mock cluster.
func NewCluster() (*Cluster, error) {
	cluster := &Cluster{}

	storage, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cluster.Cluster = c
		}),
	)
	if err != nil {
		return nil, err
	}
	cluster.Storage = storage

	session.SetSchemaLease(time.Second)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(storage)
	if err != nil {
		return nil, err
	}
	cluster.Domain = dom

	return cluster, nil
}

// Start runs a mock cluster.
func (mock *Cluster) Start() error {
	// choose a random available port
	l1, _ := net.Listen("tcp", "127.0.0.1:")
	statusPort := l1.Addr().(*net.TCPAddr).Port

	// choose a random available port
	l2, _ := net.Listen("tcp", "127.0.0.1:")
	addrPort := l2.Addr().(*net.TCPAddr).Port

	mock.TiDBDriver = server.NewTiDBDriver(mock.Storage)
	cfg := tidbConfig.NewConfig()
	cfg.Port = uint(addrPort)
	cfg.Store = "tikv"
	cfg.Status.StatusPort = uint(statusPort)
	cfg.Status.ReportStatus = true
	cfg.Socket = fmt.Sprintf("/tmp/tidb-mock-%d.sock", time.Now().UnixNano())

	// close port for next listen in NewServer
	l1.Close()
	l2.Close()
	svr, err := server.NewServer(cfg, mock.TiDBDriver)
	if err != nil {
		return err
	}
	mock.Server = svr
	mock.Server.SetDomain(mock.Domain)
	go func() {
		if err1 := svr.Run(nil); err1 != nil {
			panic(err1)
		}
	}()
	waitUntilServerOnline(cfg.Status.StatusPort)
	mock.Port = addrPort
	return nil
}

// Stop stops a mock cluster.
func (mock *Cluster) Stop() {
	if mock.Domain != nil {
		mock.Domain.Close()
	}
	if mock.Storage != nil {
		_ = mock.Storage.Close()
	}
	if mock.Server != nil {
		mock.Server.Close()
	}
}

func waitUntilServerOnline(statusPort uint) {
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry := 0; retry < 100; retry++ {
		// nolint:gosec,noctx
		// #nosec G107
		resp, err := http.Get(statusURL)
		if err == nil {
			// Ignore errors.
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
}
