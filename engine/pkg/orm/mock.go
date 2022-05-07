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

package orm

import (
	"context"
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	gsvr "github.com/dolthub/go-mysql-server/server"
	gsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

func NewMockClient() (Client, error) {
	svr, addr, err := MockBackendDB(tenant.FrameTenantID)
	if err != nil {
		return nil, err
	}
	var store metaclient.StoreConfigParams
	store.SetEndpoints(addr)
	store.Auth.User = "root"

	cli, err := NewClient(store, NewDefaultDBConfig())
	if err != nil {
		svr.Close()
		return nil, err
	}
	err = cli.Initialize(context.Background())
	if err != nil {
		svr.Close()
		return nil, err
	}

	return &mockClient{
		Client: cli,
		svr:    svr,
	}, nil
}

type mockClient struct {
	Client

	svr *gsvr.Server
}

func (m *mockClient) Close() error {
	m.Client.Close()

	if m.svr != nil {
		m.svr.Close()
	}

	return nil
}

func allocTempURL() string {
	port, err := freeport.GetFreePort()
	if err != nil {
		return ""
	}
	return fmt.Sprintf("localhost:%d", port)
}

// using go-mysql-server as backend storage
// https://github.com/dolthub/go-mysql-server
// go-mysql-server not support unique index
// ref: https://github.com/dolthub/go-mysql-server/issues/571
func MockBackendDB(db string) (*gsvr.Server, string, error) {
	addr := allocTempURL()
	engine := sqle.NewDefault(
		gsql.NewDatabaseProvider(
			createTestDatabase(db),
			information_schema.NewInformationSchemaDatabase(),
		))

	config := gsvr.Config{
		Protocol: "tcp",
		Address:  addr,
	}

	s, err := gsvr.NewDefaultServer(config, engine)
	if err != nil {
		return nil, "", err
	}

	go func() {
		err := s.Start()
		if err != nil {
			panic(err)
		}
	}()

	return s, addr, err
}

func createTestDatabase(db string) *memory.Database {
	return memory.NewDatabase(db)
}

func CloseBackendDB(svr *gsvr.Server) {
	if svr != nil {
		svr.Close()
	}
}
