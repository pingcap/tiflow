package orm

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	gsvr "github.com/dolthub/go-mysql-server/server"
	gsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/phayes/freeport"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/retry"
)

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	svr, addr, err := RetryMockBackendDB(tenant.FrameTenantID)
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

// RetryMockBackendDB retry to create backend DB if meet 'address already in use' error
// for at most 3 times.
func RetryMockBackendDB(db string) (s *gsvr.Server, addr string, err error) {
	err = retry.Do(context.TODO(), func() error {
		s, addr, err = mockBackendDB(db)
		if err != nil {
			return err
		}
		return nil
	},
		retry.WithBackoffBaseDelay(1000 /* 1000 ms */),
		retry.WithBackoffMaxDelay(3000 /* 3 seconds */),
		retry.WithMaxTries(3 /* fail after 10 seconds*/),
		retry.WithIsRetryableErr(func(err error) bool {
			if strings.Contains(err.Error(), "address already in use") {
				log.L().Info("address already in use, retry again")
				return true
			}
			return false
		}),
	)

	return
}

// mockBackendDB creates a mock mysql using go-mysql-server as backend storage
// https://github.com/dolthub/go-mysql-server
// go-mysql-server not support unique index
// ref: https://github.com/dolthub/go-mysql-server/issues/571
func mockBackendDB(db string) (s *gsvr.Server, addr string, err error) {
	addr = allocTempURL()
	engine := sqle.NewDefault(
		gsql.NewDatabaseProvider(
			createTestDatabase(db),
			information_schema.NewInformationSchemaDatabase(),
		))

	config := gsvr.Config{
		Protocol: "tcp",
		Address:  addr,
	}

	failpoint.Inject("MockDBAddressAlreadyUse", func() {
		failpoint.Return(nil, "", errors.New("address already in use"))
	})

	failpoint.Inject("MockDBOtherError", func() {
		failpoint.Return(nil, "", errors.New("error"))
	})

	s, err = gsvr.NewDefaultServer(config, engine)
	if err != nil {
		return nil, "", err
	}

	go func() {
		err = s.Start()
		if err != nil {
			panic(err)
		}
	}()

	return s, addr, err
}

func createTestDatabase(db string) *memory.Database {
	return memory.NewDatabase(db)
}

// CloseBackendDB closes backend db
func CloseBackendDB(svr *gsvr.Server) {
	if svr != nil {
		svr.Close()
	}
}
