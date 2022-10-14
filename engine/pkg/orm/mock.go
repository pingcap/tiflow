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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/zap"
)

func randomDBFile() string {
	return uuid.NewGenerator().NewString() + ".db"
}

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	// ref:https://www.sqlite.org/inmemorydb.html
	// using dsn(file:%s?mode=memory&cache=shared) format here to
	// 1. Create different DB for different TestXXX() to enable concurrent execution
	// 2. Enable DB shared for different connection
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", randomDBFile())
	cc, err := mock.NewClientConnForSQLite(dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := InitAllFrameworkModels(ctx, cc); err != nil {
		log.Error("initialize all framework models fail", zap.Error(err))
		cc.Close()
		return nil, err
	}

	cli, err := NewClient(cc)
	if err != nil {
		log.Error("new mock client fail", zap.Error(err))
		cc.Close()
		return nil, err
	}

	return &sqliteClient{
		Client: cli,
		conn:   cc,
	}, nil
}

type sqliteClient struct {
	Client
	conn metaModel.ClientConn
}

func (c *sqliteClient) Close() error {
	if c.Client != nil {
		c.Client.Close()
	}

	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}
