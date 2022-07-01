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
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func randomDBFile() string {
	return uuid.NewGenerator().NewString() + ".db"
}

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	// ref:https://www.sqlite.org/inmemorydb.html
	// using dsn(file:%s?mode=memory&cache=shared) format here to
	// 1. Create different DB for different TestXXX()
	// 2. Enable DB shared for different connection
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", randomDBFile())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, errors.ErrMetaNewClientFail.Wrap(err)
	}

	cli := &metaOpsClient{
		db: db,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Initialize(ctx); err != nil {
		cli.Close()
		return nil, err
	}

	return cli, nil
}
