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

package sqlkv

import (
	"database/sql"
	"strconv"
	"sync"

	"github.com/pingcap/tiflow/engine/pkg/dbutil"
	"github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// NewClientConnImpl return a new clientConnImpl
func NewClientConnImpl(storeConf *model.StoreConfig) (*clientConnImpl, error) {
	if storeConf == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("store config is nil")
	}

	if storeConf.StoreType != model.StoreTypeMySQL {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("sql conn but get type:%s",
			storeConf.StoreType)
	}

	db, err := NewSQLDB(storeConf)
	if err != nil {
		return nil, err
	}

	return &clientConnImpl{
		db: db,
	}, nil
}

type clientConnImpl struct {
	rwLock sync.RWMutex
	db     *sql.DB
}

// GetConn implements GetConn of ClientConn
// Return *sql.DB if no error
func (cc *clientConnImpl) GetConn() (interface{}, error) {
	cc.rwLock.RLock()
	defer cc.rwLock.RUnlock()

	if cc.db == nil {
		return nil, errors.ErrMetaOpFail.GenWithStackByArgs("connection is uninitialized")
	}

	return cc.db, nil
}

// Close implements Close of ClientConn
func (cc *clientConnImpl) Close() error {
	cc.rwLock.Lock()
	defer cc.rwLock.Unlock()

	if cc.db != nil {
		cc.db.Close()
		cc.db = nil
	}

	return nil
}

// StoreType implements StoreType of ClientConn
func (cc *clientConnImpl) StoreType() model.StoreType {
	return model.StoreTypeMySQL
}

// NewSQLDB news a sql.DB.
func NewSQLDB(storeConf *model.StoreConfig) (*sql.DB, error) {
	pairs := map[string]string{
		"sql_mode": strconv.Quote(dbutil.GetSQLStrictMode()),
	}
	dsn, err := model.GenerateDSNByParams(storeConf, pairs)
	if err != nil {
		return nil, err
	}
	return dbutil.NewSQLDB("mysql", dsn, storeConf.DBConf)
}
