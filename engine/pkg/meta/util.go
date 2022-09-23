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

package meta

import (
	"context"
	"fmt"

	"github.com/pingcap/tiflow/engine/pkg/dbutil"
	"github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// CreateSchemaIfNotExists creates a db schema if not exists
func CreateSchemaIfNotExists(ctx context.Context, storeConf model.StoreConfig) error {
	schema := storeConf.Schema
	// clear the schema to open a connection without any schema
	storeConf.Schema = ""
	dsn, err := model.GenerateDSNByParams(&storeConf, nil)
	if err != nil {
		return err
	}
	db, err := dbutil.NewSQLDB("mysql", dsn, nil /*DBConfig*/)
	if err != nil {
		return nil
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE if not exists %s", schema)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}
