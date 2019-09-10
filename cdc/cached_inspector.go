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

package cdc

import "database/sql"

type cachedInspector struct {
	db          *sql.DB
	cache       map[string]*tableInfo
	tableGetter func(*sql.DB, string, string) (*tableInfo, error)
}

func newCachedInspector(db *sql.DB) *cachedInspector {
	return &cachedInspector{
		db:          db,
		cache:       make(map[string]*tableInfo),
		tableGetter: getTableInfo,
	}
}

var _ tableInspector = &cachedInspector{}

func (i *cachedInspector) Get(schema, table string) (*tableInfo, error) {
	key := quoteSchema(schema, table)
	t, ok := i.cache[key]
	if !ok {
		var err error
		t, err = i.tableGetter(i.db, schema, table)
		if err != nil {
			return nil, err
		}
		i.cache[key] = t
	}
	return t, nil
}

func (i *cachedInspector) Refresh(schema, table string) {
	key := quoteSchema(schema, table)
	delete(i.cache, key)
}
