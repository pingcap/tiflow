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

package fixtures

import (
	"encoding/json"

	"github.com/pingcap/dm/openapi"
)

var (
	noShardTaskJSONStr = `
	{
		"enhance_online_schema_change": true,
		"meta_schema": "dm_meta",
		"name": "test",
		"on_duplicate": "error",
		"source_config": {
		  "full_migrate_conf": {
			"data_dir": "./exported_data",
			"export_threads": 4,
			"import_threads": 16
		  },
		  "incr_migrate_conf": { "repl_batch": 200, "repl_threads": 32 },
		  "source_conf": [{ "source_name": "mysql-replica-01" }]
		},
		"table_migrate_rule": [
		  {
			"source": {
			  "schema": "some_db",
			  "source_name": "mysql-replica-01",
			  "table": "*"
			},
			"target": { "schema": "new_name_db", "table": "*" }
		  }
		],
		"target_config": {
		  "host": "root",
		  "password": "123456",
		  "port": 4000,
		  "security": null,
		  "user": "root"
		},
		"task_mode": "all"
	  }
`

	shardAndFilterTaskJSONStr = `
	{
		"binlog_filter_rule": {
		  "filterA": { "ignore_event": ["drop database"], "ignore_sql": ["^Drop"] }
		},
		"enhance_online_schema_change": true,
		"meta_schema": "dm_meta",
		"name": "test",
		"on_duplicate": "error",
		"shard_mode": "pessimistic",
		"source_config": {
		  "full_migrate_conf": {
			"data_dir": "./exported_data",
			"export_threads": 4,
			"import_threads": 16
		  },
		  "incr_migrate_conf": { "repl_batch": 200, "repl_threads": 32 },
		  "source_conf": [
			{
			  "binlog_gtid": "",
			  "binlog_name": "mysql-bin.001",
			  "binlog_pos": 0,
			  "source_name": "mysql-replica-01"
			},
			{
			  "binlog_gtid": "12e57f06-f360-11eb-8235-585cc2bc66c9:1-24",
			  "binlog_name": "mysql-bin.002",
			  "binlog_pos": 1232,
			  "source_name": "mysql-replica-02"
			}
		  ]
		},
		"table_migrate_rule": [
		  {
			"binlog_filter_rule": ["filterA"],
			"source": {
			  "schema": "db_*",
			  "source_name": "mysql-replica-01",
			  "table": "tbl_1"
			},
			"target": { "schema": "db1", "table": "tbl" }
		  },
		  {
			"source": {
			  "schema": "db_*",
			  "source_name": "mysql-replica-02",
			  "table": "tbl_1"
			},
			"target": { "schema": "db1", "table": "tbl" }
		  }
		],
		"target_config": {
		  "host": "root",
		  "password": "123456",
		  "port": 4000,
		  "security": null,
		  "user": "root"
		},
		"task_mode": "all"
	  }
	`
)

// GenNoShardOpenAPITaskForTest generates a no-shard openapi.Task for test.
func GenNoShardOpenAPITaskForTest() (openapi.Task, error) {
	t := openapi.Task{}
	err := json.Unmarshal([]byte(noShardTaskJSONStr), &t)
	return t, err
}

// GenShardAndFilterOpenAPITaskForTest generates a shard-and-filter openapi.Task for test.
func GenShardAndFilterOpenAPITaskForTest() (openapi.Task, error) {
	t := openapi.Task{}
	err := json.Unmarshal([]byte(shardAndFilterTaskJSONStr), &t)
	return t, err
}
