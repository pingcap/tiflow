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

	"github.com/pingcap/tiflow/dm/openapi"
)

var sourceStr = `
	{
		"source_name": "mysql-01",
		"host": "127.0.0.1",
		"port": 3306,
		"user": "root",
		"password": "123456",
		"enable_gtid": false,
		"security": {
		  "ssl_ca_content": "",
		  "ssl_cert_content": "",
		  "ssl_key_content": "",
		  "cert_allowed_cn": [
			"string"
		  ]
		},
		"purge": {
		  "interval": 3600,
		  "expires": 0,
		  "remain_space": 15
		},
		"relay_config": {
		  "enable_relay": true,
		  "relay_binlog_name": "mysql-bin.000002",
		  "relay_binlog_gtid": "e9a1fc22-ec08-11e9-b2ac-0242ac110003:1-7849",
		  "relay_dir": "./relay_log"
		}
	  }
`

// GenOpenAPISourceForTest generates openapi.Source for test.
func GenOpenAPISourceForTest() (openapi.Source, error) {
	s := openapi.Source{}
	err := json.Unmarshal([]byte(sourceStr), &s)
	return s, err
}
