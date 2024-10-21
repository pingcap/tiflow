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

package config

import (
	"fmt"

	"github.com/pingcap/errors"
)

const (
	dmConfig = `# Diff Configuration.

######################### Global config #########################

check-thread-count = 4

export-fix-sql = true

check-struct-only = false

dm-addr = "http://127.0.0.1:8261"

dm-task = "test"

######################### Task config #########################
[task]
	output-dir = "./output"

	target-check-tables = ["hb_test.*"]

`

	normConfig = `# Diff Configuration.

######################### Global config #########################

check-thread-count = 4

export-fix-sql = true

check-struct-only = false


######################### Datasource config #########################
[data-sources]
[data-sources.mysql1]
	host = "127.0.0.1"
	port = 3306
	user = "root"
	password = ""

	route-rules = ["rule1", "rule2"]

[data-sources.tidb0]
	host = "127.0.0.1"
	port = 4000
	user = "root"
	password = ""
	# security.ca-path = "..."
	# security.cert-path = "..."
	# security.key-path = "..."
	# snapshot = "386902609362944000"

########################### Routes ###########################
[routes]
[routes.rule1]
schema-pattern = "test_*"
table-pattern = "t_*"
target-schema = "test"
target-table = "t"

[routes.rule2]
schema-pattern = "test2_*"
table-pattern = "t2_*"
target-schema = "test2"
target-table = "t2"

######################### Task config #########################
[task]
	output-dir = "./output"

	source-instances = ["mysql1"]

	target-instance = "tidb0"

	target-check-tables = ["schema*.table*", "!c.*", "test2.t2"]

	target-configs = ["config1"]

######################### Table config #########################
[table-configs.config1]
target-tables = ["schema*.test*", "test2.t2"]
range = "age > 10 AND age < 20"
index-fields = ["col1","col2"]
ignore-columns = ["",""]
chunk-size = 0
collation = ""

`
)

func ExportTemplateConfig(configType string) error {
	switch configType {
	case "dm", "DM", "Dm", "dM":
		fmt.Print(dmConfig)
	case "norm", "normal", "Norm", "Normal":
		fmt.Print(normConfig)
	default:
		return errors.Errorf("Error: unexpect template name: %s\n-T dm: export a dm config\n-T norm: export a normal config\n", configType)
	}
	return nil
}
