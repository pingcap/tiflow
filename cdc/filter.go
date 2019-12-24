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

import (
	"regexp"

	"github.com/pingcap/ticdc/cdc/model"
)

func filterBySchemaAndTable(t *model.Txn) {
	toIgnore := regexp.MustCompile("(?i)^(INFORMATION_SCHEMA|PERFORMANCE_SCHEMA|MYSQL)$")
	if t.IsDDL() {
		if toIgnore.MatchString(t.DDL.Database) {
			t.DDL = nil
		}
	} else {
		filteredDMLs := make([]*model.DML, 0, len(t.DMLs))
		for _, dml := range t.DMLs {
			if !toIgnore.MatchString(dml.Database) {
				filteredDMLs = append(filteredDMLs, dml)
			}
		}
		t.DMLs = filteredDMLs
	}
}
