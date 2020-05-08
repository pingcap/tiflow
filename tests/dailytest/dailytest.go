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

package dailytest

import (
	"database/sql"

	"github.com/pingcap/log"
)

// RunMultiSource runs the test that need multi instance TiDB, one instance for one *sql.DB* in srcs
func RunMultiSource(srcs []*sql.DB, targetDB *sql.DB, schema string) {
	runDDLTest(srcs, targetDB, schema)
}

// Run runs the daily test
func Run(sourceDB *sql.DB, targetDB *sql.DB, schema string, workerCount int, jobCount int, batch int) {

	// run the simple test case
	RunCase(sourceDB, targetDB, schema)

	log.S().Info("test pass!!!")

}
