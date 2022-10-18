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

package schema

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func TestSysbench(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ddl := GetSysbenchCreateTableStatement(1)
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(ddl)
	require.Equal(t, "sbtest1", job.TableName)
	dml := BuildSysbenchInsertSql(1, 1)
	helper.Tk().MustExec(dml)
}

func TestBank(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ddl := GetBankCreateTableStatement(1)
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(ddl)
	require.Equal(t, "cdc_test_bank1", job.TableName)
	dml := BuildBankInsertSql(1, 1)
	helper.Tk().MustExec(dml)
}

func TestExpress(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ddl := GetExpressCreateTableStatement(1)
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(ddl)
	require.Equal(t, "cdc_test_express1", job.TableName)
	dml := BuildExpressReplaceSql(1, 1)
	helper.Tk().MustExec(dml)
}
