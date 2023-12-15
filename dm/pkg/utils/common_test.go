// Copyright 2020 PingCAP, Inc.
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

package utils

import (
	"bytes"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTrimCtrlChars(t *testing.T) {
	t.Parallel()

	ddl := "create table if not exists foo.bar(id int)"
	controlChars := make([]byte, 0, 33)
	nul := byte(0x00)
	for i := 0; i < 32; i++ {
		controlChars = append(controlChars, nul)
		nul++
	}
	controlChars = append(controlChars, 0x7f)

	parser2 := parser.New()
	var buf bytes.Buffer
	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := TrimCtrlChars(buf.String())
		require.Equal(t, ddl, newDDL)

		_, err := parser2.ParseOneStmt(newDDL, "", "")
		require.NoError(t, err)
		buf.Reset()
	}
}

func TestTrimQuoteMark(t *testing.T) {
	t.Parallel()

	cases := [][]string{
		{`"123"`, `123`},
		{`123`, `123`},
		{`"123`, `"123`},
		{`'123'`, `'123'`},
	}
	for _, ca := range cases {
		require.Equal(t, TrimQuoteMark(ca[0]), ca[1])
	}
}

func TestCompareShardingDDLs(t *testing.T) {
	t.Parallel()

	var (
		DDL1 = "alter table add column c1 int"
		DDL2 = "alter table add column c2 text"
	)

	// different DDLs
	require.False(t, CompareShardingDDLs([]string{DDL1}, []string{DDL2}))

	// different length
	require.False(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2}))

	// same DDLs
	require.True(t, CompareShardingDDLs([]string{DDL1}, []string{DDL1}))
	require.True(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL1, DDL2}))

	// same contents but different order
	require.True(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2, DDL1}))
}

func TestDDLLockID(t *testing.T) {
	t.Parallel()

	task := "test"
	id := GenDDLLockID(task, "db", "tbl")
	require.Equal(t, "test-`db`.`tbl`", id)
	require.Equal(t, task, ExtractTaskFromLockID(id))

	id = GenDDLLockID(task, "d`b", "tb`l")
	require.Equal(t, "test-`d``b`.`tb``l`", id)
	require.Equal(t, task, ExtractTaskFromLockID(id))

	// invalid ID
	require.Equal(t, "", ExtractTaskFromLockID("invalid-lock-id"))
}

func TestNonRepeatStringsEqual(t *testing.T) {
	t.Parallel()

	require.True(t, NonRepeatStringsEqual([]string{}, []string{}))
	require.True(t, NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "1"}))
	require.False(t, NonRepeatStringsEqual([]string{}, []string{"1"}))
	require.False(t, NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "3"}))
}

func TestGoLogWrapper(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	// to avoid data race since there's concurrent test case writing log.L()
	l := log.Logger{Logger: zap.NewNop()}
	go GoLogWrapper(l, func() {
		defer wg.Done()
		panic("should be captured")
	})
	wg.Wait()
	// if GoLogWrapper didn't catch it, this case will fail.
}
