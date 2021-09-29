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

package quotes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteSchema(t *testing.T) {
	t.Parallel()

	cases := []struct {
		schema   string
		table    string
		expected string
	}{
		{"testdb", "t1", "`testdb`.`t1`"},
		{"test-db-1`2", "t`1", "`test-db-1``2`.`t``1`"},
	}
	for _, testCase := range cases {
		name := QuoteSchema(testCase.schema, testCase.table)
		require.Equal(t, testCase.expected, name)
	}
}

func TestQuoteName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		expected string
	}{
		{"tbl", "`tbl`"},
		{"t`bl", "`t``bl`"},
		{"t``bl", "`t````bl`"},
		{"", "``"},
	}
	for _, testCase := range cases {
		escaped := QuoteName(testCase.name)
		require.Equal(t, testCase.expected, escaped)
	}
}

func TestEscapeName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		expected string
	}{
		{"tbl", "tbl"},
		{"t`bl", "t``bl"},
		{"t``bl", "t````bl"},
		{"`", "``"},
		{"", ""},
	}
	for _, testCase := range cases {
		escaped := EscapeName(testCase.name)
		require.Equal(t, testCase.expected, escaped)
	}
}
