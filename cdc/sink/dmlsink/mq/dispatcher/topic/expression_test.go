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

package topic

import (
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestSubstituteTopicExpression(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		expression string
		schema     string
		table      string
		wantErr    string
		expected   string
	}{
		{
			name:       "valid expression containing '{schema}', but with no prefix and suffix",
			expression: "{schema}",
			schema:     "abc",
			table:      "",
			expected:   "abc",
		},
		{
			name:       "valid expression containing '{schema}', with prefix",
			expression: "abc{schema}",
			schema:     "def",
			table:      "",
			expected:   "abcdef",
		},
		{
			name:       "valid expression containing '{schema}', with suffix",
			expression: "{schema}def",
			schema:     "hello",
			table:      "",
			expected:   "hellodef",
		},
		{
			name:       "valid expression containing '{schema}', with both prefix and suffix",
			expression: "abc{schema}def",
			schema:     "hello",
			table:      "",
			expected:   "abchellodef",
		},
		{
			expression: "abc{schema}",
			name:       "valid expression containing '{schema}', schema is converted to lower case letters",
			schema:     "DEF",
			table:      "",
			expected:   "abcDEF",
		},
		{
			name:       "valid expression containing '{schema}' and special prefix, schema is converted to lower case letters",
			expression: "abc._-def{schema}abc",
			schema:     "HELLO",
			table:      "",
			expected:   "abc._-defHELLOabc",
		},
		{
			name:       "valid expression containing '{schema}', the kafka disallowed characters in schema are replaced with underscore '_'",
			expression: "abc_def{schema}",
			schema:     "!@#$%^&*()_+.{}",
			table:      "",
			expected:   "abc_def____________.__",
		},
		{
			name:       "valid expression containing '{schema}', the kafka disallowed characters in schema are replaced with underscore '_'",
			expression: "abc{schema}def",
			schema:     "你好",
			table:      "",
			expected:   "abc__def",
		},
		{
			name:       "invalid expression containing '{schema}', the prefix has punctuation marks other than '.', '-' or '_'",
			expression: "{{{{schema}",
			schema:     "abc",
			table:      "",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name:       "invalid expression containing '{schema}', the prefix has unicode characters other than [A-Za-z0-9]*",
			expression: "你好{schema}",
			schema:     "world",
			table:      "",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name:       "invalid expression containing '{schema}', the suffix must be [A-Za-z0-9\\._\\-]*",
			expression: "{schema}}}}}",
			schema:     "abc",
			table:      "",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name:       "invalid expression not containing '{schema}', with no prefix and suffix",
			expression: "{xxx}",
			schema:     "abc",
			table:      "",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name:       "invalid expressions not containing '{schema}', with prefix",
			expression: "abc{sch}",
			schema:     "def",
			table:      "",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" without prefix or suffix",
			expression: "{schema}_{table}",
			schema:     "hello",
			table:      "world",
			expected:   "hello_world",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" schema/table are converted to lower case letters",
			expression: "{schema}_{table}",
			schema:     "HELLO",
			table:      "WORLD",
			expected:   "HELLO_WORLD",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" the kafka disallowed characters in table are replaced",
			expression: "{schema}_{table}",
			schema:     "hello",
			table:      "!@#$%^&*",
			expected:   "hello_________",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" the kafka disallowed characters in schema are replaced",
			expression: "{schema}_{table}",
			schema:     "()_+.{}",
			table:      "world",
			expected:   "____.___world",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" with both prefix and suffix",
			expression: "ab.-c_{schema}_{table}_de.-f",
			schema:     "hello",
			table:      "WORLD",
			expected:   "ab.-c_hello_WORLD_de.-f",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" with customized middle delimited string",
			expression: "abc_{schema}de._-{table}_fhi",
			schema:     "hello",
			table:      "world",
			expected:   "abc_hellode._-world_fhi",
		},
		{
			name: "valid expression containing '{schema}' and '{table}'," +
				" the kafka disallowed characters in schema and table are replaced",
			expression: "{schema}_{table}",
			schema:     "你好",
			table:      "世界",
			expected:   "_____",
		},
		{
			name:       "invalid expression not containing '{schema}' and '{table}'",
			expression: "{sch}_{tab}",
			schema:     "hello",
			table:      "world",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name: "invalid expression containing '{schema}' and '{table}'," +
				" the middle delimited string must be [A-Za-z0-9\\._\\-]*",
			expression: "{schema}!@#$%^&*()+{}你好{table}",
			schema:     "hello",
			table:      "world",
			wantErr:    "invalid topic expression",
			expected:   "",
		},
		{
			name:       "invalid topic name '.'",
			expression: "{schema}",
			schema:     ".",
			table:      "",
			expected:   "_",
		},
		{
			name:       "invalid topic name '..'",
			expression: "{schema}",
			schema:     "..",
			table:      "",
			expected:   "__",
		},
	}

	for _, tc := range cases {
		topicExpr := Expression(tc.expression)
		err := topicExpr.Validate()
		if tc.wantErr != "" {
			require.Contains(t, err.Error(), tc.wantErr, fmt.Sprintf("case:%s", tc.name))
		} else {
			require.Nil(t, err, fmt.Sprintf("case:%s", tc.name))
			topicName := topicExpr.Substitute(tc.schema, tc.table)
			require.Equal(t, topicName, tc.expected, fmt.Sprintf("case:%s", tc.name))
		}
	}
}

func TestSchemaOptional(t *testing.T) {
	t.Parallel()

	expression := "prefix_{table}"
	topicExpr := Expression(expression)
	err := topicExpr.Validate()
	require.NoError(t, err)

	schemaName := "test"
	tableName := "table1"
	topicName := topicExpr.Substitute(schemaName, tableName)
	require.Equal(t, topicName, "prefix_table1")
}

func TestTableOptional(t *testing.T) {
	t.Parallel()

	expression := "prefix_{schema}"
	topicExpr := Expression(expression)
	err := topicExpr.Validate()
	require.NoError(t, err)

	schemaName := "test"
	tableName := "abc"
	topicName := topicExpr.Substitute(schemaName, tableName)
	require.Equal(t, topicName, "prefix_test")
}

func TestInvalidExpression(t *testing.T) {
	t.Parallel()

	invalidExpr := "%invalid{schema}"
	topicExpr := Expression(invalidExpr)

	err := topicExpr.Validate()
	require.ErrorIs(t, err, errors.ErrKafkaInvalidTopicExpression)
	require.ErrorContains(t, err, invalidExpr)

	err = topicExpr.ValidateForAvro()
	require.ErrorIs(t, err, errors.ErrKafkaInvalidTopicExpression)
	require.ErrorContains(t, err, "Avro")
	require.ErrorContains(t, err, invalidExpr)
}

// cmd: go test -run='^$' -bench '^(BenchmarkSubstitute)$' github.com/pingcap/tiflow/cdc/sink/dispatcher/topic
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/cdc/sink/dispatcher
// cpu: Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
// BenchmarkSubstitute/schema_substitution-40         	  199372	      6477 ns/op
// BenchmarkSubstitute/schema_table_substitution-40   	  110752	     13637 ns/op
func BenchmarkSubstitute(b *testing.B) {
	cases := []struct {
		name       string
		expression string
		schema     string
		table      string
	}{{
		name:       "schema_substitution",
		expression: "abc{schema}def",
		schema:     "hello!@#$%^&*()_+.{}world",
		table:      "",
	}, {
		name:       "schema_table_substitution",
		expression: "{schema}_{table}",
		schema:     "hello!@#$%^&*()_+.{}你好",
		table:      "world!@#$%^&*()_+.{}世界",
	}}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				topicExpr := Expression(tc.expression)
				err := topicExpr.Validate()
				require.Nil(b, err)
				_ = topicExpr.Substitute(tc.schema, tc.table)
			}
		})
	}
}
