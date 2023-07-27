// Copyright 2023 PingCAP, Inc.
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

	"github.com/stretchr/testify/require"
)

func TestPulsarValidate(t *testing.T) {
	t.Parallel()
	schema := "schema"
	table := "table"
	testTopicCases := []struct {
		name          string // test case name
		expression    string
		wantErr       string
		expectedTopic string
	}{
		// invalid cases
		{
			name:       "like a full topic ,no {schema}",
			expression: "persistent://",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic",
			expression: "persistent://{schema}",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic ,no {schema}",
			expression: "persistent://public",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic ,no {schema}",
			expression: "persistent://public_test-table",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic ,need '/' after 'test-table'",
			expression: "persistent://public/_test-table",
			wantErr:    "invalid topic expression",
		},
		//{
		//	// if the {schema} is a not exist namespace in pulsar server, pulsar client will get an error,
		//	// but cdc can not check it
		//	expression:     "persistent://public/{schema}/_test-table",
		//	expectedResult: false,
		//},
		{
			name:       "like a full topic",
			expression: "persistent_public/test__{table}",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic",
			expression: "persistent://{schema}_{table}",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic, but more '/' ",
			expression: "persistent://{schema}/{table}/test/name",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic, but more '/' ",
			expression: "persistent://test/{table}/test/name/admin",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic, but less '/' ",
			expression: "non-persistent://public/test_{schema}_{table}",
			wantErr:    "invalid topic expression",
		},
		{
			name:       "like a full topic, but less '/' ",
			expression: "non-persistent://public/test {table}_123456aaaa",
			wantErr:    "invalid topic expression",
		},
		// valid cases
		{
			name:          "simple topic ,no {schema}",
			expression:    "public", // no {schema}
			expectedTopic: "public",
		},
		{
			name:          "simple topic ,no {schema}",
			expression:    "_xyz",
			expectedTopic: "_xyz",
		},
		{
			name:          "simple topic ,no {schema}",
			expression:    "123456",
			expectedTopic: "123456",
		},
		{
			name:          "simple topic ,no {schema}",
			expression:    "ABCD",
			expectedTopic: "ABCD",
		},
		{
			name:          "like a full topic ,no {schema}",
			expression:    "persistent:public_test-table",
			expectedTopic: "persistent:public_test-table",
		},
		{
			name:          "simple topic",
			expression:    "{schema}",
			expectedTopic: "schema",
		},
		{
			name:          "simple topic",
			expression:    "AZ_{schema}",
			expectedTopic: "AZ_schema",
		},
		{
			name:          "simple topic",
			expression:    "{table}_{schema}",
			expectedTopic: "table_schema",
		},
		{
			name:          "simple topic",
			expression:    "123_{schema}_non-persistenttest__{table})",
			expectedTopic: "123_schema_non-persistenttest__table)",
		},
		{
			name:          "simple topic",
			expression:    "persistent_public_test_{schema}_{table}",
			expectedTopic: "persistent_public_test_schema_table",
		},
		{
			name:          "simple topic",
			expression:    "persistent{schema}_{table}",
			expectedTopic: "persistentschema_table",
		},
		{
			name:          "full topic",
			expression:    "persistent://public/default/{schema}_{table}",
			expectedTopic: "persistent://public/default/schema_table",
		},
		{
			name:          "full topic",
			expression:    "persistent://public/default/2342-{schema}_abc234",
			expectedTopic: "persistent://public/default/2342-schema_abc234",
		},
		{
			name:          "full topic",
			expression:    "persistent://{schema}/{schema}/2342-{schema}_abc234",
			expectedTopic: "persistent://schema/schema/2342-schema_abc234",
		},
		{
			name:          "full topic",
			expression:    "persistent://{schema}/dev/2342-{schema}_abc234",
			expectedTopic: "persistent://schema/dev/2342-schema_abc234",
		},
		{
			name:          "full topic",
			expression:    "non-persistent://public/default/test_{schema}_{table}_back_up",
			expectedTopic: "non-persistent://public/default/test_schema_table_back_up",
		},
		{
			name:          "full topic",
			expression:    "123_{schema}_non-persistenttest__{table}",
			expectedTopic: "123_schema_non-persistenttest__table",
		},
	}

	for i, tc := range testTopicCases {
		topicExpr := Expression(tc.expression)
		err := topicExpr.PulsarValidate()
		t.Logf("case %d: %s", i, tc.name)
		if err != nil {
			require.Contains(t, err.Error(), tc.wantErr, fmt.Sprintf("case:%s", tc.name))
		} else {
			require.Nil(t, err, fmt.Sprintf("case:%s", tc.name))
			topicName := topicExpr.Substitute(schema, table)
			require.Equal(t, topicName, tc.expectedTopic, fmt.Sprintf("case:%s", tc.name))
		}
	}
}
