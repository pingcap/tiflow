package topic

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
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
			name:       "simple topic ,no {schema}",
			expression: "public", // no {schema}
			wantErr:    "invalid topic expression",
		},
		{
			name:       "simple topic ,no {schema}",
			expression: "_xyz",
			wantErr:    "invalid topic expression",
		}, {
			name:       "simple topic ,no {schema}",
			expression: "123456",
			wantErr:    "invalid topic expression",
		}, {
			name:       "simple topic ,no {schema}",
			expression: "ABCD",
			wantErr:    "invalid topic expression",
		}, {
			name:       "like a full topic ,no {schema}",
			expression: "persistent:public_test-table",
			wantErr:    "invalid topic expression",
		}, {
			name:       "like a full topic ,no {schema}",
			expression: "persistent://",

			wantErr: "invalid topic expression",
		}, {
			name:       "like a full topic ,no {schema}",
			expression: "persistent://public",

			wantErr: "invalid topic expression",
		}, {
			name:       "like a full topic ,no {schema}",
			expression: "persistent://public_test-table",

			wantErr: "invalid topic expression",
		}, {
			name:       "like a full topic ,no {schema}",
			expression: "persistent://public/_test-table",

			wantErr: "invalid topic expression",
		},
		//{
		//	// if the {schema} is a not exist namespace in pulsar server, pulsar client will get an error,
		//	// but cdc can not check it
		//	expression:     "persistent://public/{schema}/_test-table",
		//	expectedResult: false,
		//},
		{
			name:       "like a full topic",
			expression: "persistent_public/test_{schema}_{table}",

			wantErr: "invalid topic expression",
		},
		{
			name:       "like a full topic",
			expression: "persistent://{schema}_{table}",

			wantErr: "invalid topic expression",
		},
		{
			name:       "like a full topic, but more '/' ",
			expression: "persistent://{schema}/{table}/test/name",

			wantErr: "invalid topic expression",
		},
		{
			name:       "like a full topic, but more '/' ",
			expression: "persistent://{schema}/{table}/test/name/admin",

			wantErr: "invalid topic expression",
		},
		{
			name:       "like a full topic, but less '/' ",
			expression: "non-persistent://public/test_{schema}_{table}",

			wantErr: "invalid topic expression",
		},
		{
			name:       "like a full topic, but less '/' ",
			expression: "non-persistent://public/test_{schema}_{table}_123456aaaa",

			wantErr: "invalid topic expression",
		},

		// valid cases
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
			name:       "simple topic",
			expression: "{table}_{schema}",

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

	for _, tc := range testTopicCases {
		topicExpr := Expression(tc.expression)
		err := topicExpr.PulsarValidate()
		if tc.wantErr != "" {
			require.Contains(t, err.Error(), tc.wantErr, fmt.Sprintf("case:%s", tc.name))
		} else {
			require.Nil(t, err, fmt.Sprintf("case:%s", tc.name))
			topicName := topicExpr.Substitute(schema, table)
			require.Equal(t, topicName, tc.expectedTopic, fmt.Sprintf("case:%s", tc.name))
		}
	}

}
