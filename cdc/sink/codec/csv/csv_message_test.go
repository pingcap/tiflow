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

package csv

import (
	"strings"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFormatWithQuotes(t *testing.T) {
	csvConfig := &config.CSVConfig{
		Quote: "\"",
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "string does not contain quote mark",
			input:    "a,b,c",
			expected: `"a,b,c"`,
		},
		{
			name:     "string contains quote mark",
			input:    `"a,b,c`,
			expected: `"""a,b,c"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
	}
	for _, tc := range testCases {
		csvMessage := newCSVMessage(csvConfig)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithQuotes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String(), tc.name)
	}
}

func TestFormatWithEscape(t *testing.T) {
	testCases := []struct {
		name      string
		csvConfig *config.CSVConfig
		input     string
		expected  string
	}{
		{
			name:      "string does not contain CR/LF/backslash/delimiter",
			csvConfig: &config.CSVConfig{Delimiter: ","},
			input:     "abcdef",
			expected:  "abcdef",
		},
		{
			name:      "string contains CRLF",
			csvConfig: &config.CSVConfig{Delimiter: ","},
			input:     "abc\r\ndef",
			expected:  "abc\\r\\ndef",
		},
		{
			name:      "string contains backslash",
			csvConfig: &config.CSVConfig{Delimiter: ","},
			input:     `abc\def`,
			expected:  `abc\\def`,
		},
		{
			name:      "string contains a single character delimiter",
			csvConfig: &config.CSVConfig{Delimiter: ","},
			input:     "abc,def",
			expected:  `abc\,def`,
		},
		{
			name:      "string contains multi-character delimiter",
			csvConfig: &config.CSVConfig{Delimiter: "***"},
			input:     "abc***def",
			expected:  `abc\*\*\*def`,
		},
		{
			name:      "string contains CR, LF, backslash and delimiter",
			csvConfig: &config.CSVConfig{Delimiter: "?"},
			input:     `abc\def?ghi\r\n`,
			expected:  `abc\\def\?ghi\\r\\n`,
		},
	}

	for _, tc := range testCases {
		csvMessage := newCSVMessage(tc.csvConfig)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithEscapes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String())
	}
}

func TestCSVMessageEncode(t *testing.T) {
	type fields struct {
		csvConfig  *config.CSVConfig
		opType     string
		tableName  string
		schemaName string
		commitTs   uint64
		columns    []any
	}
	testCases := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "csv encode with typical configurations",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     insertOperation,
				tableName:  "table1",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{123, "hello,world"},
			},
			want: []byte("\"I\",\"table1\",\"test\",435661838416609281,123,\"hello,world\"\n"),
		},
		{
			name: "csv encode values containig single-character delimter string, without quote mark",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     updateOperation,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a!b!c", "def"},
			},
			want: []byte(`U!table2!test!435661838416609281!a\!b\!c!def` + "\n"),
		},
		{
			name: "csv encode values containig single-character delimter string, with quote mark",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     updateOperation,
				tableName:  "table3",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a,b,c", "def", "2022-08-31 17:07:00"},
			},
			want: []byte(`"U","table3","test",435661838416609281,"a,b,c","def","2022-08-31 17:07:00"` + "\n"),
		},
		{
			name: "csv encode values containing multi-character delimiter string, without quote mark",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       "[*]",
					Quote:           "",
					Terminator:      "\r\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     deleteOperation,
				tableName:  "table4",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def"},
			},
			want: []byte(`D[*]table4[*]test[*]a\[\*\]b\[\*\]c[*]def` + "\r\n"),
		},
		{
			name: "csv encode with values containing multi-character delimiter string, with quote mark",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       "[*]",
					Quote:           "'",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     insertOperation,
				tableName:  "table5",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def", nil, 12345.678},
			},
			want: []byte(`'I'[*]'table5'[*]'test'[*]'a[*]b[*]c'[*]'def'[*]\N[*]12345.678` + "\n"),
		},
		{
			name: "csv encode with values containing backslash and CRLF, without quote mark",
			fields: fields{
				csvConfig: &config.CSVConfig{
					Delimiter:       ",",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     updateOperation,
				tableName:  "table6",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a\\b\\c", "def\n"},
			},
			want: []byte(`U,table6,test,435661838416609281,a\\b\\c,def\n` + "\n"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &csvMessage{
				csvConfig:  tc.fields.csvConfig,
				opType:     tc.fields.opType,
				tableName:  tc.fields.tableName,
				schemaName: tc.fields.schemaName,
				commitTs:   tc.fields.commitTs,
				columns:    tc.fields.columns,
				newRecord:  true,
			}
			require.Equal(t, tc.want, c.encode())
			// fmt.Println(string(c.encode()))
		})
	}
}
