// Copyright 2025 PingCAP, Inc.
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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchFailpointValue(t *testing.T) {
	ddl := "ALTER TABLE t ADD COLUMN c2 int"
	tests := []struct {
		name string
		val  any
		want bool
	}{
		{name: "nil", val: nil, want: true},
		{name: "bool-true", val: true, want: true},
		{name: "bool-false", val: false, want: false},
		{name: "empty-string", val: "", want: true},
		{name: "match-string", val: "c2", want: true},
		{name: "match-string-case", val: "C2", want: true},
		{name: "no-match", val: "d2", want: false},
		{name: "unknown-type", val: 123, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, matchFailpointValue(tc.val, ddl))
		})
	}
}
