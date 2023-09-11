// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"testing"
)

func TestAreStringSlicesEquivalent(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{
			name: "equal slices",
			a:    []string{"foo", "bar", "baz"},
			b:    []string{"baz", "foo", "bar"},
			want: true,
		},
		{
			name: "different lengths",
			a:    []string{"foo", "bar", "baz"},
			b:    []string{"foo", "bar"},
			want: false,
		},
		{
			name: "different elements",
			a:    []string{"foo", "bar", "baz"},
			b:    []string{"qux", "quux", "corge"},
			want: false,
		},
		{
			name: "nil elements",
			a:    []string{},
			b:    []string{},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AreStringSlicesEquivalent(tt.a, tt.b); got != tt.want {
				t.Errorf("AreStringSlicesEquivalent() = %v, want %v", got, tt.want)
			}
		})
	}
}

// END: j3d8f4b2j2p9
