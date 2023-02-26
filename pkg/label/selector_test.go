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

package label

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectorMatches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		selector    *Selector
		labels      Set
		shouldMatch bool
	}{
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpEq,
			},
			labels:      map[Key]Value{"tenant": "1", "node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpEq,
			},
			labels:      map[Key]Value{"tenant": "2", "node_type": "2"},
			shouldMatch: false,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpEq,
			},
			labels:      map[Key]Value{"node_type": "2"},
			shouldMatch: false,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpNeq,
			},
			labels:      map[Key]Value{"tenant": "2", "node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpNeq,
			},
			labels:      map[Key]Value{"tenant": "1", "node_type": "2"},
			shouldMatch: false,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1",
				Op:     OpNeq,
			},
			labels:      map[Key]Value{"node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: ".*abc.*",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"tenant": "1abc2", "node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: ".*abc.*",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"tenant": "asdf", "node_type": "2"},
			shouldMatch: false,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "^(abc|def)$",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"tenant": "def", "node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "^(abc|def)$",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"tenant": "abc", "node_type": "2"},
			shouldMatch: true,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "^(abc|def)$",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"tenant": "abcdef", "node_type": "2"},
			shouldMatch: false,
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "^(abc|def)$",
				Op:     OpRegex,
			},
			labels:      map[Key]Value{"node_type": "2"},
			shouldMatch: false,
		},
	}

	for idx, tc := range cases {
		tc := tc
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.shouldMatch, tc.selector.Matches(tc.labels))
		})
	}
}

func TestSelectorRegexLazyCompile(t *testing.T) {
	t.Parallel()

	selector := Selector{
		Key:    "tenant",
		Target: "^(abc|def)$",
		Op:     OpRegex,
	}

	labelSetMatch := map[Key]Value{"tenant": "abc", "node_type": "2"}
	labelSetNotMatch := map[Key]Value{"tenant": "abcdef", "node_type": "2"}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.True(t, selector.Matches(labelSetMatch))
			require.False(t, selector.Matches(labelSetNotMatch))
		}()
	}

	wg.Wait()
}

func TestSelectorRegexIllegal(t *testing.T) {
	t.Parallel()

	selector := Selector{
		Key:    "tenant",
		Target: "(((", // illegal regular expression
		Op:     OpRegex,
	}
	require.False(t, selector.Matches(map[Key]Value{"tenant": "abc", "node_type": "2"}))
}

func TestSelectorValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		selector *Selector
		checkErr func(err error)
	}{
		{
			selector: &Selector{
				Key:    "#@$!@#", // Illegal Key
				Target: "1234",
				Op:     OpEq,
			},
			checkErr: func(err error) {
				require.EqualError(t, err,
					"validate selector key: label string has wrong format: #@$!@#")
			},
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: "1234",
				Op:     Op("invalid"), // invalid op code
			},
			checkErr: func(err error) {
				require.EqualError(t, err, "invalid selector op: key: tenant, op: invalid")
			},
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: ")))", // invalid regex
				Op:     OpRegex,
			},
			checkErr: func(err error) {
				require.ErrorContains(t, err, ")))")
			},
		},
		{
			selector: &Selector{
				Key:    "tenant",
				Target: ".*abc.*",
				Op:     OpRegex,
			},
			checkErr: func(err error) {
				require.NoError(t, err)
			},
		},
	}

	for idx, tc := range cases {
		tc := tc
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			t.Parallel()
			tc.checkErr(tc.selector.Validate())
		})
	}
}
