// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
)

var _ = Suite(&testStringSuite{})

type testStringSuite struct{}

func (t *testStringSuite) TestTruncateString(c *C) {
	cases := []struct {
		s      string
		n      int
		expect string
	}{
		{s: "", n: 0, expect: ""},
		{s: "", n: 3, expect: ""},
		{s: "abc", n: 0, expect: "..."},
		{s: "abc", n: 1, expect: "a..."},
		{s: "abc", n: 3, expect: "abc"},
		{s: "abc", n: 9, expect: "abc"},
		{s: "abcdefg", n: 5, expect: "abcde..."},
		{s: "abcdefg", n: 10, expect: "abcdefg"},
	}

	for _, cs := range cases {
		c.Assert(TruncateString(cs.s, cs.n), Equals, cs.expect)
	}
}

func (t *testStringSuite) TestTruncateInterface(c *C) {
	type compose struct {
		b bool
		i int
		s string
	}

	var (
		i1  = compose{b: true, i: 123, s: "abc"}
		i2  = compose{b: true, i: 456, s: "def"}
		i3  = compose{b: false, i: 321, s: "cba"}
		i4  = compose{b: false, i: 654, s: "fed"}
		iis [][]interface{}
	)
	iis = append(iis, []interface{}{i1, i2}, []interface{}{i3, i4})

	cases := []struct {
		v      interface{}
		n      int
		expect string
	}{
		{v: nil, n: -1, expect: "<nil>"},
		{v: nil, n: 0, expect: "..."},
		{v: nil, n: 3, expect: "<ni..."},
		{v: nil, n: 9, expect: "<nil>"},
		{v: 123, n: 0, expect: "..."},
		{v: 123, n: 3, expect: "123"},
		{v: compose{b: true, i: 123, s: "abc"}, n: 3, expect: "{b:..."},
		{v: compose{b: true, i: 123, s: "abc"}, n: 9, expect: "{b:true i..."},
		{v: compose{b: true, i: 123, s: "abc"}, n: 99, expect: "{b:true i:123 s:abc}"},
		{v: []string{"abc", "123"}, n: 3, expect: "[ab..."},
		{v: []string{"abc", "123"}, n: 9, expect: "[abc 123]"},
		{v: []compose{{b: true, i: 123, s: "abc"}, {b: false, i: 456, s: "def"}}, n: 3, expect: "[{b..."},
		{v: []compose{{b: true, i: 123, s: "abc"}, {b: false, i: 456, s: "def"}}, n: 99, expect: "[{b:true i:123 s:abc} {b:false i:456 s:def}]"},
		{v: iis, n: 3, expect: "[[{..."},
		{v: iis, n: 9, expect: "[[{b:true..."},
		{v: iis, n: 99, expect: "[[{b:true i:123 s:abc} {b:true i:456 s:def}] [{b:false i:321 s:cba} {b:false i:654 s:fed}]]"},
	}

	for _, cs := range cases {
		c.Assert(TruncateInterface(cs.v, cs.n), Equals, cs.expect)
	}
}
