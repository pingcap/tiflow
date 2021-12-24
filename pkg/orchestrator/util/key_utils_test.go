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

package util

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&keyUtilsSuite{})

type keyUtilsSuite struct {
}

func (s *keyUtilsSuite) TestEtcdKey(c *check.C) {
	defer testleak.AfterTest(c)()
	key := NewEtcdKey("/a/b/c")
	c.Assert(key, check.Equals, NewEtcdKeyFromBytes([]byte("/a/b/c")))
	c.Assert(key.String(), check.Equals, "/a/b/c")
	c.Assert(key.Bytes(), check.BytesEquals, []byte("/a/b/c"))
	c.Assert(key.Head().String(), check.Equals, "/a")
	c.Assert(key.Tail().String(), check.Equals, "/b/c")
	c.Assert(key.RemovePrefix(&EtcdPrefix{"/a/b"}).String(), check.Equals, "/c")
	c.Assert(key.AsRelKey().String(), check.Equals, "/a/b/c")
}

func (s *keyUtilsSuite) TestEtcdRelKey(c *check.C) {
	defer testleak.AfterTest(c)()
	key := NewEtcdRelKey("/a/b/c")
	c.Assert(key, check.Equals, NewEtcdRelKeyFromBytes([]byte("/a/b/c")))
	c.Assert(key.String(), check.Equals, "/a/b/c")
	c.Assert(key.Bytes(), check.BytesEquals, []byte("/a/b/c"))
	c.Assert(key.Head().String(), check.Equals, "/a")
	c.Assert(key.Tail().String(), check.Equals, "/b/c")
	c.Assert(key.RemovePrefix(&EtcdRelPrefix{EtcdPrefix{"/a/b"}}).String(), check.Equals, "/c")
	c.Assert(key.AsPrefix().String(), check.Equals, "/a/b/c")
}

func (s *keyUtilsSuite) TestEtcdPrefix(c *check.C) {
	defer testleak.AfterTest(c)()
	prefix := NewEtcdPrefix("/aa/bb/cc")
	c.Assert(prefix, check.Equals, NewEtcdPrefixFromBytes([]byte("/aa/bb/cc")))
	c.Assert(prefix.String(), check.Equals, "/aa/bb/cc")
	c.Assert(prefix.Bytes(), check.BytesEquals, []byte("/aa/bb/cc"))
	c.Assert(prefix.Tail().String(), check.Equals, "/bb/cc")
	c.Assert(prefix.Head().String(), check.Equals, "/aa")
	c.Assert(prefix.FullKey(NewEtcdRelKey("/dd")).String(), check.Equals, "/aa/bb/cc/dd")
}

func (s *keyUtilsSuite) TestEtcdRelPrefix(c *check.C) {
	defer testleak.AfterTest(c)()
	prefix := NewEtcdRelPrefix("/aa/bb/cc")
	c.Assert(prefix, check.Equals, NewEtcdRelPrefixFromBytes([]byte("/aa/bb/cc")))
	c.Assert(prefix.String(), check.Equals, "/aa/bb/cc")
	c.Assert(prefix.Bytes(), check.BytesEquals, []byte("/aa/bb/cc"))
	c.Assert(prefix.Tail().String(), check.Equals, "/bb/cc")
	c.Assert(prefix.Head().String(), check.Equals, "/aa")
}

func (s *keyUtilsSuite) TestNormalizePrefix(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(NormalizePrefix("aaa"), check.Equals, NewEtcdPrefix("/aaa"))
	c.Assert(NormalizePrefix("aaa/"), check.Equals, NewEtcdPrefix("/aaa"))
	c.Assert(NormalizePrefix("/aaa"), check.Equals, NewEtcdPrefix("/aaa"))
	c.Assert(NormalizePrefix("/aaa/"), check.Equals, NewEtcdPrefix("/aaa"))
}

func (s *keyUtilsSuite) TestCornerCases(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(func() { NewEtcdPrefix("").Head() }, check.Panics, "Empty EtcdPrefix")
	c.Assert(func() { NewEtcdPrefix("").Tail() }, check.Panics, "Empty EtcdPrefix")
	c.Assert(NewEtcdPrefix("aaa").Head(), check.Equals, NewEtcdPrefix(""))
	c.Assert(NewEtcdPrefix("aaa").Tail(), check.Equals, NewEtcdRelPrefix("aaa"))

	c.Assert(func() { NewEtcdKey("").Head() }, check.Panics, "Empty EtcdKey")
	c.Assert(func() { NewEtcdKey("").Tail() }, check.Panics, "Empty EtcdKey")
	c.Assert(NewEtcdKey("aaa").Head(), check.Equals, NewEtcdPrefix(""))
	c.Assert(NewEtcdKey("aaa").Tail(), check.Equals, NewEtcdRelKey("aaa"))
}
