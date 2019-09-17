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

package flags

import (
	"flag"
	"os"
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFlagSuite{})

type testFlagSuite struct{}

func (s *testFlagSuite) TestSetFlagsFromEnv(c *C) {
	fs := flag.NewFlagSet("test1", flag.ExitOnError)
	fs.String("f1", "", "")
	fs.String("f2", "", "")
	fs.String("f3", "", "")
	err := fs.Parse([]string{})
	c.Assert(err, IsNil)

	os.Clearenv()
	// 1. flag is set with env vars
	os.Setenv("TEST_F1", "abc")
	// 2. flag is set by command-line args
	mustSuccess(c, fs.Set("f2", "xyz"))
	// 3. command-line flags take precedence over env vars
	os.Setenv("TEST_F3", "123")
	mustSuccess(c, fs.Set("f3", "789"))

	// before
	for fl, expected := range map[string]string{
		"f1": "",
		"f2": "xyz",
		"f3": "789",
	} {
		c.Assert(fs.Lookup(fl).Value.String(), Equals, expected)
	}

	mustSuccess(c, SetFlagsFromEnv("TEST", fs))

	// after
	for fl, expected := range map[string]string{
		"f1": "abc",
		"f2": "xyz",
		"f3": "789",
	} {
		c.Assert(fs.Lookup(fl).Value.String(), Equals, expected)
	}
}

func (s *testFlagSuite) TestSetFlagsFromEnvMore(c *C) {
	fs := flag.NewFlagSet("test2", flag.ExitOnError)
	fs.String("str", "", "")
	fs.Int("int", 0, "")
	fs.Bool("bool", false, "")
	fs.String("a-hyphen", "", "")
	fs.String("lowercase", "", "")
	err := fs.Parse([]string{})
	c.Assert(err, IsNil)

	os.Clearenv()
	os.Setenv("TEST_STR", "ijk")
	os.Setenv("TEST_INT", "654")
	os.Setenv("TEST_BOOL", "1")
	os.Setenv("TEST_A_HYPHEN", "foo")
	os.Setenv("TEST_lowertest", "bar")

	mustSuccess(c, SetFlagsFromEnv("TEST", fs))

	for fl, expected := range map[string]string{
		"str":       "ijk",
		"int":       "654",
		"bool":      "true",
		"a-hyphen":  "foo",
		"lowercase": "",
	} {
		c.Assert(fs.Lookup(fl).Value.String(), Equals, expected)
	}
}

func (s *testFlagSuite) TestSetFlagsFromEnvBad(c *C) {
	fs := flag.NewFlagSet("test3", flag.ExitOnError)
	fs.Int("num", 0, "")
	err := fs.Parse([]string{})
	c.Assert(err, IsNil)

	os.Clearenv()
	os.Setenv("TEST_NUM", "abc123")

	mustFail(c, SetFlagsFromEnv("TEST", fs))
}

func (s *testFlagSuite) TestURLValue(c *C) {
	urls := "http://192.168.1.1:1234,http://127.0.0.1:1234,http://www.pingcap.net:1234"
	urlv, err := NewURLsValue(urls)
	c.Assert(err, IsNil)
	c.Assert(urlv.String(), Equals, "http://127.0.0.1:1234,http://192.168.1.1:1234,http://www.pingcap.net:1234")
	c.Assert(urlv.HostString(), Equals, "127.0.0.1:1234,192.168.1.1:1234,www.pingcap.net:1234")
	c.Assert(urlv.StringSlice()[0], Equals, "http://127.0.0.1:1234")
	c.Assert(urlv.StringSlice()[1], Equals, "http://192.168.1.1:1234")
	c.Assert(urlv.StringSlice()[2], Equals, "http://www.pingcap.net:1234")
}

func mustSuccess(c *C, err error) {
	c.Assert(err, IsNil)
}

func mustFail(c *C, err error) {
	c.Assert(err, NotNil)
}
