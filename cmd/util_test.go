// Copyright 2021 PingCAP, Inc.
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

package cmd

import (
	"os"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/spf13/cobra"
)

type utilsSuite struct{}

var _ = check.Suite(&utilsSuite{})

func (s *utilsSuite) TestProxyFields(c *check.C) {
	defer testleak.AfterTest(c)()
	revIndex := map[string]int{
		"http_proxy":  0,
		"https_proxy": 1,
		"no_proxy":    2,
	}
	envs := [...]string{"http_proxy", "https_proxy", "no_proxy"}
	envPreset := [...]string{"http://127.0.0.1:8080", "https://127.0.0.1:8443", "localhost,127.0.0.1"}

	// Exhaust all combinations of those environment variables' selection.
	// Each bit of the mask decided whether this index of `envs` would be set.
	for mask := 0; mask <= 0b111; mask++ {
		for _, env := range envs {
			c.Assert(os.Unsetenv(env), check.IsNil)
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				c.Assert(os.Setenv(envs[i], envPreset[i]), check.IsNil)
			}
		}

		for _, field := range proxyFields() {
			idx, ok := revIndex[field.Key]
			c.Assert(ok, check.IsTrue)
			c.Assert((1<<idx)&mask, check.Not(check.Equals), 0)
			c.Assert(field.String, check.Equals, envPreset[idx])
		}
	}
}

func (s *utilsSuite) TestVerifyPdEndpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	// empty URL.
	url := ""
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// invalid URL.
	url = "\r hi"
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*invalid control character in URL.*")

	// http URL without host.
	url = "http://"
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https URL without host.
	url = "https://"
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// postgres scheme.
	url = "postgres://postgres@localhost/cargo_registry"
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https scheme without TLS.
	url = "https://aa"
	c.Assert(verifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint scheme is https, please provide certificate.*")

	// http scheme with TLS.
	url = "http://aa"
	c.Assert(verifyPdEndpoint(url, true), check.ErrorMatches, ".*PD endpoint scheme should be https.*")

	// valid http URL.
	c.Assert(verifyPdEndpoint("http://aa", false), check.IsNil)

	// valid https URL with TLS.
	c.Assert(verifyPdEndpoint("https://aa", true), check.IsNil)
}

func (s *utilsSuite) TestNeedVerifyCmd(c *check.C) {
	defer testleak.AfterTest(c)()

	// Test command tree:
	//      root
	//   l        r
	// ll       rr  rl
	root := &cobra.Command{
		Use: "root",
	}
	l := &cobra.Command{
		Use: "l",
	}
	r := &cobra.Command{
		Use: "r",
	}
	root.AddCommand(l, r)

	ll := &cobra.Command{
		Use: "ll",
	}
	l.AddCommand(ll)
	rl := &cobra.Command{
		Use: "rl",
	}
	rr := &cobra.Command{
		Use: "rr",
	}
	r.AddCommand(rl, rr)

	c.Assert(needVerifyVersion(rl, []string{"root"}), check.IsTrue)
	c.Assert(needVerifyVersion(rl, []string{"r"}), check.IsTrue)
	c.Assert(needVerifyVersion(rl, []string{"rl"}), check.IsTrue)
	c.Assert(needVerifyVersion(rl, []string{"rr"}), check.IsFalse)
	c.Assert(needVerifyVersion(rl, []string{"l"}), check.IsFalse)
	c.Assert(needVerifyVersion(rl, []string{"ll"}), check.IsFalse)
	c.Assert(needVerifyVersion(root, []string{"rl"}), check.IsFalse)
}
