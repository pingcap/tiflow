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

package util

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type utilsSuite struct{}

var _ = check.Suite(&utilsSuite{})

func (s *utilsSuite) TestVerifyPdEndpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	// empty URL.
	url := ""
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// invalid URL.
	url = "\n hi"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*invalid control character in URL.*")

	// http URL without host.
	url = "http://"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https URL without host.
	url = "https://"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// postgres scheme.
	url = "postgres://postgres@localhost/cargo_registry"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https scheme without TLS.
	url = "https://aa"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint scheme is https, please provide certificate.*")

	// http scheme with TLS.
	url = "http://aa"
	c.Assert(VerifyPdEndpoint(url, true), check.ErrorMatches, ".*PD endpoint scheme should be https.*")

	// valid http URL.
	c.Assert(VerifyPdEndpoint("http://aa", false), check.IsNil)

	// valid https URL with TLS.
	c.Assert(VerifyPdEndpoint("https://aa", true), check.IsNil)
}
