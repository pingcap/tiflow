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

package config

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
)

func TestConfig(t *testing.T) {
	check.TestingT(t)
}

type testConfig struct{}

var _ = check.Suite(&testConfig{})

func (t *testConfig) TestConverterWithSourceAndOpenAPISource(c *check.C) {
	sourceCfg1, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, check.IsNil)

	// 1. test user create source from dmctl, after convert to openapi.Source then convert back to source config
	sourceCfg2 := OpenAPISourceToSourceCfg(SourceCfgToOpenAPISource(sourceCfg1))

	// we need set ServerID and MaxAllowedPacket manually, because user don't need to config those field in openapi
	sourceCfg2.ServerID = sourceCfg1.ServerID
	sourceCfg2.From.MaxAllowedPacket = sourceCfg1.From.MaxAllowedPacket

	// we only need to make sure the source config that user can see is the same as the source config that user create
	c.Assert(sourceCfg1.String(), check.Equals, sourceCfg2.String())

	// 2. test user create source from openapi, after convert to source config then convert back to openapi.Source
	openapiSource1, err := fixtures.GenOpenAPISourceForTest()
	c.Assert(err, check.IsNil)
	openapiSource2 := SourceCfgToOpenAPISource(OpenAPISourceToSourceCfg(openapiSource1))
	openapiSource2.Password = openapiSource1.Password // we set passwd to "******" for privacy
	c.Assert(openapiSource1, check.DeepEquals, openapiSource2)
}
