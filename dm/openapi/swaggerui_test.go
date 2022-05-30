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

package openapi

import (
	"strings"
	"testing"

	"github.com/pingcap/check"
)

var _ = check.Suite(&swaggerUISuite{})

type swaggerUISuite struct{}

func TestNewSwaggerDocUI(t *testing.T) {
	check.TestingT(t)
}

func (t *swaggerUISuite) TestGetSwaggerHTML(c *check.C) {
	html, err := GetSwaggerHTML(NewSwaggerConfig("/api/v1/docs/dm.json", ""))
	c.Assert(err, check.IsNil)
	c.Assert(strings.Contains(html, "<title>API documentation</title>"), check.Equals, true)
}
