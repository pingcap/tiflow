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

package util

import (
	"context"

	"github.com/pingcap/check"
)

type captureIDSuite struct{}

var _ = check.Suite(&captureIDSuite{})

func (s *captureIDSuite) TestShouldReturnCaptureID(c *check.C) {
	ctx := PutCaptureIDInCtx(context.Background(), "ello")
	c.Assert(CaptureIDFromCtx(ctx), check.Equals, "ello")
}

func (s *captureIDSuite) TestShouldReturnEmptyStr(c *check.C) {
	c.Assert(CaptureIDFromCtx(context.Background()), check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyCaptureID, 1321)
	c.Assert(CaptureIDFromCtx(ctx), check.Equals, "")
}
