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
	"context"

	"github.com/pingcap/check"
)

type ctxValueSuite struct{}

var _ = check.Suite(&ctxValueSuite{})

func (s *ctxValueSuite) TestShouldReturnCaptureID(c *check.C) {
	ctx := PutCaptureAddrInCtx(context.Background(), "ello")
	c.Assert(CaptureAddrFromCtx(ctx), check.Equals, "ello")
}

func (s *ctxValueSuite) TestCaptureIDNotSet(c *check.C) {
	c.Assert(CaptureAddrFromCtx(context.Background()), check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyCaptureAddr, 1321)
	c.Assert(CaptureAddrFromCtx(ctx), check.Equals, "")
}

func (s *ctxValueSuite) TestShouldReturnChangefeedID(c *check.C) {
	ctx := PutChangefeedIDInCtx(context.Background(), "ello")
	c.Assert(ChangefeedIDFromCtx(ctx), check.Equals, "ello")
}

func (s *ctxValueSuite) TestChangefeedIDNotSet(c *check.C) {
	c.Assert(ChangefeedIDFromCtx(context.Background()), check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyChangefeedID, 1321)
	c.Assert(ChangefeedIDFromCtx(ctx), check.Equals, "")
}
