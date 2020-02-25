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

type ctxValueSuite struct{}

var _ = check.Suite(&ctxValueSuite{})

func (s *ctxValueSuite) TestGetValueFromCtx(c *check.C) {
	captureID := "test-capture"
	ctx := PutValueInCtx(context.Background(), CtxKeyCaptureID, captureID)
	c.Assert(GetValueFromCtx(ctx, CtxKeyCaptureID), check.Equals, captureID)
}

func (s *ctxValueSuite) TestGetValueInvalid(c *check.C) {
	c.Assert(GetValueFromCtx(context.Background(), CtxKeyCaptureID), check.Equals, "")
	ctx := context.WithValue(context.Background(), CtxKeyCaptureID, 1321)
	c.Assert(GetValueFromCtx(ctx, CtxKeyCaptureID), check.Equals, "")

}
