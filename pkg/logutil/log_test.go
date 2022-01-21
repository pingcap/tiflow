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

package logutil

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type logSuite struct{}

var _ = check.Suite(&logSuite{})

func (s *logSuite) TestInitLoggerAndSetLogLevel(c *check.C) {
	defer testleak.AfterTest(c)()
	f := filepath.Join(c.MkDir(), "test")
	cfg := &Config{
		Level: "warning",
		File:  f,
	}
	cfg.Adjust()
	err := InitLogger(cfg)
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.WarnLevel)

	// Set a different level.
	err = SetLogLevel("info")
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.InfoLevel)

	// Set the same level.
	err = SetLogLevel("info")
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.InfoLevel)

	// Set an invalid level.
	err = SetLogLevel("badlevel")
	c.Assert(err, check.NotNil)
}

func (s *logSuite) TestZapErrorFilter(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		err       = errors.New("test error")
		testCases = []struct {
			err      error
			filters  []error
			expected zap.Field
		}{
			{nil, []error{}, zap.Error(nil)},
			{err, []error{}, zap.Error(err)},
			{err, []error{context.Canceled}, zap.Error(err)},
			{err, []error{err}, zap.Error(nil)},
			{context.Canceled, []error{context.Canceled}, zap.Error(nil)},
			{errors.Annotate(context.Canceled, "annotate error"), []error{context.Canceled}, zap.Error(nil)},
		}
	)
	for _, tc := range testCases {
		c.Assert(ZapErrorFilter(tc.err, tc.filters...), check.DeepEquals, tc.expected)
	}
}
