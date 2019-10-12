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
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type logSuite struct{}

var _ = Suite(&logSuite{})

func (s *logSuite) TestInitLogger(c *C) {
	f := path.Join(c.MkDir(), "test")
	cfg := &Config{
		Level: "warning",
		File:  f,
	}
	cfg.Adjust()
	err := InitLogger(cfg)
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel(), Equals, zapcore.WarnLevel)
}
