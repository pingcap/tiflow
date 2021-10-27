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

package worker

import (
	"flag"
	"os"
	"strings"

	"github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	defaultConfigFile = "./dm-worker.toml"
	_                 = check.Suite(&testConfigSuite{})
)

type testConfigSuite struct{}

func (t *testConfigSuite) TestAdjustAddr(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.configFromFile(defaultConfigFile), check.IsNil)
	c.Assert(cfg.adjust(), check.IsNil)

	// invalid `advertise-addr`
	cfg.AdvertiseAddr = "127.0.0.1"
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)
	cfg.AdvertiseAddr = "0.0.0.0:8262"
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)

	// clear `advertise-addr`, still invalid because no `host` in `worker-addr`.
	cfg.AdvertiseAddr = ""
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)

	// TICASE-956
	cfg.WorkerAddr = "127.0.0.1:8262"
	c.Assert(cfg.adjust(), check.IsNil)
	c.Assert(cfg.AdvertiseAddr, check.Equals, cfg.WorkerAddr)
}

func (t *testConfigSuite) TestPrintSampleConfig(c *check.C) {
	buf, err := os.ReadFile(defaultConfigFile)
	c.Assert(err, check.IsNil)

	// test print sample config
	out := capturer.CaptureStdout(func() {
		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		c.Assert(err, check.ErrorMatches, flag.ErrHelp.Error())
	})
	c.Assert(strings.TrimSpace(out), check.Equals, strings.TrimSpace(string(buf)))
}
