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
	"testing"

	"github.com/kami-zh/go-capturer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

var defaultConfigFile = "./dm-worker.toml"

func TestAdjustAddr(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.configFromFile(defaultConfigFile))
	require.NoError(t, cfg.adjust())

	// invalid `advertise-addr`
	cfg.AdvertiseAddr = "127.0.0.1"
	require.True(t, terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()))
	cfg.AdvertiseAddr = "0.0.0.0:8262"
	require.True(t, terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()))

	// clear `advertise-addr`, still invalid because no `host` in `worker-addr`.
	cfg.AdvertiseAddr = ""
	require.True(t, terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()))

	// TICASE-956
	cfg.WorkerAddr = "127.0.0.1:8262"
	require.NoError(t, cfg.adjust())
	require.Equal(t, cfg.WorkerAddr, cfg.AdvertiseAddr)
}

func TestPrintSampleConfig(t *testing.T) {
	buf, err := os.ReadFile(defaultConfigFile)
	require.NoError(t, err)

	// test print sample config
	out := capturer.CaptureStdout(func() {
		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		require.Error(t, err)
		require.Regexp(t, flag.ErrHelp.Error(), err.Error())
	})
	require.Equal(t, strings.TrimSpace(string(buf)), strings.TrimSpace(out))
}
