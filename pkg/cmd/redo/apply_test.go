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

package redo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestComplete checks the default sink settings and required safe mode.
func TestComplete(t *testing.T) {
	cmd := &cobra.Command{
		Use: "test",
	}
	o := newapplyRedoOptions()
	o.sinkURI = "mysql://root@127.0.0.1:3306?safe-mode=false"
	err := o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?safe-mode=true", o.sinkURI)

	o.sinkURI = "mysql://root@127.0.0.1:3306"
	err = o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?safe-mode=true", o.sinkURI)

	o.sinkURI = "mysql://root@127.0.0.1:3306?time-zone=UTC&safe-mode=true"
	err = o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?time-zone=UTC&safe-mode=true", o.sinkURI)
	require.Equal(t, config.GetDefaultReplicaConfig(), o.cfg)
}

// TestCompleteWithSinkConfig checks redo sink configuration loading and validation.
func TestCompleteWithSinkConfig(t *testing.T) {
	for _, tc := range []struct {
		name, content, wantError string
	}{
		{
			name: "ordering and backend settings",
			content: `case-sensitive = false
[sink]
transaction-atomicity = "none"
safe-mode = false
[sink.mysql-config]
worker-count = 4
max-txn-row = 64
txn-order-tables = ["TEST.ORDERED_*"]`,
		},
		{"invalid ordering rule", "[sink.mysql-config]\ntxn-order-tables = [\"invalid\"]", "txn-order-tables"},
		{"unknown setting", "[sink.mysql-config]\ntxn-order-table = []", "unknown configuration options"},
		{"invalid TOML", "[sink", "expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "redo.toml")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))
			o := newapplyRedoOptions()
			cmd := &cobra.Command{Use: "test"}
			o.addFlags(cmd)
			require.NoError(t, cmd.ParseFlags([]string{"--config", path, "--sink-uri", "mysql://root@127.0.0.1:3306?safe-mode=false"}))
			err := o.complete(cmd)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 4, *o.cfg.Sink.MySQLConfig.WorkerCount)
			require.Equal(t, 64, *o.cfg.Sink.MySQLConfig.MaxTxnRow)
			require.Equal(t, []string{"TEST.ORDERED_*"}, o.cfg.Sink.MySQLConfig.TxnOrderTables)
			require.Equal(t, config.AtomicityLevel("none"), *o.cfg.Sink.TxnAtomicity)
			orderFilter, err := o.cfg.Sink.GetTxnOrderFilter(o.cfg.CaseSensitive)
			require.NoError(t, err)
			require.True(t, orderFilter.MatchTable("test", "ordered_t"))
			require.Contains(t, o.sinkURI, "safe-mode=true")
		})
	}

	o := newapplyRedoOptions()
	o.sinkURI = "mysql://root@127.0.0.1:3306"
	o.configFile = filepath.Join(t.TempDir(), "missing.toml")
	require.ErrorContains(t, o.complete(&cobra.Command{}), "no such file")
}
