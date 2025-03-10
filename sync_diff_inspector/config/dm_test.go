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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

func testHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, `{"result":true,"cfgs":["import-mode = \"logical\"\nis-sharding = true\nshard-mode = \"pessimistic\"\nonline-ddl-scheme = \"\"\ncase-sensitive = false\nname = \"test\"\nmode = \"all\"\nsource-id = \"mysql-replica-01\"\nserver-id = 0\nflavor = \"\"\nmeta-schema = \"dm_meta\"\nheartbeat-update-interval = 1\nheartbeat-report-interval = 10\nenable-heartbeat = false\ntimezone = \"Asia/Shanghai\"\nrelay-dir = \"\"\nuse-relay = false\nfilter-rules = []\nmydumper-path = \"./bin/mydumper\"\nthreads = 4\nchunk-filesize = \"64\"\nstatement-size = 0\nrows = 0\nwhere = \"\"\nskip-tz-utc = true\nextra-args = \"\"\npool-size = 16\ndir = \"./dumped_data.test\"\nmeta-file = \"\"\nworker-count = 16\nbatch = 100\nqueue-size = 1024\ncheckpoint-flush-interval = 30\nmax-retry = 0\nauto-fix-gtid = false\nenable-gtid = false\ndisable-detect = false\nsafe-mode = false\nenable-ansi-quotes = false\nlog-level = \"\"\nlog-file = \"\"\nlog-format = \"\"\nlog-rotate = \"\"\npprof-addr = \"\"\nstatus-addr = \"\"\nclean-dump-file = true\n\n[from]\n  host = \"127.0.0.1\"\n  port = 3306\n  user = \"root\"\n  password = \"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\"\n  max-allowed-packet = 67108864\n\n[to]\n  host = \"127.0.0.1\"\n  port = 4000\n  user = \"root\"\n  password = \"\"\n  max-allowed-packet = 67108864\n\n[[route-rules]]\n  schema-pattern = \"sharding*\"\n  table-pattern = \"t*\"\n  target-schema = \"db_target\"\n  target-table = \"t_target\"\n\n[[route-rules]]\n  schema-pattern = \"sharding*\"\n  table-pattern = \"\"\n  target-schema = \"db_target\"\n  target-table = \"\"\n\n[block-allow-list]\n  do-dbs = [\"~^sharding[\\\\d]+\"]\n\n  [[block-allow-list.do-tables]]\n    db-name = \"~^sharding[\\\\d]+\"\n    tbl-name = \"~^t[\\\\d]+\"\n","is-sharding = true\nshard-mode = \"pessimistic\"\nonline-ddl-scheme = \"\"\ncase-sensitive = false\nname = \"test\"\nmode = \"all\"\nsource-id = \"mysql-replica-02\"\nserver-id = 0\nflavor = \"\"\nmeta-schema = \"dm_meta\"\nheartbeat-update-interval = 1\nheartbeat-report-interval = 10\nenable-heartbeat = false\ntimezone = \"Asia/Shanghai\"\nrelay-dir = \"\"\nuse-relay = false\nfilter-rules = []\nmydumper-path = \"./bin/mydumper\"\nthreads = 4\nchunk-filesize = \"64\"\nstatement-size = 0\nrows = 0\nwhere = \"\"\nskip-tz-utc = true\nextra-args = \"\"\npool-size = 16\ndir = \"./dumped_data.test\"\nmeta-file = \"\"\nworker-count = 16\nbatch = 100\nqueue-size = 1024\ncheckpoint-flush-interval = 30\nmax-retry = 0\nauto-fix-gtid = false\nenable-gtid = false\ndisable-detect = false\nsafe-mode = false\nenable-ansi-quotes = false\nlog-level = \"\"\nlog-file = \"\"\nlog-format = \"\"\nlog-rotate = \"\"\npprof-addr = \"\"\nstatus-addr = \"\"\nclean-dump-file = true\n\n[from]\n  host = \"127.0.0.1\"\n  port = 3307\n  user = \"root\"\n  password = \"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\"\n  max-allowed-packet = 67108864\n\n[to]\n  host = \"127.0.0.1\"\n  port = 4000\n  user = \"root\"\n  password = \"\"\n  max-allowed-packet = 67108864\n\n[[route-rules]]\n  schema-pattern = \"sharding*\"\n  table-pattern = \"t*\"\n  target-schema = \"db_target\"\n  target-table = \"t_target\"\n\n[[route-rules]]\n  schema-pattern = \"sharding*\"\n  table-pattern = \"\"\n  target-schema = \"db_target\"\n  target-table = \"\"\n\n[block-allow-list]\n  do-dbs = [\"~^sharding[\\\\d]+\"]\n\n  [[block-allow-list.do-tables]]\n    db-name = \"~^sharding[\\\\d]+\"\n    tbl-name = \"~^t[\\\\d]+\"\n"]}`)
}

func equal(a *DataSource, b *DataSource) bool {
	return a.Host == b.Host && a.Port == b.Port && a.Password == b.Password && a.User == b.User
}

func TestGetDMTaskCfg(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(testHandler))
	defer mockServer.Close()

	dmTaskCfg, err := getDMTaskCfg(mockServer.URL, "test")
	require.NoError(t, err)
	require.Equal(t, len(dmTaskCfg), 2)
	require.Equal(t, dmTaskCfg[0].SourceID, "mysql-replica-01")
	require.Equal(t, dmTaskCfg[1].SourceID, "mysql-replica-02")

	cfg := NewConfig()
	cfg.DMAddr = mockServer.URL
	cfg.DMTask = "test"
	err = cfg.adjustConfigByDMSubTasks()
	require.NoError(t, err)

	// after adjust config, will generate source tables for target table
	require.Equal(t, len(cfg.DataSources), 3)
	require.True(t, equal(cfg.DataSources["target"], &DataSource{
		Host:     dmTaskCfg[0].To.Host,
		Port:     dmTaskCfg[0].To.Port,
		Password: utils.SecretString(dmTaskCfg[0].To.Password),
		User:     dmTaskCfg[0].To.User,
	}))

	require.True(t, equal(cfg.DataSources["mysql-replica-01"], &DataSource{
		Host:     dmTaskCfg[0].From.Host,
		Port:     dmTaskCfg[0].From.Port,
		Password: utils.SecretString(dmTaskCfg[0].From.Password),
		User:     dmTaskCfg[0].From.User,
	}))

	require.True(t, equal(cfg.DataSources["mysql-replica-02"], &DataSource{
		Host:     dmTaskCfg[1].From.Host,
		Port:     dmTaskCfg[1].From.Port,
		Password: utils.SecretString(dmTaskCfg[1].From.Password),
		User:     dmTaskCfg[1].From.User,
	}))
}
