// Copyright 2026 PingCAP, Inc.
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

package master

import (
	"context"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/stretchr/testify/require"
)

func TestCheckDuplicateBinlogFilterRules(t *testing.T) {
	for _, tc := range []struct {
		name          string
		patterns      [][2]string
		caseSensitive bool
		duplicate     bool
	}{
		{name: "empty"},
		{name: "same table", patterns: [][2]string{{"db", "t"}, {"db", "t"}}, duplicate: true},
		{name: "same schema", patterns: [][2]string{{"db", ""}, {"db", ""}}, duplicate: true},
		{name: "same wildcard", patterns: [][2]string{{"db*", "t*"}, {"db*", "t*"}}, duplicate: true},
		{name: "case insensitive", patterns: [][2]string{{"Db", "T"}, {"db", "t"}}, duplicate: true},
		{name: "case sensitive", patterns: [][2]string{{"Db", "T"}, {"db", "t"}}, caseSensitive: true},
		{name: "case sensitive duplicate", patterns: [][2]string{{"Db", "T"}, {"Db", "T"}}, caseSensitive: true, duplicate: true},
		{name: "overlap", patterns: [][2]string{{"db", "t*"}, {"db", "t"}}},
		{name: "schema and table", patterns: [][2]string{{"db", ""}, {"db", "t"}}},
		{name: "different schemas", patterns: [][2]string{{"db1", "t"}, {"db2", "t"}}},
		{name: "different tables", patterns: [][2]string{{"db", "t1"}, {"db", "t2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.SubTaskConfig{SourceID: "mysql-01", CaseSensitive: tc.caseSensitive}
			events := []bf.EventType{bf.DeleteEvent, bf.UpdateEvent}
			for i, pattern := range tc.patterns {
				cfg.FilterRules = append(cfg.FilterRules, &bf.BinlogEventRule{
					SchemaPattern: pattern[0], TablePattern: pattern[1],
					Events: []bf.EventType{events[i%len(events)]}, Action: bf.Ignore,
				})
			}
			err := checkDuplicateBinlogFilterRules(cfg)
			if tc.duplicate {
				require.True(t, terror.ErrConfigBinlogEventFilter.Equal(err))
				require.ErrorContains(t, err, `source "mysql-01": binlog filter rules 1 and 2 have duplicate schema-pattern`)
			} else {
				require.NoError(t, err)
			}
			// Validation must not mutate patterns, even in case-insensitive mode.
			for i, rule := range cfg.FilterRules {
				require.Equal(t, tc.patterns[i], [2]string{rule.SchemaPattern, rule.TablePattern})
			}
		})
	}

	// This check must not reintroduce validation of legacy invalid event types.
	cfg := &config.SubTaskConfig{FilterRules: []*bf.BinlogEventRule{
		nil,
		{SchemaPattern: "db", TablePattern: "t", Events: []bf.EventType{"truncate"}, Action: bf.Ignore},
	}}
	require.NoError(t, checkDuplicateBinlogFilterRules(cfg))
}

func (t *testMasterSuite) TestRejectDuplicateBinlogFilterPatterns() {
	server := testDefaultMasterServer(t.T())
	server.etcdClient = t.etcdTestCli
	sources, workers := defaultWorkerSource()
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = t.testMockScheduler(ctx, &wg, sources, workers, "", makeNilWorkerClients(workers))
	defer t.clearSchedulerEnv(cancel, &wg)
	defer func() { conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{} }()

	cfg := config.NewTaskConfig()
	require.NoError(t.T(), cfg.RawDecode(taskConfig))
	cfg.Filters = map[string]*bf.BinlogEventRule{
		"delete": {SchemaPattern: "db", TablePattern: "t", Events: []bf.EventType{bf.DeleteEvent}, Action: bf.Ignore},
		"update": {SchemaPattern: "db", TablePattern: "t", Events: []bf.EventType{bf.UpdateEvent}, Action: bf.Ignore},
	}
	cfg.MySQLInstances[0].FilterRules = []string{"delete", "update"}
	task := cfg.String()
	for _, operation := range []string{"start", "update", "check"} {
		mock := conn.InitVersionDB()
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.25-TiDB-v4.0.2"))
		var result bool
		var message string
		switch operation {
		case "start":
			resp, err := server.StartTask(ctx, &pb.StartTaskRequest{Task: task})
			require.NoError(t.T(), err)
			result, message = resp.Result, resp.Msg
		case "update":
			resp, err := server.UpdateTask(ctx, &pb.UpdateTaskRequest{Task: task})
			require.NoError(t.T(), err)
			result, message = resp.Result, resp.Msg
		case "check":
			resp, err := server.CheckTask(ctx, &pb.CheckTaskRequest{Task: task})
			require.NoError(t.T(), err)
			result, message = resp.Result, resp.Msg
		}
		require.False(t.T(), result, operation)
		require.Contains(t.T(), message, "duplicate schema-pattern", operation)
		require.Contains(t.T(), message, sources[0], operation)
		require.NoError(t.T(), mock.ExpectationsWereMet())
	}
	require.Empty(t.T(), server.scheduler.GetSubTaskCfgsByTask(cfg.Name))
}
