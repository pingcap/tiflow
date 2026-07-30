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

package alertmanager

import (
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const (
	binlogFileGapAlertName = "DM_binlog_file_gap_between_master_syncer"
	binlogFileGapAlertExpr = `(dm_syncer_binlog_file{node="master"} - ON(instance, task, job) dm_syncer_binlog_file{node="syncer"} > 1) and ON(instance, task, job) (dm_worker_task_state == 2)`
)

type alertRule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

type alertRulesFile struct {
	Groups []struct {
		Name  string      `yaml:"name"`
		Rules []alertRule `yaml:"rules"`
	} `yaml:"groups"`
}

func TestBinlogFileGapAlertRuleRequiresRunningTask(t *testing.T) {
	rule := loadAlertRule(t, "dm_worker.rules.yml", binlogFileGapAlertName)

	require.Equal(t, binlogFileGapAlertExpr, rule.Expr)
	require.Equal(t, binlogFileGapAlertExpr, rule.Labels["expr"])
	require.Equal(t, "10m", rule.For)

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:           10000,
		Timeout:              time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return time.Minute.Milliseconds()
		},
	})

	promql.RunTest(t, `
load 1m
  dm_syncer_binlog_file{instance="worker-1",job="dm_worker",node="master",source_id="mysql-01",task="running"} 12
  dm_syncer_binlog_file{instance="worker-1",job="dm_worker",node="syncer",source_id="mysql-01",task="running"} 10
  dm_worker_task_state{instance="worker-1",job="dm_worker",source_id="mysql-01",task="running",worker="worker-1"} 2
  dm_syncer_binlog_file{instance="worker-2",job="dm_worker",node="master",source_id="mysql-02",task="paused"} 12
  dm_syncer_binlog_file{instance="worker-2",job="dm_worker",node="syncer",source_id="mysql-02",task="paused"} 10
  dm_worker_task_state{instance="worker-2",job="dm_worker",source_id="mysql-02",task="paused",worker="worker-2"} 3
  dm_syncer_binlog_file{instance="worker-3",job="dm_worker",node="master",source_id="mysql-03",task="stopped"} 12
  dm_syncer_binlog_file{instance="worker-3",job="dm_worker",node="syncer",source_id="mysql-03",task="stopped"} 10

eval instant at 0m `+rule.Expr+`
  {instance="worker-1",job="dm_worker",task="running"} 2
`, engine)
}

func loadAlertRule(t *testing.T, filename, alertName string) alertRule {
	t.Helper()

	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	var ruleFile alertRulesFile
	require.NoError(t, yaml.Unmarshal(content, &ruleFile))

	for _, group := range ruleFile.Groups {
		for _, rule := range group.Rules {
			if rule.Alert == alertName {
				require.NotEmpty(t, rule.Expr)
				return rule
			}
		}
	}

	t.Fatalf("alert rule %q not found in %s", alertName, filename)
	return alertRule{}
}
