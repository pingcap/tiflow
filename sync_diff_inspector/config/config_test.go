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
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, os.RemoveAll("/tmp/output/config"))
	require.Nil(t, cfg.Parse([]string{"-L", "info", "--config", "config.toml"}))
	cfg = NewConfig()
	require.Contains(t, cfg.Parse([]string{"-L", "info"}).Error(), "argument --config is required")

	unknownFlag := []string{"--LL", "info"}
	err := cfg.Parse(unknownFlag)
	require.Contains(t, err.Error(), "LL")

	require.Nil(t, cfg.Parse([]string{"--config", "config.toml"}))
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// we change the config from config.toml to config_sharding.toml
	// this action will raise error.
	require.Contains(t, cfg.Init().Error(), "failed to init Task: config changes breaking the checkpoint, please use another outputDir and start over again")

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// this time will be ok, because we remove the last outputDir.
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.True(t, cfg.CheckConfig())

	var cfgJSON map[string]any
	require.NoError(t, json.Unmarshal([]byte(cfg.String()), &cfgJSON))
	require.Equal(t, true, cfgJSON["export-fix-sql"])
	require.Equal(t, false, cfgJSON["check-struct-only"])
	require.Equal(t, "config_sharding.toml", cfgJSON["ConfigFile"])

	taskJSON, ok := cfgJSON["task"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "/tmp/output/config", taskJSON["output-dir"])
	require.Equal(t, "/tmp/output/config/fix-on-tidb0", taskJSON["FixDir"])
	require.Equal(t, "/tmp/output/config/checkpoint", taskJSON["CheckpointDir"])
	hash, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)
	require.Equal(t, hash, "e4b4a202a072904121101d516f05ff8144e431ca6094db0fcca375221ddde98d")
	require.True(t, cfg.TableConfigs["config1"].Valid())

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
}

func TestComputeConfigHashIncludesExportFixSQL(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, os.RemoveAll("/tmp/output/config"))
	require.NoError(t, cfg.Parse([]string{"--config", "config.toml"}))
	require.NoError(t, cfg.Init())

	withFixSQL, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)

	cfg.ExportFixSQL = false
	cfg.Task.ExportFixSQL = false
	withoutFixSQL, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)

	require.NotEqual(t, withFixSQL, withoutFixSQL)
	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
}

func TestError(t *testing.T) {
	tableConfig := &TableConfig{}
	require.False(t, tableConfig.Valid())
	tableConfig.TargetTables = []string{"123", "234"}
	require.True(t, tableConfig.Valid())

	cfg := NewConfig()
	// Parse
	require.Contains(t, cfg.Parse([]string{"--config", "no_exist.toml"}).Error(), "no_exist.toml: no such file or directory")

	// CheckConfig
	cfg.CheckThreadCount = 0
	require.False(t, cfg.CheckConfig())
	cfg.CheckThreadCount = 1
	require.True(t, cfg.CheckConfig())

	// Init
	cfg.DataSources = make(map[string]*DataSource)
	cfg.DataSources["123"] = &DataSource{
		RouteRules: []string{"111"},
	}
	err := cfg.Init()
	require.Contains(t, err.Error(), "not found source routes for rule 111, please correct the config")
}

func TestNoSecretLeak(t *testing.T) {
	source := &DataSource{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "postgres",
		Password: "AVeryV#ryStr0ngP@ssw0rd",
		SQLMode:  "MYSQL",
		Snapshot: "2022/10/24",
	}
	cfg := &Config{}
	cfg.DataSources = map[string]*DataSource{"pg-1": source}
	require.NotContains(t, cfg.String(), "AVeryV#ryStr0ngP@ssw0rd", "%s", cfg.String())
	sourceJSON := []byte(`
		{
			"host": "127.0.0.1",
			"port": 5432,
			"user": "postgres",
			"password": "meow~~~"
		}
	`)
	s := DataSource{}
	json.Unmarshal(sourceJSON, &s)
	require.Equal(t, string(s.Password), "meow~~~")
}

func TestComputeConfigHashIgnoresTLSName(t *testing.T) {
	task := &TaskConfig{
		SourceInstances: []*DataSource{
			{
				Host: "127.0.0.1",
				Port: 4000,
				User: "root",
				Security: &Security{
					TLSName: "source-tls-1",
					CAPath:  "/tmp/source-ca.pem",
				},
			},
		},
		TargetInstance: &DataSource{
			Host: "127.0.0.1",
			Port: 4001,
			User: "root",
			Security: &Security{
				TLSName: "target-tls-1",
				CAPath:  "/tmp/target-ca.pem",
			},
		},
		TargetTableConfigs: []*TableConfig{
			{TargetTables: []string{"test.t1"}},
		},
		CheckTables: []string{"test.t1"},
	}

	hash1, err := task.ComputeConfigHash()
	require.NoError(t, err)

	task.SourceInstances[0].Security.TLSName = "source-tls-2"
	task.TargetInstance.Security.TLSName = "target-tls-2"

	hash2, err := task.ComputeConfigHash()
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)

	task.TargetInstance.Security.CAPath = "/tmp/target-ca-new.pem"
	hash3, err := task.ComputeConfigHash()
	require.NoError(t, err)
	require.NotEqual(t, hash2, hash3)
}
