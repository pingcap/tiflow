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
	require.Contains(t, cfg.Init().Error(), "failed to init Task: config changes breaking the checkpoint, please use another outputDir and start over again!")

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// this time will be ok, because we remove the last outputDir.
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.True(t, cfg.CheckConfig())

	// we might not use the same config to run this test. e.g. MYSQL_PORT can be 4000
	require.JSONEq(t, cfg.String(),
		"{\"check-thread-count\":4,\"split-thread-count\":5,\"export-fix-sql\":true,\"check-struct-only\":false,\"dm-addr\":\"\",\"dm-task\":\"\",\"data-sources\":{\"mysql1\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"mysql2\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"mysql3\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule3\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"tidb0\":{\"host\":\"127.0.0.1\",\"port\":4000,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":null,\"Router\":{\"Selector\":{}},\"Conn\":null}},\"routes\":{\"rule1\":{\"schema-pattern\":\"test_*\",\"table-pattern\":\"t_*\",\"target-schema\":\"test\",\"target-table\":\"t\"},\"rule2\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test2\",\"target-table\":\"t2\"},\"rule3\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test\",\"target-table\":\"t\"}},\"table-configs\":{\"config1\":{\"target-tables\":[\"schema*.table*\",\"test2.t2\"],\"Schema\":\"\",\"Table\":\"\",\"ConfigIndex\":0,\"HasMatched\":false,\"IgnoreColumns\":[\"\",\"\"],\"Fields\":[\"\"],\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"TargetTableInfo\":null,\"Collation\":\"\",\"chunk-size\":0}},\"task\":{\"source-instances\":[\"mysql1\",\"mysql2\",\"mysql3\"],\"source-routes\":null,\"target-instance\":\"tidb0\",\"target-check-tables\":[\"schema*.table*\",\"!c.*\",\"test2.t2\"],\"target-configs\":[\"config1\"],\"output-dir\":\"/tmp/output/config\",\"SourceInstances\":[{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":[\"rule1\",\"rule3\"],\"Router\":{\"Selector\":{}},\"Conn\":null}],\"TargetInstance\":{\"host\":\"127.0.0.1\",\"port\":4000,\"user\":\"root\",\"password\":\"******\",\"sql-mode\":\"\",\"snapshot\":\"\",\"security\":null,\"route-rules\":null,\"Router\":{\"Selector\":{}},\"Conn\":null},\"TargetTableConfigs\":[{\"target-tables\":[\"schema*.table*\",\"test2.t2\"],\"Schema\":\"\",\"Table\":\"\",\"ConfigIndex\":0,\"HasMatched\":false,\"IgnoreColumns\":[\"\",\"\"],\"Fields\":[\"\"],\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"TargetTableInfo\":null,\"Collation\":\"\",\"chunk-size\":0}],\"TargetCheckTables\":[{},{},{}],\"FixDir\":\"/tmp/output/config/fix-on-tidb0\",\"CheckpointDir\":\"/tmp/output/config/checkpoint\",\"HashFile\":\"\"},\"ConfigFile\":\"config_sharding.toml\",\"PrintVersion\":false}")
	hash, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)
	require.Equal(t, hash, "c080f9894ec24aadb4aaec1109cd1951454f09a1233f2034bc3b06e0903cb289")

	require.True(t, cfg.TableConfigs["config1"].Valid())

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
		SqlMode:  "MYSQL",
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
