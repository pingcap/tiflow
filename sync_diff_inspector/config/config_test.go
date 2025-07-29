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
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	uuid.SetRand(rand.New(rand.NewSource(1)))
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
	require.Contains(t, cfg.Init().Error(), "failed to init Task: config changes breaking the checkpoint, please use another outputDir and start over again")

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// this time will be ok, because we remove the last outputDir.
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.True(t, cfg.CheckConfig())

	// we might not use the same config to run this test. e.g. MYSQL_PORT can be 4000
	require.JSONEq(t,
		"{\"ConfigFile\":\"config_sharding.toml\",\"PrintVersion\":false,\"check-struct-only\":false,\"check-thread-count\":4,\"data-sources\":{\"mysql1\":{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule2\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},\"mysql2\":{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule2\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},\"mysql3\":{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule3\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},\"tidb0\":{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":4000,\"route-rules\":null,\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":{\"max_execution_time\":86400,\"tidb_opt_prefer_range_scan\":\"ON\"},\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"}},\"dm-addr\":\"\",\"dm-task\":\"\",\"export-fix-sql\":true,\"routes\":{\"rule1\":{\"schema-pattern\":\"test_*\",\"table-pattern\":\"t_*\",\"target-schema\":\"test\",\"target-table\":\"t\"},\"rule2\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test2\",\"target-table\":\"t2\"},\"rule3\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test\",\"target-table\":\"t\"}},\"split-thread-count\":5,\"table-configs\":{\"config1\":{\"Collation\":\"\",\"ConfigIndex\":0,\"Fields\":[\"\"],\"HasMatched\":false,\"IgnoreColumns\":[\"\",\"\"],\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"Schema\":\"\",\"Table\":\"\",\"TargetTableInfo\":null,\"chunk-size\":0,\"target-tables\":[\"schema*.table*\",\"test2.t2\"]}},\"task\":{\"CheckpointDir\":\"/tmp/output/config/checkpoint\",\"FixDir\":\"/tmp/output/config/fix-on-tidb0\",\"HashFile\":\"\",\"SourceInstances\":[{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule2\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule2\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":3306,\"route-rules\":[\"rule1\",\"rule3\"],\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":null,\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"}],\"TargetCheckTables\":[{},{},{}],\"TargetInstance\":{\"Conn\":null,\"Router\":{\"Selector\":{}},\"host\":\"127.0.0.1\",\"password\":\"******\",\"port\":4000,\"route-rules\":null,\"security\":{\"ca-bytes\":\"\",\"ca-path\":\"\",\"cert-bytes\":\"\",\"cert-path\":\"\",\"key-bytes\":\"\",\"key-path\":\"\"},\"session\":{\"max_execution_time\":86400,\"tidb_opt_prefer_range_scan\":\"ON\"},\"snapshot\":\"\",\"sql-hint-use-index\":\"\",\"sql-mode\":\"\",\"user\":\"root\"},\"TargetTableConfigs\":[{\"Collation\":\"\",\"ConfigIndex\":0,\"Fields\":[\"\"],\"HasMatched\":false,\"IgnoreColumns\":[\"\",\"\"],\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"Schema\":\"\",\"Table\":\"\",\"TargetTableInfo\":null,\"chunk-size\":0,\"target-tables\":[\"schema*.table*\",\"test2.t2\"]}],\"output-dir\":\"/tmp/output/config\",\"source-instances\":[\"mysql1\",\"mysql2\",\"mysql3\"],\"source-routes\":null,\"target-check-tables\":[\"schema*.table*\",\"!c.*\",\"test2.t2\"],\"target-configs\":[\"config1\"],\"target-instance\":\"tidb0\"}}", cfg.String())
	hash, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)
	require.Equal(t, "043cfdd2038e52e4af9ddaa202f4509b9e35b8d16224053ec9fa8b611168264b", hash)
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
