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

package config

import (
	"reflect"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/filter"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func (t *testConfig) TestSubTask(c *C) {
	cfg := &SubTaskConfig{
		Name:            "test-task",
		IsSharding:      true,
		ShardMode:       "optimistic",
		SourceID:        "mysql-instance-01",
		OnlineDDL:       false,
		OnlineDDLScheme: PT,
		From: DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "Up8156jArvIPymkVC+5LxkAT6rek",
		},
		To: DBConfig{
			Host:     "127.0.0.1",
			Port:     4306,
			User:     "root",
			Password: "",
		},
	}
	cfg.From.Adjust()
	cfg.To.Adjust()

	clone1, err := cfg.Clone()
	c.Assert(err, IsNil)
	c.Assert(cfg, DeepEquals, clone1)

	clone1.From.Password = "1234"
	clone2, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone2, DeepEquals, clone1)

	cfg.From.Password = "xxx"
	_, err = cfg.DecryptPassword()
	c.Assert(err, IsNil)
	err = cfg.Adjust(true)
	c.Assert(err, IsNil)
	c.Assert(cfg.OnlineDDL, IsTrue)
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)

	cfg.From.Password = ""
	clone3, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone3, DeepEquals, cfg)

	err = cfg.Adjust(true)
	c.Assert(err, IsNil)

	cfg.ValidatorCfg = ValidatorConfig{Mode: ValidationFast}
	err = cfg.Adjust(true)
	c.Assert(err, IsNil)

	cfg.ValidatorCfg = ValidatorConfig{Mode: "invalid-mode"}
	err = cfg.Adjust(true)
	c.Assert(terror.ErrConfigValidationMode.Equal(err), IsTrue)
}

func (t *testConfig) TestSubTaskAdjustFail(c *C) {
	newSubTaskConfig := func() *SubTaskConfig {
		return &SubTaskConfig{
			Name:      "test-task",
			SourceID:  "mysql-instance-01",
			OnlineDDL: true,
			From: DBConfig{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "root",
				Password: "Up8156jArvIPymkVC+5LxkAT6rek",
			},
			To: DBConfig{
				Host:     "127.0.0.1",
				Port:     4306,
				User:     "root",
				Password: "",
			},
		}
	}
	testCases := []struct {
		genFunc     func() *SubTaskConfig
		errorFormat string
	}{
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.Name = ""
				return cfg
			},
			"\\[.*\\], Message: task name should not be empty.*",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = ""
				return cfg
			},
			"\\[.*\\], Message: empty source-id not valid.*",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = "source-id-length-more-than-thirty-two"
				return cfg
			},
			"\\[.*\\], Message: too long source-id not valid.*",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.ShardMode = "invalid-shard-mode"
				return cfg
			},
			"\\[.*\\], Message: shard mode invalid-shard-mode not supported.*",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.OnlineDDLScheme = "rtc"
				return cfg
			},
			"\\[.*\\], Message: online scheme rtc not supported.*",
		},
	}

	for _, tc := range testCases {
		cfg := tc.genFunc()
		err := cfg.Adjust(true)
		c.Assert(err, ErrorMatches, tc.errorFormat)
	}
}

func (t *testConfig) TestSubTaskBlockAllowList(c *C) {
	filterRules1 := &filter.Rules{
		DoDBs: []string{"s1"},
	}

	filterRules2 := &filter.Rules{
		DoDBs: []string{"s2"},
	}

	cfg := &SubTaskConfig{
		Name:     "test",
		SourceID: "source-1",
		BWList:   filterRules1,
	}

	// BAList is nil, will set BAList = BWList
	err := cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.BAList, Equals, filterRules1)

	// BAList is not nil, will not update it
	cfg.BAList = filterRules2
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.BAList, Equals, filterRules2)
}

func (t *testConfig) TestSubTaskAdjustLoaderS3Dir(c *C) {
	cfg := &SubTaskConfig{
		Name:     "test",
		SourceID: "source-1",
		Mode:     ModeAll,
	}

	// default loader
	cfg.LoaderConfig = DefaultLoaderConfig()
	err := cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, defaultDir+"."+cfg.Name)

	// file
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "file:///tmp/storage",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "file:///tmp/storage"+"."+cfg.Name)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "./dump_data",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "./dump_data"+"."+cfg.Name)

	// s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket2/prefix",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "s3://bucket2/prefix"+"/"+cfg.Name+"."+cfg.SourceID)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "s3://bucket3/prefix/path"+"/"+cfg.Name+"."+cfg.SourceID+"?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc")

	// invaild dir
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "1invalid:",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, ErrorMatches, "\\[.*\\], Message: loader's dir 1invalid: is invalid.*")

	// use loader and not s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "file:///tmp/storage",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "file:///tmp/storage"+"."+cfg.Name)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "./dumpdir",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "./dumpdir"+"."+cfg.Name)

	// use loader and s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket2/prefix",
		ImportMode: LoadModeLoader,
	}
	err = cfg.Adjust(false)
	c.Assert(err, ErrorMatches, "\\[.*\\], Message: loader's dir s3://bucket2/prefix is s3 dir, but s3 is not supported.*")

	// not all or full mode
	cfg.Mode = ModeIncrement
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "1invalid:",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)
	c.Assert(cfg.LoaderConfig.Dir, Equals, "1invalid:")
}

func (t *testConfig) TestDBConfigClone(c *C) {
	a := &DBConfig{
		Host:     "127.0.0.1",
		Port:     4306,
		User:     "root",
		Password: "123",
		Session:  map[string]string{"1": "1"},
		RawDBCfg: DefaultRawDBConfig(),
	}

	// When add new fields, also update this value
	c.Assert(reflect.Indirect(reflect.ValueOf(a)).NumField(), Equals, 8)

	b := a.Clone()
	c.Assert(a, DeepEquals, b)
	c.Assert(a.RawDBCfg, Not(Equals), b.RawDBCfg)

	a.RawDBCfg.MaxIdleConns = 123
	c.Assert(a, Not(DeepEquals), b)

	packet := 1
	a.MaxAllowedPacket = &packet
	b = a.Clone()
	c.Assert(a, DeepEquals, b)
	c.Assert(a.MaxAllowedPacket, Not(Equals), b.MaxAllowedPacket)

	a.Session["2"] = "2"
	c.Assert(a, Not(DeepEquals), b)

	a.RawDBCfg = nil
	a.Security = &Security{}
	b = a.Clone()
	c.Assert(a, DeepEquals, b)
	c.Assert(a.Security, Not(Equals), b.Security)
}
