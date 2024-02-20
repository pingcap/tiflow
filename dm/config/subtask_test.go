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
	"context"
	"crypto/rand"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/encrypt"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestSubTask(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	t.Cleanup(func() {
		encrypt.InitCipher(nil)
	})
	encrypt.InitCipher(key)
	encryptedPass, err := utils.Encrypt("1234")
	require.NoError(t, err)
	require.NotEqual(t, "1234", encryptedPass)
	cfg := &SubTaskConfig{
		Name:            "test-task",
		IsSharding:      true,
		ShardMode:       "optimistic",
		SourceID:        "mysql-instance-01",
		OnlineDDL:       false,
		OnlineDDLScheme: PT,
		From: dbconfig.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: encryptedPass,
		},
		To: dbconfig.DBConfig{
			Host:     "127.0.0.1",
			Port:     4306,
			User:     "root",
			Password: "",
		},
	}
	cfg.From.Adjust()
	cfg.To.Adjust()

	clone1, err := cfg.Clone()
	require.NoError(t, err)
	require.Equal(t, cfg, clone1)

	clone1.From.Password = "1234"
	clone2, err := cfg.DecryptedClone()
	require.NoError(t, err)
	require.Equal(t, clone1, clone2)

	cfg.From.Password = "xxx"
	_, err = cfg.DecryptedClone()
	require.NoError(t, err)
	err = cfg.Adjust(true)
	require.NoError(t, err)
	require.True(t, cfg.OnlineDDL)
	err = cfg.Adjust(false)
	require.NoError(t, err)

	cfg.From.Password = ""
	clone3, err := cfg.DecryptedClone()
	require.NoError(t, err)
	require.Equal(t, cfg, clone3)

	err = cfg.Adjust(true)
	require.NoError(t, err)

	cfg.ValidatorCfg = ValidatorConfig{Mode: ValidationFast}
	err = cfg.Adjust(true)
	require.NoError(t, err)

	cfg.ValidatorCfg = ValidatorConfig{Mode: "invalid-mode"}
	err = cfg.Adjust(true)
	require.True(t, terror.ErrConfigValidationMode.Equal(err))
}

func TestSubTaskAdjustFail(t *testing.T) {
	newSubTaskConfig := func() *SubTaskConfig {
		return &SubTaskConfig{
			Name:      "test-task",
			SourceID:  "mysql-instance-01",
			OnlineDDL: true,
			From: dbconfig.DBConfig{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "root",
				Password: "Up8156jArvIPymkVC+5LxkAT6rek",
			},
			To: dbconfig.DBConfig{
				Host:     "127.0.0.1",
				Port:     4306,
				User:     "root",
				Password: "",
			},
		}
	}
	testCases := []struct {
		genFunc func() *SubTaskConfig
		errMsg  string
	}{
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.Name = ""
				return cfg
			},
			"Message: task name should not be empty",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = ""
				return cfg
			},
			"Message: empty source-id not valid",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = "source-id-length-more-than-thirty-two"
				return cfg
			},
			"Message: too long source-id not valid",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.ShardMode = "invalid-shard-mode"
				return cfg
			},
			"Message: shard mode invalid-shard-mode not supported",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.OnlineDDLScheme = "rtc"
				return cfg
			},
			"Message: online scheme rtc not supported",
		},
	}

	for _, tc := range testCases {
		cfg := tc.genFunc()
		err := cfg.Adjust(true)
		require.ErrorContains(t, err, tc.errMsg)
	}
}

func TestSubTaskBlockAllowList(t *testing.T) {
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
	require.NoError(t, err)
	require.Equal(t, filterRules1, cfg.BAList)

	// BAList is not nil, will not update it
	cfg.BAList = filterRules2
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, filterRules2, cfg.BAList)
}

func TestSubTaskAdjustLoaderS3Dir(t *testing.T) {
	cfg := &SubTaskConfig{
		Name:     "test",
		SourceID: "source-1",
		Mode:     ModeAll,
	}

	// default loader
	cfg.LoaderConfig = DefaultLoaderConfig()
	err := cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, defaultDir+"."+cfg.Name, cfg.LoaderConfig.Dir)

	// file
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "file:///tmp/storage",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "file:///tmp/storage"+"."+cfg.Name, cfg.LoaderConfig.Dir)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "./dump_data",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "./dump_data"+"."+cfg.Name, cfg.LoaderConfig.Dir)

	// s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket2/prefix",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket2/prefix"+"/"+cfg.Name+"."+cfg.SourceID, cfg.LoaderConfig.Dir)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket3/prefix/path/"+cfg.Name+"."+cfg.SourceID+"?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc", cfg.LoaderConfig.Dir)

	// invaild dir
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "1invalid:",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.ErrorContains(t, err, "Message: loader's dir 1invalid: is invalid")

	// use loader and not s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "file:///tmp/storage",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "file:///tmp/storage."+cfg.Name, cfg.LoaderConfig.Dir)

	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "./dumpdir",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "./dumpdir."+cfg.Name, cfg.LoaderConfig.Dir)

	// use loader and s3
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "s3://bucket2/prefix",
		ImportMode: LoadModeLoader,
	}
	err = cfg.Adjust(false)
	require.ErrorContains(t, err, "Message: loader's dir s3://bucket2/prefix is s3 dir, but s3 is not supported")

	// not all or full mode
	cfg.Mode = ModeIncrement
	cfg.LoaderConfig = LoaderConfig{
		PoolSize:   defaultPoolSize,
		Dir:        "1invalid:",
		ImportMode: LoadModeSQL,
	}
	err = cfg.Adjust(false)
	require.NoError(t, err)
	require.Equal(t, "1invalid:", cfg.LoaderConfig.Dir)
}

func TestDBConfigClone(t *testing.T) {
	a := &dbconfig.DBConfig{
		Host:     "127.0.0.1",
		Port:     4306,
		User:     "root",
		Password: "123",
		Session:  map[string]string{"1": "1"},
		RawDBCfg: dbconfig.DefaultRawDBConfig(),
	}

	// When add new fields, also update this value
	require.Equal(t, 9, reflect.Indirect(reflect.ValueOf(a)).NumField())

	b := a.Clone()
	require.Equal(t, a, b)
	require.NotSame(t, a.RawDBCfg, b.RawDBCfg)

	a.RawDBCfg.MaxIdleConns = 123
	require.NotEqual(t, a, b)

	packet := 1
	a.MaxAllowedPacket = &packet
	b = a.Clone()
	require.Equal(t, a, b)
	require.NotSame(t, a.MaxAllowedPacket, b.MaxAllowedPacket)

	a.Session["2"] = "2"
	require.NotEqual(t, a, b)

	a.RawDBCfg = nil
	a.Security = &security.Security{}
	b = a.Clone()
	require.Equal(t, a, b)
	require.NotSame(t, a.Security, b.Security)
}

func TestFetchTZSetting(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(mock.NewRows([]string{""}).AddRow("01:00:00"))
	tz, err := FetchTimeZoneSetting(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, "+01:00", tz)
}
