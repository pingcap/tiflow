// Copyright 2022 PingCAP, Inc.
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

package model

import (
	"regexp"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/dbutil"
	"github.com/stretchr/testify/require"
)

func TestGenerateDSNByParams(t *testing.T) {
	t.Parallel()

	sf := DefaultStoreConfig()
	sf.Endpoints = []string{"1.1.1.1"}
	sf.User = "user"
	sf.Password = "passwd"
	dsn, err := GenerateDSNByParams(sf, map[string]string{
		"sql_model": dbutil.GetSQLStrictMode(),
	})
	require.NoError(t, err)
	require.Equal(t, "user:passwd@tcp(1.1.1.1)/?interpolateParams=true&"+
		"loc=Local&parseTime=true&readTimeout=3s&sql_model=STRICT_TRANS_TABLES%2CSTRIC"+
		"T_ALL_TABLES%2CERROR_FOR_DIVISION_BY_ZERO&timeout=3s&writeTimeout=3s", dsn)
}

func TestValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		caseName string
		conf     *StoreConfig
		errMsg   string
	}{
		{
			caseName: "Normal",
			conf: &StoreConfig{
				StoreType: defaultStoreType,
				Schema:    "test",
			},
			errMsg: "",
		},
		{
			caseName: "StoreTypeNotInEtcdOrSQL",
			conf: &StoreConfig{
				StoreType: "unknown",
			},
			errMsg: "store-type: must be a valid value",
		},
		{
			caseName: "StoreTypeEtcdNotCheckSchema",
			conf: &StoreConfig{
				StoreType: StoreTypeEtcd,
				Schema:    "",
			},
			errMsg: "",
		},
		{
			caseName: "StoreTypeMySQLCheckSchema",
			conf: &StoreConfig{
				StoreType: StoreTypeMySQL,
				Schema:    "",
			},
			errMsg: "schema: cannot be blank",
		},
	}

	for _, ce := range cases {
		err := ce.conf.Validate()
		if ce.errMsg == "" {
			require.NoError(t, err)
		} else {
			require.Regexp(t, regexp.QuoteMeta(ce.errMsg), err.Error())
		}
	}
}
