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

package metaclient

import (
	"strings"

	dmysql "github.com/go-sql-driver/mysql"

	"github.com/pingcap/tiflow/engine/pkg/dbutil"
)

const (
	defaultReadTimeout  = "3s"
	defaultWriteTimeout = "3s"
	defaultDialTimeout  = "3s"
)

// AuthConfParams is basic authentication configurations
type AuthConfParams struct {
	User   string `toml:"user" json:"user"`
	Passwd string `toml:"passwd" json:"passwd"`
}

// StoreConfig is metastore connection configurations
type StoreConfig struct {
	// storeID is the unique readable identifier for a store
	StoreID   string          `toml:"store-id" json:"store-id"`
	Endpoints []string        `toml:"endpoints" json:"endpoints"`
	Auth      *AuthConfParams `toml:"auth" json:"auth"`
	// Schema is the predefine schema name for cluster metastore
	Schema       string `toml:"schema" json:"schema"`
	ReadTimeout  string `toml:"read-timeout" json:"read-timeout"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	DialTimeout  string `toml:"dial-timeout" json:"dial-timeout"`
	// DB configs if backend metastore is DB
	DBConf *dbutil.DBConfig `toml:"meta-dbconfs" json:"meta-dbconfs"`
}

// SetEndpoints sets endpoints to StoreConfig
func (s *StoreConfig) SetEndpoints(endpoints string) {
	if endpoints != "" {
		s.Endpoints = strings.Split(endpoints, ",")
	}
}

// DefaultStoreConfig return a default StoreConfig
func DefaultStoreConfig() StoreConfig {
	dbConf := dbutil.DefaultDBConfig()
	return StoreConfig{
		Endpoints:    []string{},
		Auth:         &AuthConfParams{},
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
		DialTimeout:  defaultDialTimeout,
		DBConf:       &dbConf,
	}
}

// GenerateDSNByParams generates a dsn string.
// dsn format: [username[:password]@][protocol[(address)]]/
func GenerateDSNByParams(storeConf *StoreConfig) string {
	if storeConf == nil {
		return "invalid dsn"
	}

	dsnCfg := dmysql.NewConfig()
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	if storeConf.Auth != nil {
		dsnCfg.User = storeConf.Auth.User
		dsnCfg.Passwd = storeConf.Auth.Passwd
	}
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = storeConf.Endpoints[0]
	dsnCfg.DBName = storeConf.Schema
	dsnCfg.InterpolateParams = true
	// dsnCfg.MultiStatements = true
	dsnCfg.Params["parseTime"] = "true"
	// TODO: check for timezone
	dsnCfg.Params["loc"] = "Local"
	dsnCfg.Params["readTimeout"] = storeConf.ReadTimeout
	dsnCfg.Params["writeTimeout"] = storeConf.WriteTimeout
	dsnCfg.Params["timeout"] = storeConf.DialTimeout

	return dsnCfg.FormatDSN()
}
