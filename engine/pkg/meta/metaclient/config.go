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

	"github.com/pingcap/tiflow/engine/pkg/sqlutil"
)

// defines const variables used in metastore client
const (
	FrameMetaID       = "root"
	DefaultUserMetaID = "default"

	DefaultFrameMetaEndpoints = "127.0.0.1:3336"
	DefaultFrameMetaUser      = "root"
	DefaultFrameMetaPassword  = "123456"

	DefaultUserMetaEndpoints = "127.0.0.1:12479"
	DefaultUserMetaUser      = "root"
	DefaultUserMetaPassword  = "123456"

	DefaultReadTimeout  = "3s"
	DefaultWriteTimeout = "3s"
	DefaultDialTimeout  = "3s"
)

// AuthConfParams is basic password authentication configurations
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
	// unique schema for a tiflow cluster
	Schema       string `toml:"meta-schema" json:"meta-schema"`
	ReadTimeout  string `toml:"read-timeout" json:"read-timeout"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	DialTimeout  string `toml:"dial-timeout" json:"dial-timeout"`
	// DB configs for backend metastore
	DBConf *sqlutil.DBConfig `toml:"meta-dbconfs" json:"meta-dbconfs"`
}

func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		ReadTimeout:  DefaultReadTimeout,
		WriteTimeout: DefaultWriteTimeout,
		DialTimeout:  DefaultDialTimeout,
	}
}

// SetEndpoints sets endpoints to StoreConfig
func (s *StoreConfig) SetEndpoints(endpoints string) {
	if endpoints != "" {
		s.Endpoints = strings.Split(endpoints, ",")
	}
}
