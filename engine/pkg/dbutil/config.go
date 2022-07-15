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

package dbutil

import "time"

const (
	defaultConnMaxIdleTime = 1 * time.Minute
	defaultConnMaxLifeTime = 12 * time.Hour
	defaultMaxIdleConns    = 3
	defaultMaxOpenConns    = 7
	// TODO: more params for mysql connection
)

// DBConfig defines some configuration used in database connection
// refer to: https://pkg.go.dev/database/sql#SetConnMaxIdleTime
type DBConfig struct {
	ConnMaxIdleTime time.Duration `toml:"conn-max-idle-time" json:"conn-max-idle-time"`
	ConnMaxLifeTime time.Duration `toml:"conn-max-life-time" json:"conn-max-life-time"`
	MaxIdleConns    int           `toml:"max-idle-conns" json:"max-idle-conns"`
	MaxOpenConns    int           `toml:"max-open-conns" json:"max-open-conns"`
}

// DefaultDBConfig creates a default DBConfig
func DefaultDBConfig() *DBConfig {
	return &DBConfig{
		ConnMaxIdleTime: defaultConnMaxIdleTime,
		ConnMaxLifeTime: defaultConnMaxLifeTime,
		MaxIdleConns:    defaultMaxIdleConns,
		MaxOpenConns:    defaultMaxOpenConns,
	}
}
