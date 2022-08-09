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

package orm

import "time"

// TODO: split the config file
const (
	DefaultFrameMetaEndpoints = "127.0.0.1:3336"
	DefaultFrameMetaUser      = "root"
	DefaultFrameMetaPassword  = ""
)

const (
	defaultConnMaxIdleTime = 30 * time.Second
	defaultConnMaxLifeTime = 12 * time.Hour
	defaultMaxIdleConns    = 3
	defaultMaxOpenConns    = 10
	defaultReadTimeout     = "3s"
	defaultWriteTimeout    = "3s"
	defaultDialTimeout     = "3s"
	// TODO: more params for mysql connection
)

// DBConfig defines some configuration used in database connection
// refer to: https://pkg.go.dev/database/sql#SetConnMaxIdleTime
type DBConfig struct {
	ReadTimeout     string
	WriteTimeout    string
	DialTimeout     string
	ConnMaxIdleTime time.Duration
	ConnMaxLifeTime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
}

// NewDefaultDBConfig creates a default DBConfig
func NewDefaultDBConfig() DBConfig {
	return DBConfig{
		ReadTimeout:     defaultReadTimeout,
		WriteTimeout:    defaultWriteTimeout,
		DialTimeout:     defaultDialTimeout,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
		ConnMaxLifeTime: defaultConnMaxLifeTime,
		MaxIdleConns:    defaultMaxIdleConns,
		MaxOpenConns:    defaultMaxOpenConns,
	}
}
