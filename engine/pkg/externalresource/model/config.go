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
	"path/filepath"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
)

const defaultLocalStorageDirPrefix = "/tmp/dfe-storage"

// DefaultConfig defines the default configuration for external storage
var DefaultConfig = Config{
	Local: LocalFileConfig{BaseDir: ""},
	S3: S3Config{
		S3BackendOptions: brStorage.S3BackendOptions{
			ForcePathStyle: true,
		},
		Bucket: "",
		Prefix: "",
	},
}

// Config defines configurations for an external storage resource
type Config struct {
	Local LocalFileConfig `json:"local" toml:"local"`
	S3    S3Config        `json:"s3" toml:"s3"`
}

// LocalEnabled returns true if the local storage is enabled
func (c *Config) LocalEnabled() bool {
	return c.Local.BaseDir != ""
}

// S3Enabled returns true if the S3 storage is enabled
func (c *Config) S3Enabled() bool {
	return c.S3.Bucket != ""
}

// ValidateAndAdjust validates and adjusts the configuration
func (c *Config) ValidateAndAdjust(executorID ExecutorID) {
	c.Local.validateAndAdjust(executorID)
}

// LocalFileConfig defines configurations for a local file based resource
type LocalFileConfig struct {
	BaseDir string `json:"base-dir" toml:"base-dir"`
}

func (c *LocalFileConfig) validateAndAdjust(executorID ExecutorID) {
	if c.BaseDir == "" {
		c.BaseDir = defaultLocalStorageDirPrefix
	}
	c.BaseDir = filepath.Join(c.BaseDir, string(executorID))
}

// S3Config defines configurations for s3 based resources
type S3Config struct {
	brStorage.S3BackendOptions
	Bucket string `json:"bucket" toml:"bucket"`
	Prefix string `json:"prefix" toml:"prefix"`
}
