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

package storagecfg

// Config defines configurations for a external storage resource
type Config struct {
	Local *LocalFileConfig `json:"local" toml:"local"`
}

// LocalFileConfig defines configurations for a local file based resource
type LocalFileConfig struct {
	BaseDir string `json:"base-dir" toml:"base-dir"`
}
