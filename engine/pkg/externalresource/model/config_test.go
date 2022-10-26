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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAndAdjust(t *testing.T) {
	dirs := []string{"", "/tmp/dfe-storage", "/var/engine/", "/a/b", "/a/b/c"}
	for _, dir := range dirs {
		cfg := Config{
			Local: LocalFileConfig{
				BaseDir: dir,
			},
		}
		if dir == "" {
			dir = defaultLocalStorageDirPrefix
		}
		oldCfg := cfg
		cfg.ValidateAndAdjust("test-executor")
		require.Equal(t, oldCfg.S3, cfg.S3, "inputBaseDir: %s", dir)
		require.NotEqual(t, oldCfg.Local, cfg.Local, "inputBaseDir: %s", dir)

		expected := filepath.Join(dir, "test-executor")
		require.Equal(t, expected, cfg.Local.BaseDir, "inputBaseDir: %s", dir)
	}
}
