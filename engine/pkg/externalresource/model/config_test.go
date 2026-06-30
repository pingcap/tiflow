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

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAdjust(t *testing.T) {
	t.Parallel()
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
		cfg.Adjust("test-executor")
		require.Equal(t, oldCfg.S3, cfg.S3, "inputBaseDir: %s", dir)
		require.NotEqual(t, oldCfg.Local, cfg.Local, "inputBaseDir: %s", dir)

		expected := filepath.Join(dir, "test-executor")
		require.Equal(t, expected, cfg.Local.BaseDir, "inputBaseDir: %s", dir)
	}
}

func TestToBrBackendOptions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		config         *Config
		expectedOpts   *brStorage.BackendOptions
		expectedBucket string
		expectedPrefix string
		expectedType   ResourceType
	}{
		{
			config:         &Config{},
			expectedOpts:   &brStorage.BackendOptions{},
			expectedBucket: "",
			expectedPrefix: "",
			expectedType:   ResourceTypeNone,
		},
		{
			config: &Config{
				S3: S3Config{
					Bucket: "s3-bucket",
					Prefix: "pe",
				},
				GCS: GCSConfig{
					Prefix: "pe1",
				},
			},
			expectedOpts:   &brStorage.BackendOptions{},
			expectedBucket: "s3-bucket",
			expectedPrefix: "pe",
			expectedType:   ResourceTypeS3,
		},
		{
			config: &Config{
				GCS: GCSConfig{
					Bucket: "gcs-bucket",
					Prefix: "pe1",
				},
			},
			expectedOpts:   &brStorage.BackendOptions{},
			expectedBucket: "gcs-bucket",
			expectedPrefix: "pe1",
			expectedType:   ResourceTypeGCS,
		},
	}

	for _, cs := range cases {
		opts, bucket, prefix, tp := cs.config.ToBrBackendOptions()
		require.Equal(t, cs.expectedOpts, opts)
		require.Equal(t, cs.expectedBucket, bucket)
		require.Equal(t, cs.expectedPrefix, prefix)
		require.Equal(t, cs.expectedType, tp)
	}
}
