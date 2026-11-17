// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsistentStorageValidationDoesNotEchoCredentials(t *testing.T) {
	file := filepath.Join(t.TempDir(), "existing-file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	for _, storage := range []string{
		"s3://bucket/%zz?access-key=credential-sentinel",
		"local://" + file + "/redo?access-key=credential-sentinel",
	} {
		cfg := &ConsistentConfig{Level: "eventual", Storage: storage}
		err := cfg.ValidateAndAdjust()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "credential-sentinel")
		require.Contains(t, err.Error(), "invalid storage uri")
	}
}
