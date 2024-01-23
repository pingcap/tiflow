// Copyright 2023 PingCAP, Inc.
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
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestConfigSecretKeyPath(t *testing.T) {
	cfg := &Common{}
	require.NoError(t, cfg.Adjust())

	// non exist file
	dir := t.TempDir()
	cfg.SecretKeyPath = filepath.Join(dir, "non-exist")
	err := cfg.Adjust()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "no such file")
	require.Nil(t, cfg.SecretKey)

	// not hex string
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret"), []byte("secret"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret")
	err = cfg.Adjust()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "encoding/hex: invalid byte")
	require.Nil(t, cfg.SecretKey)

	// not enough length
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret-2"), []byte("2334"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret-2")
	err = cfg.Adjust()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "hex AES-256 key of length 64")
	require.Nil(t, cfg.SecretKey)

	// works
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret-3"), []byte(" \t"+hex.EncodeToString(key)+"\r\n   \n"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret-3")
	require.NoError(t, cfg.Adjust())
	require.Equal(t, key, cfg.SecretKey)
}
