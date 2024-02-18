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
	"encoding/hex"
	"os"
	"strings"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Common is the common configuration for dm-master and dm-ctl.
type Common struct {
	SecretKeyPath string `toml:"secret-key-path" json:"secret-key-path"  yaml:"secret-key-path"`
	SecretKey     []byte `toml:"-" json:"-" yaml:"-"`
}

// Adjust adjusts configs.
func (c *Common) Adjust() error {
	if c.SecretKeyPath == "" {
		return nil
	}

	content, err := os.ReadFile(c.SecretKeyPath)
	if err != nil {
		return terror.ErrConfigSecretKeyPath.Generate(err)
	}
	contentStr := strings.TrimSpace(string(content))
	decodeContent, err := hex.DecodeString(contentStr)
	if err != nil {
		return terror.ErrConfigSecretKeyPath.Generate(err)
	}
	if len(decodeContent) != 32 {
		return terror.ErrConfigSecretKeyPath.Generate("the secret key must be a hex AES-256 key of length 64")
	}
	c.SecretKey = decodeContent
	return nil
}
