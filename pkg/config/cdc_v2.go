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
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"net/url"

	"github.com/pingcap/errors"
)

// CDCV2 represents config for ticdc v2
type CDCV2 struct {
	// Enable represents if the cdc v2 is enabled or not
	Enable bool `toml:"enable" json:"enable"`
	// MetaStoreConfig  represents config for new meta store configurations
	MetaStoreConfig MetaStoreConfiguration `toml:"meta-store" json:"meta-store"`
}

// MetaStoreConfiguration represents config for new meta store configurations
type MetaStoreConfiguration struct {
	// URI is the address of the meta store.
	// for example:  "mysql://127.0.0.1:3306/test"
	URI string `toml:"uri" json:"uri"`
}

// ValidateAndAdjust validates the meta store configurations
func (c *CDCV2) ValidateAndAdjust() error {
	if !c.Enable {
		return nil
	}
	if c.MetaStoreConfig.URI == "" {
		return errors.New("missing meta store uri configuration")
	}
	parsedURI, err := url.Parse(c.MetaStoreConfig.URI)
	if err != nil {
		return errors.Trace(err)
	}
	if !isSupportedScheme(parsedURI.Scheme) {
		return errors.Errorf("the %s scheme is not supported by meta store", parsedURI.Scheme)
	}
	return nil
}

// isSupportedScheme returns true if the scheme is compatible with MySQL.
func isSupportedScheme(scheme string) bool {
	return scheme == "mysql"
}
