// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/openapi"
)

// SourceCfgToOpenAPISource converter SourceConfig to openapi.Source.
func SourceCfgToOpenAPISource(cfg *SourceConfig) openapi.Source {
	source := openapi.Source{
		Enable:     cfg.Enable,
		EnableGtid: cfg.EnableGTID,
		Host:       cfg.From.Host,
		Password:   &ObfuscatedPasswordForFeedback, // PM's requirement, we always return obfuscated password to users
		Port:       cfg.From.Port,
		SourceName: cfg.SourceID,
		User:       cfg.From.User,
		Purge: &openapi.Purge{
			Expires:     &cfg.Purge.Expires,
			Interval:    &cfg.Purge.Interval,
			RemainSpace: &cfg.Purge.RemainSpace,
		},
		RelayConfig: &openapi.RelayConfig{
			EnableRelay:     &cfg.EnableRelay,
			RelayBinlogGtid: &cfg.RelayBinlogGTID,
			RelayBinlogName: &cfg.RelayBinLogName,
			RelayDir:        &cfg.RelayDir,
		},
	}
	if cfg.Flavor != "" {
		source.Flavor = &cfg.Flavor
	}
	if cfg.From.Security != nil {
		// NOTE we don't return security content here, because we don't want to expose it to the user.
		var certAllowedCn []string
		certAllowedCn = append(certAllowedCn, cfg.From.Security.CertAllowedCN...)
		source.Security = &openapi.Security{CertAllowedCn: &certAllowedCn}
	}
	return source
}

// OpenAPISourceToSourceCfg converter openapi.Source to SourceConfig.
func OpenAPISourceToSourceCfg(source openapi.Source) *SourceConfig {
	cfg := NewSourceConfig()
	from := dbconfig.DBConfig{
		Host: source.Host,
		Port: source.Port,
		User: source.User,
	}
	if source.Password != nil {
		from.Password = *source.Password
	}
	if source.Security != nil {
		from.Security = &security.Security{
			SSLCABytes:   []byte(source.Security.SslCaContent),
			SSLKeyBytes:  []byte(source.Security.SslKeyContent),
			SSLCertBytes: []byte(source.Security.SslCertContent),
		}
		if source.Security.CertAllowedCn != nil {
			from.Security.CertAllowedCN = *source.Security.CertAllowedCn
		}
	}
	cfg.From = from
	if source.Flavor != nil {
		cfg.Flavor = *source.Flavor
	}
	cfg.Enable = source.Enable
	cfg.EnableGTID = source.EnableGtid
	cfg.SourceID = source.SourceName
	if purge := source.Purge; purge != nil {
		if purge.Expires != nil {
			cfg.Purge.Expires = *purge.Expires
		}
		if purge.Interval != nil {
			cfg.Purge.Interval = *purge.Interval
		}
		if purge.RemainSpace != nil {
			cfg.Purge.RemainSpace = *purge.RemainSpace
		}
	}
	if relayConfig := source.RelayConfig; relayConfig != nil {
		if relayConfig.EnableRelay != nil {
			cfg.EnableRelay = *relayConfig.EnableRelay
		}
		if relayConfig.RelayBinlogGtid != nil {
			cfg.RelayBinlogGTID = *relayConfig.RelayBinlogGtid
		}
		if relayConfig.RelayBinlogName != nil {
			cfg.RelayBinLogName = *relayConfig.RelayBinlogName
		}
		if relayConfig.RelayDir != nil {
			cfg.RelayDir = *relayConfig.RelayDir
		}
	}
	return cfg
}
