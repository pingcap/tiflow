// Copyright 2020 PingCAP, Inc.
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
	"encoding/base64"
	"fmt"
	"os"
	"path"

	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// Security config.
type Security struct {
	SSLCA         string   `toml:"ssl-ca" json:"ssl-ca" yaml:"ssl-ca"`
	SSLCert       string   `toml:"ssl-cert" json:"ssl-cert" yaml:"ssl-cert"`
	SSLKey        string   `toml:"ssl-key" json:"ssl-key" yaml:"ssl-key"`
	CertAllowedCN strArray `toml:"cert-allowed-cn" json:"cert-allowed-cn" yaml:"cert-allowed-cn"`
	SSLCABytes    []byte   `toml:"ssl-ca-bytes" json:"-" yaml:"ssl-ca-bytes"`
	SSLKeyBytes   []byte   `toml:"ssl-key-bytes" json:"-" yaml:"ssl-key-bytes"`
	SSLCertBytes  []byte   `toml:"ssl-cert-bytes" json:"-" yaml:"ssl-cert-bytes"`
	SSLCABase64   string   `toml:"ssl-ca-base64" json:"-" yaml:"ssl-ca-base64"`
	SSLKeyBase64  string   `toml:"ssl-key-base64" json:"-" yaml:"ssl-key-base64"`
	SSLCertBase64 string   `toml:"ssl-cert-base64" json:"-" yaml:"ssl-cert-base64"`
}

// used for parse string slice in flag.
type strArray []string

func (i *strArray) String() string {
	return fmt.Sprint([]string(*i))
}

func (i *strArray) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// LoadTLSContent load all tls config from file or base64 fields.
func (s *Security) LoadTLSContent() error {
	var firstErr error
	convertAndAssign := func(source string, convert func(string) ([]byte, error), target *[]byte) {
		if firstErr != nil {
			return
		}
		// already loaded. And DM does not support certificate rotation.
		if len(*target) > 0 {
			return
		}

		if source == "" {
			return
		}

		dat, err := convert(source)
		if err != nil {
			firstErr = err
			return
		}
		*target = dat
	}

	convertAndAssign(s.SSLCABase64, base64.StdEncoding.DecodeString, &s.SSLCABytes)
	convertAndAssign(s.SSLKeyBase64, base64.StdEncoding.DecodeString, &s.SSLKeyBytes)
	convertAndAssign(s.SSLCertBase64, base64.StdEncoding.DecodeString, &s.SSLCertBytes)
	convertAndAssign(s.SSLCA, os.ReadFile, &s.SSLCABytes)
	convertAndAssign(s.SSLKey, os.ReadFile, &s.SSLKeyBytes)
	convertAndAssign(s.SSLCert, os.ReadFile, &s.SSLCertBytes)
	return firstErr
}

// DumpTLSContent dump tls certs data to file.
// if user specified the path for certs but the cert doesn't exist or user didn't specify the path for certs
// dump certs to dm-worker folder and change the cert path.
// see more here https://github.com/pingcap/tiflow/pull/3260#discussion_r749052994
func (s *Security) DumpTLSContent(baseDirPath string) error {
	isSSLCANotExist := s.SSLCA == "" || !utils.IsFileExists(s.SSLCA)
	isSSLCertNotExist := s.SSLCert == "" || !utils.IsFileExists(s.SSLCert)
	isSSLKeyNotExist := s.SSLKey == "" || !utils.IsFileExists(s.SSLKey)
	if isSSLCANotExist || isSSLCertNotExist || isSSLKeyNotExist {
		if !utils.IsDirExists(baseDirPath) {
			if err := os.MkdirAll(baseDirPath, 0o700); err != nil {
				return err
			}
		}
	}
	if isSSLCANotExist {
		s.SSLCA = path.Join(baseDirPath, "ca.pem")
		if err := utils.WriteFileAtomic(s.SSLCA, s.SSLCABytes, 0o600); err != nil {
			return err
		}
	}
	if isSSLCertNotExist {
		s.SSLCert = path.Join(baseDirPath, "cert.pem")
		if err := utils.WriteFileAtomic(s.SSLCert, s.SSLCertBytes, 0o600); err != nil {
			return err
		}
	}
	if isSSLKeyNotExist {
		s.SSLKey = path.Join(baseDirPath, "key.pem")
		if err := utils.WriteFileAtomic(s.SSLKey, s.SSLKeyBytes, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// ClearSSLBytesData clear all tls config bytes data.
func (s *Security) ClearSSLBytesData() {
	s.SSLCABytes = s.SSLCABytes[:0]
	s.SSLKeyBytes = s.SSLKeyBytes[:0]
	s.SSLCertBytes = s.SSLCertBytes[:0]
}

// Clone returns a deep copy of Security.
func (s *Security) Clone() *Security {
	if s == nil {
		return nil
	}
	clone := *s
	clone.CertAllowedCN = append(strArray(nil), s.CertAllowedCN...)
	clone.SSLCABytes = append([]byte(nil), s.SSLCABytes...)
	clone.SSLKeyBytes = append([]byte(nil), s.SSLKeyBytes...)
	clone.SSLCertBytes = append([]byte(nil), s.SSLCertBytes...)
	return &clone
}
