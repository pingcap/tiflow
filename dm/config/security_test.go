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
	"bytes"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	caFile        = "ca.pem"
	caFileContent = `
-----BEGIN CERTIFICATE-----
test no content
-----END CERTIFICATE-----
`
	certFile        = "cert.pem"
	certFileContent = `
-----BEGIN CERTIFICATE-----
test no content
-----END CERTIFICATE-----
`
	keyFile        = "key.pem"
	keyFileContent = `
-----BEGIN RSA PRIVATE KEY-----
test no content
-----END RSA PRIVATE KEY-----
`
)

var (
	caFilePath   string
	certFilePath string
	keyFilePath  string
)

func createTestFixture(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	caFilePath = path.Join(dir, caFile)
	err := os.WriteFile(caFilePath, []byte(caFileContent), 0o644)
	require.NoError(t, err)

	certFilePath = path.Join(dir, certFile)
	err = os.WriteFile(certFilePath, []byte(certFileContent), 0o644)
	require.NoError(t, err)

	keyFilePath = path.Join(dir, keyFile)
	err = os.WriteFile(keyFilePath, []byte(keyFileContent), 0o644)
	require.NoError(t, err)
}

func TestPessimistSuite(t *testing.T) {
	suite.Run(t, new(testTLSConfig))
}

type testTLSConfig struct {
	suite.Suite

	noContent []byte
}

func (c *testTLSConfig) SetupSuite() {
	createTestFixture(c.T())
	c.noContent = []byte("test no content")
}

func (c *testTLSConfig) TestLoadAndClearContent() {
	s := &Security{
		SSLCA:   caFilePath,
		SSLCert: certFilePath,
		SSLKey:  keyFilePath,
	}
	err := s.LoadTLSContent()
	c.Require().NoError(err)
	c.Require().Greater(len(s.SSLCABytes), 0)
	c.Require().Greater(len(s.SSLCertBytes), 0)
	c.Require().Greater(len(s.SSLKeyBytes), 0)

	c.Require().True(bytes.Contains(s.SSLCABytes, c.noContent))
	c.Require().True(bytes.Contains(s.SSLCertBytes, c.noContent))
	c.Require().True(bytes.Contains(s.SSLKeyBytes, c.noContent))

	s.ClearSSLBytesData()
	c.Require().Len(s.SSLCABytes, 0)
	c.Require().Len(s.SSLCertBytes, 0)
	c.Require().Len(s.SSLKeyBytes, 0)

	s.SSLCABase64 = "MTIz"
	err = s.LoadTLSContent()
	c.Require().NoError(err)
	c.Require().Greater(len(s.SSLCABytes), 0)
}

func (c *testTLSConfig) TestTLSTaskConfig() {
	taskRowStr := fmt.Sprintf(`---
name: test
task-mode: all
target-database:
    host: "127.0.0.1"
    port: 3307
    user: "root"
    password: "123456"
    security:
      ssl-ca: %s
      ssl-cert: %s
      ssl-key: %s
block-allow-list:
  instance:
    do-dbs: ["dm_benchmark"]
mysql-instances:
  - source-id: "mysql-replica-01-tls"
    block-allow-list: "instance"
`, caFilePath, certFilePath, keyFilePath)
	task1 := NewTaskConfig()
	err := task1.RawDecode(taskRowStr)
	c.Require().NoError(err)
	c.Require().NoError(task1.TargetDB.Security.LoadTLSContent())
	// test load tls content
	c.Require().True(bytes.Contains(task1.TargetDB.Security.SSLCABytes, c.noContent))
	c.Require().True(bytes.Contains(task1.TargetDB.Security.SSLCertBytes, c.noContent))
	c.Require().True(bytes.Contains(task1.TargetDB.Security.SSLKeyBytes, c.noContent))

	// test after to string, taskStr can be `Decode` normally
	taskStr := task1.String()
	task2 := NewTaskConfig()
	err = task2.Decode(taskStr)
	c.Require().NoError(err)
	c.Require().True(bytes.Contains(task2.TargetDB.Security.SSLCABytes, c.noContent))
	c.Require().True(bytes.Contains(task2.TargetDB.Security.SSLCertBytes, c.noContent))
	c.Require().True(bytes.Contains(task2.TargetDB.Security.SSLKeyBytes, c.noContent))
	c.Require().NoError(task2.adjust())
}

func (c *testTLSConfig) TestClone() {
	s := &Security{
		SSLCA:         "a",
		SSLCert:       "b",
		SSLKey:        "c",
		CertAllowedCN: []string{"d"},
		SSLCABytes:    nil,
		SSLKeyBytes:   []byte("e"),
		SSLCertBytes:  []byte("f"),
	}
	// When add new fields, also update this value
	// TODO: check it
	c.Require().Equal(10, reflect.TypeOf(*s).NumField())
	clone := s.Clone()
	c.Require().Equal(s, clone)
	clone.CertAllowedCN[0] = "g"
	c.Require().NotEqual(s, clone)
}

func (c *testTLSConfig) TestLoadDumpTLSContent() {
	s := &Security{
		SSLCA:   caFilePath,
		SSLCert: certFilePath,
		SSLKey:  keyFilePath,
	}
	err := s.LoadTLSContent()
	c.Require().NoError(err)
	c.Require().Greater(len(s.SSLCABytes), 0)
	c.Require().Greater(len(s.SSLCertBytes), 0)
	c.Require().Greater(len(s.SSLKeyBytes), 0)

	// cert file not exist
	s.SSLCA += ".new"
	s.SSLCert += ".new"
	s.SSLKey += ".new"
	c.Require().NoError(s.DumpTLSContent(c.T().TempDir()))
	c.Require().FileExists(s.SSLCA)
	c.Require().FileExists(s.SSLCert)
	c.Require().FileExists(s.SSLKey)

	// user not specify cert file
	s.SSLCA = ""
	s.SSLCert = ""
	s.SSLKey = ""
	c.Require().NoError(s.DumpTLSContent(c.T().TempDir()))
	c.Require().FileExists(s.SSLCA)
	c.Require().FileExists(s.SSLCert)
	c.Require().FileExists(s.SSLKey)
}
