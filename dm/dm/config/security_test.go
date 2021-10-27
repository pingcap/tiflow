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

	. "github.com/pingcap/check"
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

func createTestFixture(c *C) {
	dir := c.MkDir()

	caFilePath = path.Join(dir, caFile)
	err := os.WriteFile(caFilePath, []byte(caFileContent), 0o644)
	c.Assert(err, IsNil)

	certFilePath = path.Join(dir, certFile)
	err = os.WriteFile(certFilePath, []byte(certFileContent), 0o644)
	c.Assert(err, IsNil)

	keyFilePath = path.Join(dir, keyFile)
	err = os.WriteFile(keyFilePath, []byte(keyFileContent), 0o644)
	c.Assert(err, IsNil)
}

type testTLSConfig struct{}

var _ = Suite(&testTLSConfig{})

func (t *testTLSConfig) SetUpTest(c *C) {
	createTestFixture(c)
}

func (t *testTLSConfig) TestLoadAndClearContent(c *C) {
	s := &Security{
		SSLCA:   caFilePath,
		SSLCert: certFilePath,
		SSLKey:  keyFilePath,
	}
	err := s.LoadTLSContent()
	c.Assert(err, IsNil)
	c.Assert(len(s.SSLCABytes) > 0, Equals, true)
	c.Assert(len(s.SSLCertBytes) > 0, Equals, true)
	c.Assert(len(s.SSLKEYBytes) > 0, Equals, true)

	noContentBytes := []byte("test no content")

	c.Assert(bytes.Contains(s.SSLCABytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(s.SSLKEYBytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(s.SSLCertBytes, noContentBytes), Equals, true)

	s.ClearSSLBytesData()
	c.Assert(s.SSLCABytes, HasLen, 0)
	c.Assert(s.SSLCertBytes, HasLen, 0)
	c.Assert(s.SSLKEYBytes, HasLen, 0)
}

func (t *testTLSConfig) TestTLSTaskConfig(c *C) {
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
	c.Assert(err, IsNil)
	c.Assert(task1.TargetDB.Security.LoadTLSContent(), IsNil)
	// test load tls content
	noContentBytes := []byte("test no content")
	c.Assert(bytes.Contains(task1.TargetDB.Security.SSLCABytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(task1.TargetDB.Security.SSLKEYBytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(task1.TargetDB.Security.SSLCertBytes, noContentBytes), Equals, true)

	// test after to string, taskStr can be `Decode` normally
	taskStr := task1.String()
	task2 := NewTaskConfig()
	err = task2.Decode(taskStr)
	c.Assert(err, IsNil)
	c.Assert(bytes.Contains(task2.TargetDB.Security.SSLCABytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(task2.TargetDB.Security.SSLKEYBytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(task2.TargetDB.Security.SSLCertBytes, noContentBytes), Equals, true)
	c.Assert(task2.adjust(), IsNil)
}

func (t *testTLSConfig) TestClone(c *C) {
	s := &Security{
		SSLCA:         "a",
		SSLCert:       "b",
		SSLKey:        "c",
		CertAllowedCN: []string{"d"},
		SSLCABytes:    nil,
		SSLKEYBytes:   []byte("e"),
		SSLCertBytes:  []byte("f"),
	}
	// When add new fields, also update this value
	c.Assert(reflect.Indirect(reflect.ValueOf(s)).NumField(), Equals, 7)
	clone := s.Clone()
	c.Assert(clone, DeepEquals, s)
	clone.CertAllowedCN[0] = "g"
	c.Assert(clone, Not(DeepEquals), s)
}
