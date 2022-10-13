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

package ha

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

const (
	sourceSampleFile  = "source.yaml"
	sourceFileContent = `---
server-id: 101
source-id: mysql-replica-01
relay-dir: ./relay_log
enable-gtid: true
relay-binlog-gtid: "e68f6068-53ec-11eb-9c5f-0242ac110003:1-50"
from:
  host: 127.0.0.1
  user: root
  password: Up8156jArvIPymkVC+5LxkAT6rek
  port: 3306
  max-allowed-packet: 0
  security:
    ssl-ca: "%s"
    ssl-cert: "%s"
    ssl-key: "%s"
`
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
	sourceSampleFilePath string
	caFilePath           string
	certFilePath         string
	keyFilePath          string

	etcdTestCli *clientv3.Client
)

func TestHA(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	c.Assert(ClearTestInfoOperation(etcdTestCli), IsNil)
}

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) SetUpTest(c *C) {
	createTestFixture(c)
}

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

	sourceSampleFilePath = path.Join(dir, sourceSampleFile)
	sourceFileContent := fmt.Sprintf(sourceFileContent, caFilePath, certFilePath, keyFilePath)
	err = os.WriteFile(sourceSampleFilePath, []byte(sourceFileContent), 0o644)
	c.Assert(err, IsNil)
}

func (t *testForEtcd) TestSourceEtcd(c *C) {
	defer clearTestInfoOperation(c)

	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	c.Assert(err, IsNil)
	source := cfg.SourceID
	cfgExtra := *cfg
	cfgExtra.SourceID = "mysql-replica-2"

	// no source config exist.
	scm1, rev1, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(scm1, HasLen, 0)
	cfgM, _, err := GetSourceCfg(etcdTestCli, "", 0)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 0)

	// put a source config.
	c.Assert(cfg.From.Security.LoadTLSContent(), IsNil)
	rev2, err := PutSourceCfg(etcdTestCli, cfg)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get the config back.
	scm2, rev3, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	cfg2 := scm2[source]
	c.Assert(cfg2, DeepEquals, cfg)
	noContentBytes := []byte("test no content")
	c.Assert(bytes.Contains(cfg2.From.Security.SSLCABytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(cfg2.From.Security.SSLKeyBytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(cfg2.From.Security.SSLCertBytes, noContentBytes), Equals, true)
	// put another source config.
	rev2, err = PutSourceCfg(etcdTestCli, &cfgExtra)
	c.Assert(err, IsNil)

	// get all two config.
	cfgM, rev3, err = GetSourceCfg(etcdTestCli, "", 0)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(cfgM, HasLen, 2)
	c.Assert(cfgM[source], DeepEquals, cfg)
	c.Assert(cfgM[cfgExtra.SourceID], DeepEquals, &cfgExtra)

	// delete the config.
	deleteOp := deleteSourceCfgOp(source)
	deleteResp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	scm3, rev4, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, deleteResp.Header.Revision)
	c.Assert(scm3, HasLen, 0)
}
