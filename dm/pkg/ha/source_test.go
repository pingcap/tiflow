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

	"github.com/pingcap/tiflow/dm/config"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

	suite.Run(t, new(testForEtcd))
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(t require.TestingT) {
	require.NoError(t, ClearTestInfoOperation(etcdTestCli))
}

type testForEtcd struct {
	suite.Suite
}

func (s *testForEtcd) SetupTest() {
	createTestFixture(s.T())
}

func createTestFixture(t *testing.T) {
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

	sourceSampleFilePath = path.Join(dir, sourceSampleFile)
	sourceFileContent := fmt.Sprintf(sourceFileContent, caFilePath, certFilePath, keyFilePath)
	err = os.WriteFile(sourceSampleFilePath, []byte(sourceFileContent), 0o644)
	require.NoError(t, err)
}

func (s *testForEtcd) TestSourceEtcd() {
	defer clearTestInfoOperation(s.T())

	cfg, err := config.LoadFromFile(sourceSampleFilePath)
	s.Require().NoError(err)
	source := cfg.SourceID
	cfgExtra := *cfg
	cfgExtra.SourceID = "mysql-replica-2"

	// no source config exist.
	scm1, rev1, err := GetSourceCfg(etcdTestCli, source, 0)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Len(scm1, 0)
	cfgM, _, err := GetSourceCfg(etcdTestCli, "", 0)
	s.Require().NoError(err)
	s.Require().Len(cfgM, 0)

	// put a source config.
	s.Require().NoError(cfg.From.Security.LoadTLSContent())
	rev2, err := PutSourceCfg(etcdTestCli, cfg)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get the config back.
	scm2, rev3, err := GetSourceCfg(etcdTestCli, source, 0)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	cfg2 := scm2[source]
	s.Require().Equal(cfg, cfg2)
	noContentBytes := []byte("test no content")
	s.Require().Equal(true, bytes.Contains(cfg2.From.Security.SSLCABytes, noContentBytes))
	s.Require().Equal(true, bytes.Contains(cfg2.From.Security.SSLKeyBytes, noContentBytes))
	s.Require().Equal(true, bytes.Contains(cfg2.From.Security.SSLCertBytes, noContentBytes))
	// put another source config.
	rev2, err = PutSourceCfg(etcdTestCli, &cfgExtra)
	s.Require().NoError(err)

	// get all two config.
	cfgM, rev3, err = GetSourceCfg(etcdTestCli, "", 0)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(cfgM, 2)
	s.Require().Equal(cfg, cfgM[source])
	s.Require().Equal(&cfgExtra, cfgM[cfgExtra.SourceID])

	// delete the config.
	deleteOp := deleteSourceCfgOp(source)
	deleteResp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	s.Require().NoError(err)

	// get again, not exists now.
	scm3, rev4, err := GetSourceCfg(etcdTestCli, source, 0)
	s.Require().NoError(err)
	s.Require().Equal(deleteResp.Header.Revision, rev4)
	s.Require().Len(scm3, 0)
}
