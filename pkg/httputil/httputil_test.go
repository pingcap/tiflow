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

package httputil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type httputilSuite struct{}

var _ = check.Suite(&httputilSuite{})

var httputilServerMsg = "this is httputil test server"

func (s *httputilSuite) TestHttputilNewClient(c *check.C) {
	defer testleak.AfterTest(c)()
	port := 8303
	_, cancel := context.WithCancel(context.Background())

	dir, err := os.Getwd()
	c.Assert(err, check.IsNil)
	certDir := "_certificates"
	serverTLS, err := utils.ToTLSConfigWithVerify(
		filepath.Join(dir, certDir, "ca.pem"),
		filepath.Join(dir, certDir, "server.pem"),
		filepath.Join(dir, certDir, "server-key.pem"),
		[]string{},
	)
	c.Assert(err, check.IsNil)
	server := runServer(serverTLS, port, c)
	defer func() {
		cancel()
		server.Close()
	}()
	credential := &security.Credential{
		CAPath:        filepath.Join(dir, certDir, "ca.pem"),
		CertPath:      filepath.Join(dir, certDir, "client.pem"),
		KeyPath:       filepath.Join(dir, certDir, "client-key.pem"),
		CertAllowedCN: []string{},
	}
	cli, err := NewClient(credential)
	c.Assert(err, check.IsNil)
	url := fmt.Sprintf("https://127.0.0.1:%d/", port)
	resp, err := cli.Get(url)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	c.Assert(string(body), check.Equals, httputilServerMsg)
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	//nolint:errcheck
	w.Write([]byte(httputilServerMsg))
}

func runServer(tlsCfg *tls.Config, port int, c *check.C) *http.Server {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		c.Assert(err, check.IsNil)
	}

	tlsListener := tls.NewListener(conn, tlsCfg)
	go func() {
		//nolint:errcheck
		server.Serve(tlsListener)
	}()
	return server
}
