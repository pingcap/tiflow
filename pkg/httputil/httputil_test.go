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
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/util"
)

var httputilServerMsg = "this is httputil test server"

func TestHttputilNewClient(t *testing.T) {
	t.Parallel()

	port := 8303
	_, cancel := context.WithCancel(context.Background())

	dir, err := os.Getwd()
	require.Nil(t, err)
	certDir := "_certificates"
	serverTLS, err := util.ToTLSConfigWithVerify(
		filepath.Join(dir, certDir, "ca.pem"),
		filepath.Join(dir, certDir, "server.pem"),
		filepath.Join(dir, certDir, "server-key.pem"),
		[]string{},
	)
	require.Nil(t, err)
	server := runServer(serverTLS, port, t)
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
	require.Nil(t, err)
	url := fmt.Sprintf("https://127.0.0.1:%d/", port)
	resp, err := cli.Get(context.Background(), url)
	require.Nil(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, httputilServerMsg, string(body))
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	//nolint:errcheck
	w.Write([]byte(httputilServerMsg))
}

func runServer(tlsCfg *tls.Config, port int, t *testing.T) *http.Server {
	http.HandleFunc("/", handler)
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: nil}

	conn, err := net.Listen("tcp", server.Addr)
	if err != nil {
		require.Nil(t, err)
	}

	tlsListener := tls.NewListener(conn, tlsCfg)
	go func() {
		//nolint:errcheck
		server.Serve(tlsListener)
	}()
	return server
}
