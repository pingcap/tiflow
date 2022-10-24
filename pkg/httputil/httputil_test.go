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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

var httputilServerMsg = "this is httputil test server"

func TestHttputilNewClient(t *testing.T) {
	t.Parallel()

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
	server, addr := runServer(serverTLS)
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
	url := fmt.Sprintf("https://%s/", addr)
	resp, err := cli.Get(context.Background(), url)
	require.Nil(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, httputilServerMsg, string(body))
}

func TestStatusCodeCreated(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	server, addr := runServer(nil)
	defer func() {
		cancel()
		server.Close()
	}()
	cli, err := NewClient(nil)
	require.Nil(t, err)
	url := fmt.Sprintf("http://%s/create", addr)
	respBody, err := cli.DoRequest(ctx, url, http.MethodPost, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`"{"id": "value"}"`), respBody)
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	//nolint:errcheck
	w.Write([]byte(httputilServerMsg))
}

func createHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	//nolint:errcheck
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`"{"id": "value"}"`))
}

func runServer(tlsCfg *tls.Config) (*httptest.Server, string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)
	mux.HandleFunc("/create", createHandler)
	server := httptest.NewUnstartedServer(mux)
	addr := server.Listener.Addr().String()

	if tlsCfg != nil {
		server.TLS = tlsCfg
		go func() { server.StartTLS() }()
	} else {
		go func() { server.Start() }()
	}
	return server, addr
}
