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
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

var httputilServerMsg = "this is httputil test server"

func TestHttputilNewClient(t *testing.T) {
	t.Parallel()

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
	server, addr := runServer(http.HandlerFunc(handler), serverTLS)
	defer func() {
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

	server, addr := runServer(http.HandlerFunc(createHandler), nil)
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

func TestTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 500 * time.Millisecond

	server, addr := runServer(sleepHandler(time.Second), nil)
	defer func() {
		server.Close()
	}()
	cli, err := NewClient(nil)
	require.NoError(t, err)

	cli.SetTimeout(timeout)
	start := time.Now()
	resp, err := cli.Get(context.Background(), fmt.Sprintf("http://%s/", addr))
	if resp != nil && resp.Body != nil {
		require.NoError(t, resp.Body.Close())
	}
	var uErr *url.Error
	require.ErrorAs(t, err, &uErr)
	require.True(t, uErr.Timeout())
	require.GreaterOrEqual(t, time.Since(start), timeout)
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

func sleepHandler(d time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-time.After(d):
		case <-req.Context().Done():
		}
	}
}

func runServer(handler http.Handler, tlsCfg *tls.Config) (*httptest.Server, string) {
	server := httptest.NewUnstartedServer(handler)
	addr := server.Listener.Addr().String()

	if tlsCfg != nil {
		server.TLS = tlsCfg
		server.StartTLS()
	} else {
		server.Start()
	}
	return server, addr
}
