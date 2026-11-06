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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const validDMResponse = `{"result":true,"cfgs":["source-id = \"test\"\n[from]\nhost = \"127.0.0.1\"\n[to]\nhost = \"127.0.0.1\"\n"]}`

func TestDMTLSConfigurationRequiresHTTPS(t *testing.T) {
	_, err := getDMTaskCfgWithSecurity("http://127.0.0.1:8261", "test", &Security{})
	require.ErrorContains(t, err, "requires an https:// dm-addr")
}

func TestDMRejectsIncompleteClientCertificate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		security Security
		want     string
	}{
		{"certificate path", Security{CertPath: "client.pem"}, "paths must be set together"},
		{"key path", Security{KeyPath: "client.key"}, "paths must be set together"},
		{"certificate content", Security{CertBytes: "certificate"}, "contents must be set together"},
		{"key content", Security{KeyBytes: "key"}, "contents must be set together"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := getDMTaskCfgWithSecurity("https://127.0.0.1:1", "test", &tc.security)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDMTLSRejectsRedirects(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect destination must not receive a request")
	}))
	defer destination.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	_, err := getDMTaskCfgWithSecurity(server.URL, "test", &Security{CABytes: string(ca)})
	require.ErrorContains(t, err, "redirects are not allowed")
}

func TestDMCustomCAAndClientCertificate(t *testing.T) {
	ca, err := os.ReadFile("../../dm/tests/tls/conf/ca.pem")
	require.NoError(t, err)
	cert, err := tls.LoadX509KeyPair("../../dm/tests/tls/conf/dm.pem", "../../dm/tests/tls/conf/dm.key")
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.TLS.PeerCertificates)
		_, _ = io.WriteString(w, validDMResponse)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	defer server.Close()
	security := &Security{CABytes: string(ca), CertPath: "../../dm/tests/tls/conf/dm.pem", KeyPath: "../../dm/tests/tls/conf/dm.key"}
	cfgs, err := getDMTaskCfgWithSecurity(server.URL, "test", security)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	_, err = getDMTaskCfgWithSecurity(server.URL, "test", &Security{CABytes: string(ca)})
	require.Error(t, err, "missing client certificate must be rejected")
	_, err = getDMTaskCfg(server.URL, "test")
	require.Error(t, err, "an untrusted server must be rejected")
}
