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

package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetCommonName(t *testing.T) {
	cd := &Credential{
		CAPath:   "../../tests/integration_tests/_certificates/ca.pem",
		CertPath: "../../tests/integration_tests/_certificates/server.pem",
		KeyPath:  "../../tests/integration_tests/_certificates/server-key.pem",
	}
	cn, err := cd.getSelfCommonName()
	require.Nil(t, err)
	require.Equal(t, "tidb-server", cn)

	cd.CertPath = "../../tests/integration_tests/_certificates/server-key.pem"
	_, err = cd.getSelfCommonName()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to decode PEM block to certificate")
}

// testAuthority is a throwaway CA used to check the client-certificate
// allowlist. Its CN is what an adversary tries to match through the chain.
type testAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newTestAuthority(t *testing.T, cn string) testAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testAuthority{cert: cert, key: key, der: der}
}

func (ca testAuthority) issue(t *testing.T, cn string, server bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der, ca.der}, PrivateKey: key}
}

func writeTestCert(t *testing.T, cert tls.Certificate, withKey bool) (certPath, keyPath string) {
	t.Helper()
	var err error
	certPath, err = WriteFile("security-test-cert",
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(certPath) })
	if !withKey {
		return certPath, ""
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	require.NoError(t, err)
	keyPath, err = WriteFile("security-test-key",
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(keyPath) })
	return certPath, keyPath
}

func (ca testAuthority) dial(t *testing.T, client tls.Certificate, serverTLS *tls.Config) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		if conn, err := ln.Accept(); err == nil {
			_, _ = conn.Write([]byte("ok"))
			_ = conn.Close()
		}
	}()

	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		RootCAs:      roots,
		Certificates: []tls.Certificate{client},
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	// TLS 1.3 finishes the client handshake before the server evaluates the
	// client certificate, so the rejection only shows up on the first read.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, 2)
	_, err = conn.Read(buf)
	return err
}

// TestCertAllowedCNOnlyMatchesLeaf certifies that a CN belonging to a CA in the
// verified chain cannot be used to bypass cert-allowed-cn. Only the leaf
// certificate's CN may satisfy the allowlist.
func TestCertAllowedCNOnlyMatchesLeaf(t *testing.T) {
	ca := newTestAuthority(t, "allowed-ca")
	server := ca.issue(t, "server", true)
	intruder := ca.issue(t, "intruder", false)
	allowedLeaf := ca.issue(t, "allowed-leaf", false)

	caPath, err := WriteFile("security-test-ca", ca.cert.Raw)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(caPath) })
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.der}), 0o600))
	serverCertPath, serverKeyPath := writeTestCert(t, server, true)

	newServerTLS := func(allowedCN ...string) *tls.Config {
		t.Helper()
		cfg, err := ToTLSConfigWithVerify(caPath, serverCertPath, serverKeyPath, allowedCN, true)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		return cfg
	}

	// The CA CN is on the allowlist, but the leaf CN is not: this must be
	// rejected, otherwise any certificate signed by that CA would pass.
	require.Error(t, ca.dial(t, intruder, newServerTLS("allowed-ca")))
	// A leaf CN on the allowlist is still accepted.
	require.NoError(t, ca.dial(t, allowedLeaf, newServerTLS("allowed-leaf")))
	require.NoError(t, ca.dial(t, allowedLeaf, newServerTLS("allowed-ca", "allowed-leaf")))
	// The CA CN alone does not admit other leaves either.
	require.Error(t, ca.dial(t, allowedLeaf, newServerTLS("allowed-ca")))
}
