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

package security

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"time"
)

// WriteFile write content to a temp file
func WriteFile(fileName string, content []byte) (path string, err error) {
	cert, err := os.CreateTemp("", fileName)
	if err != nil {
		return "", err
	}
	_, err = cert.Write(content)
	return cert.Name(), err
}

// NewServerCredential4Test return a Credential for testing
func NewServerCredential4Test(cn string) (*CA, *Credential, error) {
	var caPath, certPath, keyPath string

	ca, err := NewCA()
	if err != nil {
		return nil, nil, err
	}
	if caPath, err = WriteFile("ticdc-test-ca", ca.CAPEM); err != nil {
		return nil, nil, err
	}

	certPEM, keyPEM, err := ca.GenerateCerts(cn)
	if err != nil {
		return nil, nil, err
	}
	if certPath, err = WriteFile("ticdc-test-cert", certPEM); err != nil {
		return nil, nil, err
	}
	if keyPath, err = WriteFile("ticdc-test-key", keyPEM); err != nil {
		return nil, nil, err
	}

	res := &Credential{
		CAPath:   caPath,
		CertPath: certPath,
		KeyPath:  keyPath,
	}
	if cn != "" {
		res.CertAllowedCN = append(res.CertAllowedCN, cn)
	}
	return ca, res, nil
}

// CA represents a certificate authority
type CA struct {
	privateKey *rsa.PrivateKey

	Cert  *x509.Certificate
	CAPEM []byte
}

// NewCA create a new CA
func NewCA() (*CA, error) {
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	caCert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization: []string{"test"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(10, 0, 0),
		IsCA:      true,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// self-signed certificate
	caBytes, err := x509.CreateCertificate(rand.Reader, caCert, caCert,
		&caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	caPem := new(bytes.Buffer)
	err = pem.Encode(caPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	if err != nil {
		return nil, err
	}
	return &CA{
		privateKey: caPrivKey,
		Cert:       caCert,
		CAPEM:      caPem.Bytes(),
	}, err
}

// GetPrivKeyPEM returns the PEM contents of the private key.
func (ca *CA) GetPrivKeyPEM() ([]byte, error) {
	caPrivKeyPEM := new(bytes.Buffer)
	err := pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(ca.privateKey),
	})
	if err != nil {
		return nil, err
	}
	return caPrivKeyPEM.Bytes(), nil
}

// GenerateCerts returns the PEM contents of a CA certificate and some certificates
// and private keys per Common Name in commonNames.
// thanks to https://shaneutt.com/blog/golang-ca-and-signed-cert-go/.
func (ca *CA) GenerateCerts(commonName string) (certPEM, KeyPEM []byte, err error) {
	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1658),
		Subject: pkix.Name{
			Organization: []string{"test"},
			CommonName:   commonName,
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// signing the certificate with CA
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca.Cert,
		&certPrivKey.PublicKey, ca.privateKey)
	if err != nil {
		return nil, nil, err
	}

	certPEMBuffer := new(bytes.Buffer)
	err = pem.Encode(certPEMBuffer, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	if err != nil {
		return nil, nil, err
	}

	keyPEMBuffer := new(bytes.Buffer)
	err = pem.Encode(keyPEMBuffer, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})
	if err != nil {
		return nil, nil, err
	}

	return certPEMBuffer.Bytes(), keyPEMBuffer.Bytes(), nil
}
