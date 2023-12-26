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
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"encoding/json"
	"encoding/pem"
	"os"
	"strings"

	"github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Credential holds necessary path parameter to build a tls.Config
type Credential struct {
	CAPath        string   `toml:"ca-path" json:"ca-path"`
	CertPath      string   `toml:"cert-path" json:"cert-path"`
	KeyPath       string   `toml:"key-path" json:"key-path"`
	CertAllowedCN []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`

	// MTLS indicates whether use mTLS, by default it will affect all connections,
	// cludings:
	// 1) connections between TiCDC and TiKV
	// 2) connections between TiCDC and PD
	// 3) http server of TiCDC which is used for open API
	// 4) p2p server of TiCDC which is used sending messages between TiCDC nodes
	// Todo: just enable mTLS for 4) and 5) by default
	MTLS bool `toml:"mtls" json:"mtls"`
}

// Value implements the driver.Valuer interface
func (s Credential) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements the sql.Scanner interface
func (s *Credential) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, s)
}

// IsTLSEnabled checks whether TLS is enabled or not.
func (s *Credential) IsTLSEnabled() bool {
	return len(s.CAPath) != 0 && len(s.CertPath) != 0 && len(s.KeyPath) != 0
}

// IsEmpty checks whether Credential is empty or not.
func (s *Credential) IsEmpty() bool {
	return len(s.CAPath) == 0 && len(s.CertPath) == 0 && len(s.KeyPath) == 0
}

// PDSecurityOption creates a new pd SecurityOption from Security
func (s *Credential) PDSecurityOption() pd.SecurityOption {
	return pd.SecurityOption{
		CAPath:   s.CAPath,
		CertPath: s.CertPath,
		KeyPath:  s.KeyPath,
	}
}

// ToGRPCDialOption constructs a gRPC dial option.
func (s *Credential) ToGRPCDialOption() (grpc.DialOption, error) {
	tlsCfg, err := s.ToTLSConfig()
	if err != nil || tlsCfg == nil {
		return grpc.WithInsecure(), err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), nil
}

// ToTLSConfig generates tls's config from *Security
func (s *Credential) ToTLSConfig() (*tls.Config, error) {
	cfg, err := ToTLSConfigWithVerify(s.CAPath, s.CertPath, s.KeyPath, nil, s.MTLS)
	return cfg, errors.WrapError(errors.ErrToTLSConfigFailed, err)
}

// ToTLSConfigWithVerify generates tls's config from *Security and requires
// the remote common name to be verified.
func (s *Credential) ToTLSConfigWithVerify() (*tls.Config, error) {
	cfg, err := ToTLSConfigWithVerify(s.CAPath, s.CertPath, s.KeyPath, s.CertAllowedCN, s.MTLS)
	return cfg, errors.WrapError(errors.ErrToTLSConfigFailed, err)
}

func (s *Credential) getSelfCommonName() (string, error) {
	if s.CertPath == "" {
		return "", nil
	}
	data, err := os.ReadFile(s.CertPath)
	if err != nil {
		return "", errors.WrapError(errors.ErrToTLSConfigFailed, err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", errors.ErrToTLSConfigFailed.
			GenWithStack("failed to decode PEM block to certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", errors.WrapError(errors.ErrToTLSConfigFailed, err)
	}
	return certificate.Subject.CommonName, nil
}

// AddSelfCommonName add Common Name in certificate that specified by s.CertPath
// to s.CertAllowedCN
func (s *Credential) AddSelfCommonName() error {
	cn, err := s.getSelfCommonName()
	if err != nil {
		return err
	}
	if cn == "" {
		return nil
	}
	s.CertAllowedCN = append(s.CertAllowedCN, cn)
	return nil
}

// ToTLSConfigWithVerify constructs a `*tls.Config` from the CA, certification and key
// paths, and add verify for CN.
//
// If the CA path is empty, returns nil.
func ToTLSConfigWithVerify(
	caPath, certPath, keyPath string, verifyCN []string, mTLS bool,
) (*tls.Config, error) {
	if len(caPath) == 0 {
		return nil, nil
	}

	// Create a certificate pool from CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.Annotate(err, "could not read ca certificate")
	}

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, errors.New("failed to append ca certs")
	}

	tlsCfg := &tls.Config{
		RootCAs:    certPool,
		ClientCAs:  certPool,
		NextProtos: []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
		MinVersion: tls.VersionTLS12,
	}

	if mTLS {
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if len(certPath) != 0 && len(keyPath) != 0 {
		loadCert := func() (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return nil, errors.Annotate(err, "could not load client key pair")
			}
			return &cert, nil
		}
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return loadCert()
		}
		tlsCfg.GetCertificate = func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCert()
		}
	}

	addVerifyPeerCertificate(tlsCfg, verifyCN)
	return tlsCfg, nil
}

func addVerifyPeerCertificate(tlsCfg *tls.Config, verifyCN []string) {
	if len(verifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range verifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.VerifyPeerCertificate = func(
			rawCerts [][]byte, verifiedChains [][]*x509.Certificate,
		) error {
			cns := make([]string, 0, len(verifiedChains))
			for _, chains := range verifiedChains {
				for _, chain := range chains {
					cns = append(cns, chain.Subject.CommonName)
					if _, match := checkCN[chain.Subject.CommonName]; match {
						return nil
					}
				}
			}
			return errors.Errorf("client certificate authentication failed. "+
				"The Common Name from the client certificate %v was not found "+
				"in the configuration cluster-verify-cn with value: %s", cns, verifyCN)
		}
	}
}
