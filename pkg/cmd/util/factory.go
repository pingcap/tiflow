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

package util

import (
	"crypto/tls"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

type Factory interface {
	ClientGetter
	EtcdClient() (*kv.CDCEtcdClient, error)
	PdClient() (pd.Client, error)
}

type ClientGetter interface {
	ToTLSConfig() (*tls.Config, error)
	ToGRPCDialOption() (grpc.DialOption, error)
	GetPdAddr() string
	GetCredential() *security.Credential
}

type ClientFlags struct {
	cliPdAddr string
	caPath    string
	certPath  string
	keyPath   string
}

var _ ClientGetter = &ClientFlags{}

func (c *ClientFlags) ToTLSConfig() (*tls.Config, error) {
	credential := c.GetCredential()
	tlsConfig, err := credential.ToTLSConfig()
	if err != nil {
		return nil, errors.Annotate(err, "fail to validate TLS settings")
	}
	return tlsConfig, nil
}

func (c *ClientFlags) ToGRPCDialOption() (grpc.DialOption, error) {
	credential := c.GetCredential()
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Annotate(err, "fail to validate TLS settings")
	}

	return grpcTLSOption, nil
}

func (c *ClientFlags) GetPdAddr() string {
	return c.cliPdAddr
}

func NewCredentialFlags() *ClientFlags {
	return &ClientFlags{}
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (c *ClientFlags) AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&c.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.PersistentFlags().StringVar(&c.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.PersistentFlags().StringVar(&c.keyPath, "key", "", "Private key path for TLS connection")
}

func (c *ClientFlags) GetCredential() *security.Credential {
	var certAllowedCN []string

	return &security.Credential{
		CAPath:        c.caPath,
		CertPath:      c.certPath,
		KeyPath:       c.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}
