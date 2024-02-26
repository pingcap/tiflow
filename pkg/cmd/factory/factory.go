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

package factory

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
	"golang.org/x/term"
	"google.golang.org/grpc"
)

const (
	envVarTiCDCUser              = "TICDC_USER"
	envVarTiCDCPassword          = "TICDC_PASSWORD"
	defaultCrendentialConfigFile = ".ticdc/credentials"
)

// Factory defines the client-side construction factory.
type Factory interface {
	ClientGetter
	EtcdClient() (*etcd.CDCEtcdClientImpl, error)
	PdClient() (pd.Client, error)
	APIV2Client() (apiv2client.APIV2Interface, error)
}

// ClientGetter defines the client getter.
type ClientGetter interface {
	ToTLSConfig() (*tls.Config, error)
	ToGRPCDialOption() (grpc.DialOption, error)
	GetPdAddr() string
	GetServerAddr() string
	GetLogLevel() string
	GetCredential() *security.Credential
	GetAuthParameters() url.Values
}

// ClientAuth specifies the authentication parameters.
type ClientAuth struct {
	User     string `toml:"ticdc_user,omitempty"`
	Password string `toml:"ticdc_password,omitempty"`
}

// ClientFlags specifies the parameters needed to construct the client.
type ClientFlags struct {
	ClientAuth
	pdAddr     string
	serverAddr string
	logLevel   string
	caPath     string
	certPath   string
	keyPath    string
}

var _ ClientGetter = &ClientFlags{}

// ToTLSConfig returns the configuration of tls.
func (c *ClientFlags) ToTLSConfig() (*tls.Config, error) {
	credential := c.GetCredential()
	tlsConfig, err := credential.ToTLSConfig()
	if err != nil {
		return nil, errors.Annotate(err, "fail to validate TLS settings")
	}
	return tlsConfig, nil
}

// ToGRPCDialOption returns the option of GRPC dial.
func (c *ClientFlags) ToGRPCDialOption() (grpc.DialOption, error) {
	credential := c.GetCredential()
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Annotate(err, "fail to validate TLS settings")
	}

	return grpcTLSOption, nil
}

// GetPdAddr returns pd address.
func (c *ClientFlags) GetPdAddr() string {
	return c.pdAddr
}

// GetLogLevel returns log level.
func (c *ClientFlags) GetLogLevel() string {
	return c.logLevel
}

// GetServerAddr returns cdc cluster id.
func (c *ClientFlags) GetServerAddr() string {
	return c.serverAddr
}

// NewClientFlags creates new client flags.
func NewClientFlags() *ClientFlags {
	return &ClientFlags{}
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (c *ClientFlags) AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&c.serverAddr, "server",
		"", "CDC server address")
	cmd.PersistentFlags().StringVar(&c.pdAddr, "pd", "",
		"PD address, use ',' to separate multiple PDs, "+
			"Parameter --pd is deprecated, please use parameter --server instead.")
	cmd.PersistentFlags().StringVar(&c.caPath, "ca", "",
		"CA certificate path for TLS connection to CDC server")
	cmd.PersistentFlags().StringVar(&c.certPath, "cert", "",
		"Certificate path for TLS connection to CDC server")
	cmd.PersistentFlags().StringVar(&c.keyPath, "key", "",
		"Private key path for TLS connection to CDC server")
	cmd.PersistentFlags().StringVar(&c.logLevel, "log-level", "warn",
		"log level (etc: debug|info|warn|error)")

	cmd.PersistentFlags().StringVar(&c.User, "user", "", "User name for authentication. "+
		"You can sqpecify it via environment variable TICDC_USER")
	cmd.PersistentFlags().StringVar(&c.Password, "password", "", "Password for authentication. "+
		"You can specify it via environment variable TICDC_PASSWORD")
}

// GetCredential returns credential.
func (c *ClientFlags) GetCredential() *security.Credential {
	var certAllowedCN []string

	return &security.Credential{
		CAPath:        c.caPath,
		CertPath:      c.certPath,
		KeyPath:       c.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// CompleteAuthParameters completes the authentication parameters.
func (c *ClientFlags) CompleteAuthParameters(cmd *cobra.Command) (err error) {
	authType := "command line"
	defer func() {
		if err == nil {
			if c.User == "" && c.Password != "" {
				err = errors.ErrCredentialNotFound.GenWithStackByArgs("invalid atuhentication: password is specified without user")
			}
		}
		log.Info(fmt.Sprintf("cli authentication type: %s", authType))
	}()
	// If user is specified via command line, password should be specified as well.
	if c.User != "" {
		if c.Password == "" {
			cmd.Print("Enter password: ")
			password, err := term.ReadPassword(int(os.Stdin.Fd()))
			if err != nil {
				return errors.ErrCredentialNotFound.GenWithStackByArgs(c.User, "Error reading password, ", err)
			}
			cmd.Println()
			c.Password = string(password)
		}
		return nil
	}

	// If user is not specified via command line, try to get it from environment variable.
	authType = "environment variable"
	c.User = os.Getenv(envVarTiCDCUser)
	c.Password = os.Getenv(envVarTiCDCPassword)
	if c.User != "" {
		return nil
	}

	// If user is not specified via command line or environment variable, try to get it from credential file.
	authType = "credential file"
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return errors.WrapError(errors.ErrCredentialNotFound, err, "failed to get user home directory")
	}
	filename := filepath.Join(homeDir, defaultCrendentialConfigFile)
	if _, err := os.Stat(filename); err == nil {
		err = util.StrictDecodeFile(filename, "cdc cli auth config", &c.ClientAuth)
		if err != nil {
			msg := fmt.Sprintf("failed to parse authentication from creandential file <%s>", filename)
			return errors.WrapError(errors.ErrCredentialNotFound, err, msg)
		}
	}

	return nil
}

// GetAuthParameters returns the authentication parameters.
func (c *ClientFlags) GetAuthParameters() url.Values {
	if c.User == "" {
		return nil
	}
	return url.Values{
		api.APIOpVarTiCDCUser:     {c.User},
		api.APIOpVarTiCDCPassword: {c.Password},
	}
}
