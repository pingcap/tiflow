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

	"github.com/BurntSushi/toml"
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
	defaultCrendentialConfigFile = ".ticdc/credentials"
	// User Credential Environment Variables
	envVarTiCDCUser     = "TICDC_USER"
	envVarTiCDCPassword = "TICDC_PASSWORD"
	// TLS Client Certificate Environment Variables
	envVarTiCDCCAPath   = "TICDC_CA_PATH"
	envVarTiCDCCertPath = "TICDC_CERT_PATH"
	envVarTiCDCKeyPath  = "TICDC_KEY_PATH"
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
	// User Credential
	User     string `toml:"ticdc_user,omitempty"`
	Password string `toml:"ticdc_password,omitempty"`

	// TLS Client Certificate
	CaPath   string `toml:"ca_path,omitempty"`
	CertPath string `toml:"cert_path,omitempty"`
	KeyPath  string `toml:"key_path,omitempty"`
}

// StoreToDefaultPath stores the client authentication to default path.
func (c *ClientAuth) StoreToDefaultPath() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		msg := "failed to get user home directory"
		return fmt.Errorf("%s: %w", msg, err)
	}

	filename := filepath.Join(homeDir, defaultCrendentialConfigFile)
	err = os.MkdirAll(filepath.Dir(filename), os.ModePerm)
	if err != nil {
		msg := fmt.Sprintf("failed to create directory for creandential file <%s>", filename)
		return fmt.Errorf("%s: %w", msg, err)
	}
	file, err := os.Create(filename)
	if err != nil {
		msg := fmt.Sprintf("failed to create creandential file <%s>", filename)
		return fmt.Errorf("%s: %w", msg, err)
	}

	err = toml.NewEncoder(file).Encode(c)
	if err != nil {
		msg := fmt.Sprintf("failed to encode client authentication to creandential file <%s>", filename)
		return fmt.Errorf("%s: %w", msg, err)
	}

	err = file.Close()
	if err != nil {
		msg := fmt.Sprintf("failed to store client authentication to creandential file <%s>", filename)
		return fmt.Errorf("%s: %w", msg, err)
	}
	return nil
}

// ReadFromDefaultPath reads the client authentication from default path.
func ReadFromDefaultPath() (*ClientAuth, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		msg := "failed to get user home directory"
		return nil, fmt.Errorf("%s: %w", msg, err)
	}

	res := &ClientAuth{}
	filename := filepath.Join(homeDir, defaultCrendentialConfigFile)
	if _, err := os.Stat(filename); err == nil {
		err = util.StrictDecodeFile(filename, "cdc cli auth config", res)
		if err != nil {
			msg := fmt.Sprintf("failed to parse client authentication from creandential file <%s>", filename)
			return nil, fmt.Errorf("%s: %w", msg, err)
		}
	}
	return res, nil
}

// ClientFlags specifies the parameters needed to construct the client.
type ClientFlags struct {
	ClientAuth
	pdAddr     string
	serverAddr string
	logLevel   string
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
	cmd.PersistentFlags().StringVar(&c.CaPath, "ca", "",
		"CA certificate path for TLS connection to CDC server")
	cmd.PersistentFlags().StringVar(&c.CertPath, "cert", "",
		"Certificate path for TLS connection to CDC server")
	cmd.PersistentFlags().StringVar(&c.KeyPath, "key", "",
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
		CAPath:        c.CaPath,
		CertPath:      c.CertPath,
		KeyPath:       c.KeyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// CompleteClientAuthParameters completes the authentication parameters.
func (c *ClientFlags) CompleteClientAuthParameters(cmd *cobra.Command) error {
	c.completeTLSClientCertificate(cmd)
	return c.completeUserCredential(cmd)
}

func (c *ClientFlags) completeUserCredential(cmd *cobra.Command) (err error) {
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
	res, err := ReadFromDefaultPath()
	if err != nil {
		return errors.WrapError(errors.ErrCredentialNotFound, err)
	}
	if res != nil {
		c.User = res.User
		c.Password = res.Password
	}
	return nil
}

func (c *ClientFlags) completeTLSClientCertificate(cmd *cobra.Command) {
	authType := "command line"
	defer func() {
		if c.CaPath == "" && c.CertPath == "" && c.KeyPath == "" {
			authType = "disabled"
		}
		log.Info(fmt.Sprintf("cli tls client certificate type: %s", authType))
	}()
	// If one of the client certificate is specified via command line, all of them should be specified.
	if c.CaPath != "" || c.CertPath != "" || c.KeyPath != "" {
		return
	}

	// If none of the client certificate is specified via command line, try to get it from environment variable.
	authType = "environment variable"
	c.CaPath = os.Getenv(envVarTiCDCCAPath)
	c.CertPath = os.Getenv(envVarTiCDCCertPath)
	c.KeyPath = os.Getenv(envVarTiCDCKeyPath)
	if c.CaPath != "" || c.CertPath != "" || c.KeyPath != "" {
		return
	}

	// If none of the client certificate is specified via command line or environment variable, try to get it from credential file.
	authType = "credential file"
	res, err := ReadFromDefaultPath()
	if err != nil {
		cmd.Println("failed to read client certificate from default config file: , try to use insecure connection", err)
	}
	if res != nil {
		c.CaPath = res.CaPath
		c.CertPath = res.CertPath
		c.KeyPath = res.KeyPath
	}
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
