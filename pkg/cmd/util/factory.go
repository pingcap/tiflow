package util

import (
	"strings"

	"crypto/tls"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/pflag"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

type Factory interface {
	ClientGetter
	EtcdClient() (*kv.CDCEtcdClient, error)
	PdClient() (*pd.Client, error)
}

type ClientGetter interface {
	ToTLSConfig() (*tls.Config, error)
	ToGRPCDialOption() (grpc.DialOption, error)
	GetPdAddr() string
	GetCredential() *security.Credential
}

type ClientFlags struct {
	cliPdAddr     string
	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
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
	return &ClientFlags{
		caPath:        "",
		certPath:      "",
		keyPath:       "",
		allowedCertCN: "",
	}
}

func (c *ClientFlags) AddFlags(flags *pflag.FlagSet, isServer bool) {
	flags.StringVar(&c.caPath, "ca", "", "CA certificate path for TLS connection")
	flags.StringVar(&c.certPath, "cert", "", "Certificate path for TLS connection")
	flags.StringVar(&c.keyPath, "key", "", "Private key path for TLS connection")
	if isServer {
		flags.StringVar(&c.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
	}
}

func (c *ClientFlags) GetCredential() *security.Credential {
	var certAllowedCN []string
	if len(c.allowedCertCN) != 0 {
		certAllowedCN = strings.Split(c.allowedCertCN, ",")
	}
	return &security.Credential{
		CAPath:        c.caPath,
		CertPath:      c.certPath,
		KeyPath:       c.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}
