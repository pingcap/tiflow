// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"net"
	"net/url"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

// CDCV2 represents config for ticdc v2
type CDCV2 struct {
	// Enable represents if the cdc v2 is enabled or not
	Enable bool `toml:"enable" json:"enable"`
	// MetaStoreConfig  represents config for new meta store configurations
	MetaStoreConfig MetaStoreConfiguration `toml:"meta-store" json:"meta-store"`
}

// MetaStoreConfiguration represents config for new meta store configurations
type MetaStoreConfiguration struct {
	// URI is the address of the meta store.
	// for example:  "mysql://127.0.0.1:3306/test"
	URI string `toml:"uri" json:"uri"`
	// SSLCA is the path of the CA certificate file.
	SSLCa   string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey  string `toml:"ssl-key" json:"ssl-key"`
}

// ValidateAndAdjust validates the meta store configurations
func (c *CDCV2) ValidateAndAdjust() error {
	if !c.Enable {
		return nil
	}
	if c.MetaStoreConfig.URI == "" {
		return errors.New("missing meta store uri configuration")
	}
	parsedURI, err := url.Parse(c.MetaStoreConfig.URI)
	if err != nil {
		return errors.Trace(err)
	}
	if !isSupportedScheme(parsedURI.Scheme) {
		return errors.Errorf("the %s scheme is not supported by meta store", parsedURI.Scheme)
	}
	return nil
}

// GenDSN generates a DSN from the given  metastore config.
func (cfg *MetaStoreConfiguration) GenDSN() (*dmysql.Config, error) {
	endpoint, err := url.Parse(cfg.URI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tls, err := cfg.getSSLParam()
	if err != nil {
		return nil, errors.Trace(err)
	}
	username := endpoint.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := endpoint.User.Password()

	hostName := endpoint.Hostname()
	port := endpoint.Port()
	if port == "" {
		port = "3306"
	}

	// This will handle the IPv6 address format.
	var dsn *dmysql.Config
	host := net.JoinHostPort(hostName, port)
	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsnStr := fmt.Sprintf("%s:%s@tcp(%s)%s%s", username, password, host, endpoint.Path, tls)
	if dsn, err = dmysql.ParseDSN(dsnStr); err != nil {
		return nil, errors.Trace(err)
	}

	// create test db used for parameter detection
	// Refer https://github.com/go-sql-driver/mysql#parameters
	if dsn.Params == nil {
		dsn.Params = make(map[string]string)
	}
	// enable parseTime for time.Time type
	dsn.Params["parseTime"] = "true"
	for key, pa := range endpoint.Query() {
		dsn.Params[key] = pa[0]
	}
	return dsn, nil
}

func (cfg *MetaStoreConfiguration) getSSLParam() (string, error) {
	if len(cfg.SSLCa) == 0 || len(cfg.SSLCert) == 0 || len(cfg.SSLKey) == 0 {
		return "", nil
	}
	credential := security.Credential{
		CAPath:   cfg.SSLCa,
		CertPath: cfg.SSLCert,
		KeyPath:  cfg.SSLKey,
	}
	tlsCfg, err := credential.ToTLSConfig()
	if err != nil {
		return "", errors.Trace(err)
	}
	name := "cdc_mysql_tls_meta_store"
	err = dmysql.RegisterTLSConfig(name, tlsCfg)
	if err != nil {
		return "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	return "?tls=" + name, nil
}

// isSupportedScheme returns true if the scheme is compatible with MySQL.
func isSupportedScheme(scheme string) bool {
	return scheme == "mysql"
}
