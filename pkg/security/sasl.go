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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
)

// SASLMechanism defines SASL mechanism.
type SASLMechanism string

// The mechanisms we currently support.
const (
	// UnknownMechanism means the SASL mechanism is unknown.
	UnknownMechanism SASLMechanism = ""
	// PlainMechanism means the SASL mechanism is plain.
	PlainMechanism SASLMechanism = sarama.SASLTypePlaintext
	// SCRAM256Mechanism means the SASL mechanism is SCRAM-SHA-256.
	SCRAM256Mechanism SASLMechanism = sarama.SASLTypeSCRAMSHA256
	// SCRAM512Mechanism means the SASL mechanism is SCRAM-SHA-512.
	SCRAM512Mechanism SASLMechanism = sarama.SASLTypeSCRAMSHA512
	// GSSAPIMechanism means the SASL mechanism is GSSAPI.
	GSSAPIMechanism SASLMechanism = sarama.SASLTypeGSSAPI
)

// SASLMechanismFromString converts the string to SASL mechanism.
func SASLMechanismFromString(s string) (SASLMechanism, error) {
	switch strings.ToLower(s) {
	case "plain":
		return PlainMechanism, nil
	case "scram-sha-256":
		return SCRAM256Mechanism, nil
	case "scram-sha-512":
		return SCRAM512Mechanism, nil
	case "gssapi":
		return GSSAPIMechanism, nil
	default:
		return UnknownMechanism, errors.Errorf("unknown %s SASL mechanism", s)
	}
}

// SASL holds necessary path parameter to support sasl-scram
type SASL struct {
	SASLUser      string        `toml:"sasl-user" json:"sasl-user"`
	SASLPassword  string        `toml:"sasl-password" json:"sasl-password"`
	SASLMechanism SASLMechanism `toml:"sasl-mechanism" json:"sasl-mechanism"`
	GSSAPI        GSSAPI        `toml:"sasl-gssapi" json:"sasl-gssapi"`
}

// GSSAPIAuthType defines the type of GSSAPI authentication.
type GSSAPIAuthType int

const (
	// UnknownAuth means the auth type is unknown.
	UnknownAuth GSSAPIAuthType = 0
	// UserAuth means the auth type is user.
	UserAuth GSSAPIAuthType = sarama.KRB5_USER_AUTH
	// KeyTabAuth means the auth type is keytab.
	KeyTabAuth GSSAPIAuthType = sarama.KRB5_KEYTAB_AUTH
)

// AuthTypeFromString convent the string to GSSAPIAuthType.
func AuthTypeFromString(s string) (GSSAPIAuthType, error) {
	switch strings.ToLower(s) {
	case "user":
		return UserAuth, nil
	case "keytab":
		return KeyTabAuth, nil
	default:
		return UnknownAuth, errors.Errorf("unknown %s auth type", s)
	}
}

// GSSAPI holds necessary path parameter to support sasl-gssapi.
type GSSAPI struct {
	AuthType           GSSAPIAuthType `toml:"sasl-gssapi-auth-type" json:"sasl-gssapi-auth-type"`
	KeyTabPath         string         `toml:"sasl-gssapi-keytab-path" json:"sasl-gssapi-keytab-path"`
	KerberosConfigPath string         `toml:"sasl-gssapi-kerberos-config-path" json:"sasl-gssapi-kerberos-config-path"`
	ServiceName        string         `toml:"sasl-gssapi-service-name" json:"sasl-gssapi-service-name"`
	Username           string         `toml:"sasl-gssapi-user" json:"sasl-gssapi-user"`
	Password           string         `toml:"sasl-gssapi-password" json:"sasl-gssapi-password"`
	Realm              string         `toml:"sasl-gssapi-realm" json:"sasl-gssapi-realm"`
	DisablePAFXFAST    bool           `toml:"sasl-gssapi-disable-pafxfast" json:"sasl-gssapi-disable-pafxfast"`
}
