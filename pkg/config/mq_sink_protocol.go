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
	"strings"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// ProtocolKey specifies the key of the protocol in the SinkURI.
	ProtocolKey = "protocol"
)

// Protocol is the protocol of the mq message.
type Protocol int

// Enum types of the Protocol.
const (
	ProtocolDefault Protocol = iota
	ProtocolCanal
	ProtocolAvro
	ProtocolMaxwell
	ProtocolCanalJSON
	ProtocolCraft
	ProtocolOpen
)

// IsBatchEncode returns whether the protocol is a batch encoder.
func (p Protocol) IsBatchEncode() bool {
	return p == ProtocolOpen || p == ProtocolCanal || p == ProtocolMaxwell || p == ProtocolCraft
}

// FromString converts the protocol from string to Protocol enum type.
func (p *Protocol) FromString(protocol string) error {
	switch strings.ToLower(protocol) {
	case "default":
		*p = ProtocolOpen
	case "canal":
		*p = ProtocolCanal
	case "avro":
		*p = ProtocolAvro
	case "flat-avro":
		*p = ProtocolAvro
	case "maxwell":
		*p = ProtocolMaxwell
	case "canal-json":
		*p = ProtocolCanalJSON
	case "craft":
		*p = ProtocolCraft
	case "open-protocol":
		*p = ProtocolOpen
	default:
		return cerror.ErrMQSinkUnknownProtocol.GenWithStackByArgs(protocol)
	}

	return nil
}

// String converts the Protocol enum type string to string.
func (p Protocol) String() string {
	switch p {
	case ProtocolDefault:
		return "default"
	case ProtocolCanal:
		return "canal"
	case ProtocolAvro:
		return "avro"
	case ProtocolMaxwell:
		return "maxwell"
	case ProtocolCanalJSON:
		return "canal-json"
	case ProtocolCraft:
		return "craft"
	case ProtocolOpen:
		return "open-protocol"
	default:
		panic("unreachable")
	}
}
