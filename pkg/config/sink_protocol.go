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

// Protocol is the protocol of the message.
type Protocol int

// Enum types of the Protocol.
const (
	ProtocolUnknown Protocol = iota
	ProtocolDefault
	ProtocolCanal
	ProtocolAvro
	ProtocolMaxwell
	ProtocolCanalJSON
	ProtocolCraft
	ProtocolOpen
	ProtocolCsv
	ProtocolDebezium
	ProtocolSimple
)

// IsBatchEncode returns whether the protocol is a batch encoder.
func (p Protocol) IsBatchEncode() bool {
	return p == ProtocolOpen || p == ProtocolCanal || p == ProtocolMaxwell || p == ProtocolCraft
}

// ParseSinkProtocolFromString converts the protocol from string to Protocol enum type.
func ParseSinkProtocolFromString(protocol string) (Protocol, error) {
	switch strings.ToLower(protocol) {
	case "default":
		return ProtocolOpen, nil
	case "canal":
		return ProtocolCanal, nil
	case "avro":
		return ProtocolAvro, nil
	case "flat-avro":
		return ProtocolAvro, nil
	case "maxwell":
		return ProtocolMaxwell, nil
	case "canal-json":
		return ProtocolCanalJSON, nil
	case "craft":
		return ProtocolCraft, nil
	case "open-protocol":
		return ProtocolOpen, nil
	case "csv":
		return ProtocolCsv, nil
	case "debezium":
		return ProtocolDebezium, nil
	case "simple":
		return ProtocolSimple, nil
	default:
		return ProtocolUnknown, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(protocol)
	}
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
	case ProtocolCsv:
		return "csv"
	case ProtocolDebezium:
		return "debezium"
	case ProtocolSimple:
		return "simple"
	default:
		panic("unreachable")
	}
}
