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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		protocol             string
		expectedProtocolEnum Protocol
	}{
		{
			protocol:             "unknown",
			expectedProtocolEnum: ProtocolDefault,
		},
		{
			protocol:             "default",
			expectedProtocolEnum: ProtocolDefault,
		},
		{
			protocol:             "canal",
			expectedProtocolEnum: ProtocolCanal,
		},
		{
			protocol:             "canal-json",
			expectedProtocolEnum: ProtocolCanalJSON,
		},
		{
			protocol:             "maxwell",
			expectedProtocolEnum: ProtocolMaxwell,
		},
		{
			protocol:             "avro",
			expectedProtocolEnum: ProtocolAvro,
		},
		{
			protocol:             "craft",
			expectedProtocolEnum: ProtocolCraft,
		},
	}

	for _, tc := range testCases {
		var protocol Protocol
		protocol.FromString(tc.protocol)
		require.Equal(t, tc.expectedProtocolEnum, protocol)
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		protocolEnum     Protocol
		expectedProtocol string
	}{
		{
			protocolEnum:     ProtocolDefault,
			expectedProtocol: "default",
		},
		{
			protocolEnum:     ProtocolCanal,
			expectedProtocol: "canal",
		},
		{
			protocolEnum:     ProtocolCanalJSON,
			expectedProtocol: "canal-json",
		},
		{
			protocolEnum:     ProtocolMaxwell,
			expectedProtocol: "maxwell",
		},
		{
			protocolEnum:     ProtocolAvro,
			expectedProtocol: "avro",
		},
		{
			protocolEnum:     ProtocolCraft,
			expectedProtocol: "craft",
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expectedProtocol, tc.protocolEnum.String())
	}
}
