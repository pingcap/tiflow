// Copyright 2022 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsValidIPv6AddressFormatInURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"valid ipv6 address", "[::1]", true},
		{"valid ipv6 address1 with port", "[::1]:8080", true},
		{"valid ipv6 address2 with port", "[1080:0:0:0:8:800:200C:417A]:8080", true},
		{"valid ipv6 address3 with port", "[::FFFF:129.144.52.38]:8080", true},
		{"invalid ipv6 address", "::1", false},
		{"invalid ipv6 address with port", "::1:8000", false},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, IsValidIPv6AddressFormatInURI(test.host))
		})
	}
}

func TestIsIPv6Address(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"valid ipv6 address1", "::1", true},
		{"valid ipv6 address2", "1080:0:0:0:8:800:200C:417A", true},
		{"ipv4 address", "127.0.0.1", false},
		{"empty address", "", false},
		{"not ip address", "emmmmmmmm", false},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, IsIPv6Address(test.host))
		})
	}
}

func TestMaskSinkURI(t *testing.T) {
	tests := []struct {
		uri    string
		masked string
	}{
		{
			"mysql://root:123456@127.0.0.1:3306/?time-zone=Asia/Shanghai",
			"mysql://root:xxxxx@127.0.0.1:3306/?time-zone=Asia/Shanghai",
		},
		{
			"kafka://127.0.0.1:9093/cdc?sasl-mechanism=SCRAM-SHA-256&sasl-user=ticdc&sasl-password=verysecure",
			"kafka://127.0.0.1:9093/cdc?sasl-mechanism=SCRAM-SHA-256&sasl-password=xxxxx&sasl-user=ticdc",
		},
	}

	for _, tt := range tests {
		maskedURI, err := MaskSinkURI(tt.uri)
		require.NoError(t, err)
		require.Equal(t, tt.masked, maskedURI)
	}
}

func TestMaskSensitiveDataInURI(t *testing.T) {
	tests := []struct {
		uri    string
		masked string
	}{
		{
			"mysql://root:123456@127.0.0.1:3306/?time-zone=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?time-zone=c",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?access_key=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?access_key=xxxxx",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?secret_access_key=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?secret_access_key=xxxxx",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?client_secret=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?client_secret=xxxxx",
		},
		{
			"",
			"",
		},
		{
			"abc",
			"abc",
		},
	}
	for _, q := range sensitiveQueryParameterNames {
		tests = append(tests, struct {
			uri    string
			masked string
		}{
			"kafka://127.0.0.1:9093/cdc?" + q + "=verysecure",
			"kafka://127.0.0.1:9093/cdc?" + q + "=xxxxx",
		})
	}

	for _, tt := range tests {
		maskedURI := MaskSensitiveDataInURI(tt.uri)
		require.Equal(t, tt.masked, maskedURI)
	}
}
