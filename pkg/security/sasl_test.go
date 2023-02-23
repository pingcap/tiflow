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

package security

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSASLMechanismFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		s                 string
		expectedMechanism string
		expectErr         string
	}{
		{
			name:              "random mechanism",
			s:                 "random",
			expectedMechanism: "",
			expectErr:         "unknown random SASL mechanism",
		},
		{
			name:              "lower case plain mechanism",
			s:                 "plain",
			expectedMechanism: "PLAIN",
		},
		{
			name:              "upper case plain mechanism",
			s:                 "PLAIN",
			expectedMechanism: "PLAIN",
		},
		{
			name:              "lower case scram-sha-256 mechanism",
			s:                 "scram-sha-256",
			expectedMechanism: "SCRAM-SHA-256",
		},
		{
			name:              "upper case SCRAM-SHA-256 mechanism",
			s:                 "SCRAM-SHA-256",
			expectedMechanism: "SCRAM-SHA-256",
		},
		{
			name:              "lower case scram-sha-512 mechanism",
			s:                 "scram-sha-512",
			expectedMechanism: "SCRAM-SHA-512",
		},
		{
			name:              "upper case SCRAM-SHA-512 mechanism",
			s:                 "SCRAM-SHA-512",
			expectedMechanism: "SCRAM-SHA-512",
		},
		{
			name:              "lower case gssapi mechanism",
			s:                 "gssapi",
			expectedMechanism: "GSSAPI",
		},
		{
			name:              "upper case GSSAPI mechanism",
			s:                 "GSSAPI",
			expectedMechanism: "GSSAPI",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mechanism, err := SASLMechanismFromString(test.s)
			if test.expectErr != "" {
				require.Error(t, err)
				require.Regexp(t, test.expectErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedMechanism, string(mechanism))
			}
		})
	}
}

func TestAuthTypeFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		s            string
		expectedType int
		expectErr    string
	}{
		{
			name:         "unknown",
			s:            "a",
			expectedType: 0,
			expectErr:    "unknown a auth type",
		},
		{
			name:         "lower case user",
			s:            "user",
			expectedType: 1,
		},
		{
			name:         "upper case user",
			s:            "USER",
			expectedType: 1,
		},
		{
			name:         "lower case keytab",
			s:            "keytab",
			expectedType: 2,
		},
		{
			name:         "upper case keytab",
			s:            "KEYTAB",
			expectedType: 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			authType, err := AuthTypeFromString(test.s)
			if test.expectErr != "" {
				require.Error(t, err)
				require.Regexp(t, test.expectErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedType, int(authType))
			}
		})
	}
}
