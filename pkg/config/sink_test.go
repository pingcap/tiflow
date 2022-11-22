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
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOldValue(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		protocol       string
		enableOldValue bool
		expectedErr    string
	}{
		{
			protocol:       "default",
			enableOldValue: false,
			expectedErr:    "",
		},
		{
			protocol:       "default",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "canal-json",
			enableOldValue: false,
			expectedErr:    ".*canal-json protocol requires old value to be enabled.*",
		},
		{
			protocol:       "canal-json",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "canal",
			enableOldValue: false,
			expectedErr:    ".*canal protocol requires old value to be enabled.*",
		},
		{
			protocol:       "canal",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "maxwell",
			enableOldValue: false,
			expectedErr:    ".*maxwell protocol requires old value to be enabled.*",
		},
		{
			protocol:       "maxwell",
			enableOldValue: true,
			expectedErr:    "",
		},
	}

	for _, tc := range testCases {
		cfg := SinkConfig{
			Protocol: tc.protocol,
		}
		if tc.expectedErr == "" {
			require.Nil(t, cfg.validateAndAdjust(nil, tc.enableOldValue))
		} else {
			require.Regexp(t, tc.expectedErr, cfg.validateAndAdjust(nil, tc.enableOldValue))
		}
	}
}

func TestValidateApplyParameter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		sinkURI       string
		expectedErr   string
		expectedLevel AtomicityLevel
	}{
		{
			sinkURI:       "mysql://normal:123456@127.0.0.1:3306",
			expectedErr:   "",
			expectedLevel: tableTxnAtomicity,
		},
		{
			sinkURI:       "mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=table",
			expectedErr:   "",
			expectedLevel: tableTxnAtomicity,
		},
		{
			sinkURI:       "mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=none",
			expectedErr:   "",
			expectedLevel: noneTxnAtomicity,
		},
		{
			sinkURI:     "mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=global",
			expectedErr: "global level atomicity is not supported by.*",
		},
		{
			sinkURI:     "tidb://normal:123456@127.0.0.1:3306?protocol=canal",
			expectedErr: ".*protocol canal is incompatible with tidb scheme.*",
		},
		{
			sinkURI:     "tidb://normal:123456@127.0.0.1:3306?protocol=default",
			expectedErr: ".*protocol default is incompatible with tidb scheme.*",
		},
		{
			sinkURI:     "tidb://normal:123456@127.0.0.1:3306?protocol=random",
			expectedErr: ".*protocol .* is incompatible with tidb scheme.*",
		},
		{
			sinkURI:       "blackhole://normal:123456@127.0.0.1:3306?transaction-atomicity=none",
			expectedErr:   "",
			expectedLevel: noneTxnAtomicity,
		},
		{
			sinkURI: "kafka://127.0.0.1:9092?transaction-atomicity=none" +
				"&protocol=open-protocol",
			expectedErr:   "",
			expectedLevel: noneTxnAtomicity,
		},
		{
			sinkURI: "pulsar://127.0.0.1:9092?transaction-atomicity=table" +
				"&protocol=open-protocol",
			expectedErr:   "",
			expectedLevel: noneTxnAtomicity,
		},
		{
			sinkURI:       "kafka://127.0.0.1:9092?protocol=default",
			expectedErr:   "",
			expectedLevel: noneTxnAtomicity,
		},
		{
			sinkURI:     "kafka://127.0.0.1:9092?transaction-atomicity=table",
			expectedErr: ".*unknown .* protocol for Message Queue sink.*",
		},
	}

	for _, tc := range testCases {
		cfg := SinkConfig{}
		parsedSinkURI, err := url.Parse(tc.sinkURI)
		require.Nil(t, err)
		if tc.expectedErr == "" {
			require.Nil(t, cfg.validateAndAdjust(parsedSinkURI, true))
			require.Equal(t, tc.expectedLevel, cfg.TxnAtomicity)
		} else {
			require.Regexp(t, tc.expectedErr, cfg.validateAndAdjust(parsedSinkURI, true))
		}
	}
}

func TestApplyParameter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		sinkConfig *SinkConfig
		sinkURI    string
		result     string
	}{
		{
			sinkConfig: &SinkConfig{
				Protocol: "default",
			},
			sinkURI: "kafka://127.0.0.1:9092?protocol=whatever",
			result:  "whatever",
		},
		{
			sinkConfig: &SinkConfig{},
			sinkURI:    "kafka://127.0.0.1:9092?protocol=default",
			result:     "default",
		},
		{
			sinkConfig: &SinkConfig{
				Protocol: "default",
			},
			sinkURI: "kafka://127.0.0.1:9092",
			result:  "default",
		},
	}
	for _, c := range testCases {
		parsedSinkURI, err := url.Parse(c.sinkURI)
		require.Nil(t, err)
		c.sinkConfig.applyParameter(parsedSinkURI)
		require.Equal(t, c.result, c.sinkConfig.Protocol)
	}
}
