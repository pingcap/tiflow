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

	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestValidateTxnAtomicity(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		sinkURI        string
		expectedErr    string
		shouldSplitTxn bool
	}{
		{
			sinkURI:        "mysql://normal:123456@127.0.0.1:3306",
			expectedErr:    "",
			shouldSplitTxn: true,
		},
		{
			sinkURI:        "mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=table",
			expectedErr:    "",
			shouldSplitTxn: false,
		},
		{
			sinkURI:        "mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=none",
			expectedErr:    "",
			shouldSplitTxn: true,
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
			sinkURI:        "blackhole://normal:123456@127.0.0.1:3306?transaction-atomicity=none",
			expectedErr:    "",
			shouldSplitTxn: true,
		},
		{
			sinkURI: "kafka://127.0.0.1:9092?transaction-atomicity=none" +
				"&protocol=open-protocol",
			expectedErr:    "",
			shouldSplitTxn: true,
		},
		{
			sinkURI:        "kafka://127.0.0.1:9092?protocol=default",
			expectedErr:    "",
			shouldSplitTxn: true,
		},
		{
			sinkURI:     "kafka://127.0.0.1:9092?transaction-atomicity=none",
			expectedErr: ".*unknown .* message protocol for sink.*",
		},
		{
			sinkURI: "kafka://127.0.0.1:9092?transaction-atomicity=table" +
				"&protocol=open-protocol",
			expectedErr: "table level atomicity is not supported by kafka scheme",
		},
		{
			sinkURI: "kafka://127.0.0.1:9092?transaction-atomicity=invalid" +
				"&protocol=open-protocol",
			expectedErr: "invalid level atomicity is not supported by kafka scheme",
		},
		{
			sinkURI: "pulsar://127.0.0.1:6550?transaction-atomicity=invalid" +
				"&protocol=open-protocol",
			expectedErr: "invalid level atomicity is not supported by pulsar scheme",
		},
		{
			sinkURI:        "pulsar://127.0.0.1:6550/test?protocol=canal-json",
			shouldSplitTxn: true,
		},
	}

	for _, tc := range testCases {
		cfg := SinkConfig{}
		parsedSinkURI, err := url.Parse(tc.sinkURI)
		require.Nil(t, err)
		if tc.expectedErr == "" {
			require.Nil(t, cfg.validateAndAdjust(parsedSinkURI))
			require.Equal(t, tc.shouldSplitTxn, util.GetOrZero(cfg.TxnAtomicity).ShouldSplitTxn())
		} else {
			require.Regexp(t, tc.expectedErr, cfg.validateAndAdjust(parsedSinkURI))
		}
	}
}

func TestValidateProtocol(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		sinkConfig *SinkConfig
		sinkURI    string
		result     string
	}{
		{
			sinkConfig: &SinkConfig{
				Protocol: util.AddressOf("default"),
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
				Protocol: util.AddressOf("default"),
			},
			sinkURI: "kafka://127.0.0.1:9092",
			result:  "default",
		},
		{
			sinkConfig: &SinkConfig{
				Protocol: util.AddressOf("default"),
			},
			sinkURI: "pulsar://127.0.0.1:6650",
			result:  "default",
		},
		{
			sinkConfig: &SinkConfig{
				Protocol: util.AddressOf("canal-json"),
			},
			sinkURI: "pulsar://127.0.0.1:6650/test?protocol=canal-json",
			result:  "canal-json",
		},
	}
	for _, c := range testCases {
		parsedSinkURI, err := url.Parse(c.sinkURI)
		require.Nil(t, err)
		c.sinkConfig.validateAndAdjustSinkURI(parsedSinkURI)
		require.Equal(t, c.result, util.GetOrZero(c.sinkConfig.Protocol))
	}
}

func TestApplyParameterBySinkURI(t *testing.T) {
	t.Parallel()
	kafkaURI := "kafka://127.0.0.1:9092?protocol=whatever&transaction-atomicity=none"
	testCases := []struct {
		sinkConfig           *SinkConfig
		sinkURI              string
		expectedErr          string
		expectedProtocol     string
		expectedTxnAtomicity AtomicityLevel
	}{
		// test only config file
		{
			sinkConfig: &SinkConfig{
				Protocol:     util.AddressOf("default"),
				TxnAtomicity: util.AddressOf(noneTxnAtomicity),
			},
			sinkURI:              "kafka://127.0.0.1:9092",
			expectedProtocol:     "default",
			expectedTxnAtomicity: noneTxnAtomicity,
		},
		// test only sink uri
		{
			sinkConfig:           &SinkConfig{},
			sinkURI:              kafkaURI,
			expectedProtocol:     "whatever",
			expectedTxnAtomicity: noneTxnAtomicity,
		},
		// test conflict scenarios
		{
			sinkConfig: &SinkConfig{
				Protocol:     util.AddressOf("default"),
				TxnAtomicity: util.AddressOf(tableTxnAtomicity),
			},
			sinkURI:              kafkaURI,
			expectedProtocol:     "whatever",
			expectedTxnAtomicity: noneTxnAtomicity,
			expectedErr:          "incompatible configuration in sink uri",
		},
		{
			sinkConfig: &SinkConfig{
				Protocol:     util.AddressOf("default"),
				TxnAtomicity: util.AddressOf(unknownTxnAtomicity),
			},
			sinkURI:              kafkaURI,
			expectedProtocol:     "whatever",
			expectedTxnAtomicity: noneTxnAtomicity,
			expectedErr:          "incompatible configuration in sink uri",
		},
	}
	for _, tc := range testCases {
		parsedSinkURI, err := url.Parse(tc.sinkURI)
		require.Nil(t, err)
		err = tc.sinkConfig.applyParameterBySinkURI(parsedSinkURI)

		require.Equal(t, util.AddressOf(tc.expectedProtocol), tc.sinkConfig.Protocol)
		require.Equal(t, util.AddressOf(tc.expectedTxnAtomicity), tc.sinkConfig.TxnAtomicity)
		if tc.expectedErr == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.expectedErr)
		}
	}
}

func TestCheckCompatibilityWithSinkURI(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		newSinkConfig        *SinkConfig
		oldSinkConfig        *SinkConfig
		newsinkURI           string
		expectedErr          string
		expectedProtocol     *string
		expectedTxnAtomicity *AtomicityLevel
	}{
		// test no update
		{
			newSinkConfig:        &SinkConfig{},
			oldSinkConfig:        &SinkConfig{},
			newsinkURI:           "kafka://",
			expectedProtocol:     nil,
			expectedTxnAtomicity: nil,
		},
		// test update config return err
		{
			newSinkConfig: &SinkConfig{
				TxnAtomicity: util.AddressOf(tableTxnAtomicity),
			},
			oldSinkConfig: &SinkConfig{
				TxnAtomicity: util.AddressOf(noneTxnAtomicity),
			},
			newsinkURI:           "kafka://127.0.0.1:9092?transaction-atomicity=none",
			expectedErr:          "incompatible configuration in sink uri",
			expectedProtocol:     nil,
			expectedTxnAtomicity: util.AddressOf(noneTxnAtomicity),
		},
		// test update compatible config
		{
			newSinkConfig: &SinkConfig{
				Protocol: util.AddressOf("canal"),
			},
			oldSinkConfig: &SinkConfig{
				TxnAtomicity: util.AddressOf(noneTxnAtomicity),
			},
			newsinkURI:           "kafka://127.0.0.1:9092?transaction-atomicity=none",
			expectedProtocol:     util.AddressOf("canal"),
			expectedTxnAtomicity: util.AddressOf(noneTxnAtomicity),
		},
		// test update sinkuri
		{
			newSinkConfig: &SinkConfig{
				TxnAtomicity: util.AddressOf(noneTxnAtomicity),
			},
			oldSinkConfig: &SinkConfig{
				TxnAtomicity: util.AddressOf(noneTxnAtomicity),
			},
			newsinkURI:           "kafka://127.0.0.1:9092?transaction-atomicity=table",
			expectedProtocol:     nil,
			expectedTxnAtomicity: util.AddressOf(tableTxnAtomicity),
		},
	}
	for _, tc := range testCases {
		err := tc.newSinkConfig.CheckCompatibilityWithSinkURI(tc.oldSinkConfig, tc.newsinkURI)
		if tc.expectedErr == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.expectedErr)
		}
		require.Equal(t, tc.expectedProtocol, tc.newSinkConfig.Protocol)
		require.Equal(t, tc.expectedTxnAtomicity, tc.newSinkConfig.TxnAtomicity)
	}
}

func TestValidateAndAdjustCSVConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  *CSVConfig
		wantErr string
	}{
		{
			name: "valid quote",
			config: &CSVConfig{
				Quote:                "\"",
				Delimiter:            ",",
				BinaryEncodingMethod: BinaryEncodingBase64,
			},
			wantErr: "",
		},
		{
			name: "quote has multiple characters",
			config: &CSVConfig{
				Quote: "***",
			},
			wantErr: "csv config quote contains more than one character",
		},
		{
			name: "quote contains line break character",
			config: &CSVConfig{
				Quote: "\n",
			},
			wantErr: "csv config quote cannot be line break character",
		},
		{
			name: "valid delimiter1",
			config: &CSVConfig{
				Quote:                "\"",
				Delimiter:            ",",
				BinaryEncodingMethod: BinaryEncodingHex,
			},
			wantErr: "",
		},
		{
			name: "delimiter is empty",
			config: &CSVConfig{
				Quote:     "'",
				Delimiter: "",
			},
			wantErr: "csv config delimiter cannot be empty",
		},
		{
			name: "delimiter contains line break character",
			config: &CSVConfig{
				Quote:     "'",
				Delimiter: "\r",
			},
			wantErr: "csv config delimiter contains line break characters",
		},
		{
			name: "delimiter contains more than one character",
			config: &CSVConfig{
				Quote:     "'",
				Delimiter: "\r\t",
			},
			wantErr: "csv config delimiter contains more than one character",
		},
		{
			name: "delimiter and quote are same",
			config: &CSVConfig{
				Quote:     "'",
				Delimiter: "'",
			},
			wantErr: "csv config quote and delimiter cannot be the same",
		},
		{
			name: "invalid binary encoding method",
			config: &CSVConfig{
				Quote:                "\"",
				Delimiter:            ",",
				BinaryEncodingMethod: "invalid",
			},
			wantErr: "csv config binary-encoding-method can only be hex or base64",
		},
	}
	for _, c := range tests {
		tc := c
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &SinkConfig{
				CSVConfig: tc.config,
			}
			if tc.wantErr == "" {
				require.Nil(t, s.CSVConfig.validateAndAdjust())
			} else {
				require.Regexp(t, tc.wantErr, s.CSVConfig.validateAndAdjust())
			}
		})
	}
}

func TestValidateAndAdjustStorageConfig(t *testing.T) {
	t.Parallel()

	sinkURI, err := url.Parse("s3://bucket?protocol=csv")
	require.NoError(t, err)
	s := GetDefaultReplicaConfig()
	err = s.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)
	require.Equal(t, DefaultFileIndexWidth, util.GetOrZero(s.Sink.FileIndexWidth))

	err = s.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)
	require.Equal(t, DefaultFileIndexWidth, util.GetOrZero(s.Sink.FileIndexWidth))

	s.Sink.FileIndexWidth = util.AddressOf(16)
	err = s.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)
	require.Equal(t, 16, util.GetOrZero(s.Sink.FileIndexWidth))
}
