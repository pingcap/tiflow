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
		sinkURI          string
		expectedErr      string
		expectedSplitTxn bool
	}{
		{
			sinkURI:          "mysql://normal:123456@127.0.0.1:3306",
			expectedErr:      "",
			expectedSplitTxn: false,
		},
		{
			sinkURI:          "mysql://normal:123456@127.0.0.1:3306?split-txn=true",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
		{
			sinkURI:          "mysql://normal:123456@127.0.0.1:3306?split-txn=false",
			expectedErr:      "",
			expectedSplitTxn: false,
		},
		{
			sinkURI:     "mysql://normal:123456@127.0.0.1:3306?split-txn=invalid",
			expectedErr: ".*invalid split-txn value.*",
		},
		{
			sinkURI:     "tidb://normal:123456@127.0.0.1:3306?split-txn=true&protocol=canal",
			expectedErr: ".*protocol cannot be configured when using tidb scheme.*",
		},
		{
			sinkURI:          "blackhole://normal:123456@127.0.0.1:3306?split-txn=false",
			expectedErr:      "",
			expectedSplitTxn: false,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?split-txn=true&protocol=open-protocol",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?split-txn=false&protocol=open-protocol",
			expectedErr:      "",
			expectedSplitTxn: false,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?split-txn=true&protocol=default",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?split-txn=false&protocol=default",
			expectedErr:      "",
			expectedSplitTxn: false,
		},
		{
			sinkURI:     "kafka://127.0.0.1:9092?split-txn=false",
			expectedErr: ".*unknown .* protocol for Message Queue sink.*",
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?protocol=canal-json",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?protocol=avro&split-txn=false",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
		{
			sinkURI:          "kafka://127.0.0.1:9092?protocol=maxwell&split-txn=true",
			expectedErr:      "",
			expectedSplitTxn: true,
		},
	}

	for _, tc := range testCases {
		cfg := SinkConfig{}
		parsedSinkURI, err := url.Parse(tc.sinkURI)
		require.Nil(t, err)
		if tc.expectedErr == "" {
			require.Nil(t, cfg.validateAndAdjust(parsedSinkURI, true))
			require.Equal(t, tc.expectedSplitTxn, cfg.SplitTxn)
		} else {
			require.Regexp(t, tc.expectedErr, cfg.validateAndAdjust(parsedSinkURI, true))
		}
	}
}
