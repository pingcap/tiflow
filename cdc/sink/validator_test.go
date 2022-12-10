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

package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreCheckSinkURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  string
		err  string
	}{
		{
			name: "valid domain MySQL URI",
			uri:  "mysql://root:111@baidu.com:3306/",
			err:  "",
		},
		{
			name: "valid IPv4 MySQL URI",
			uri:  "mysql://root:111@127.0.0.1:3306/",
			err:  "",
		},
		{
			name: "valid IPv6 MySQL URI",
			uri:  "mysql://root:111@[::1]:3306/",
			err:  "",
		},
		{
			name: "valid IPv4 Kafka URI",
			uri:  "kafka://127.0.0.1:9092/topic1",
			err:  "",
		},
		{
			name: "valid IPv6 Kafka URI",
			uri:  "kafka://[3333:10:9:101::204]:9092/topic1",
			err:  "",
		},
		{
			name: "blackhole URI",
			uri:  "blackhole://",
			err:  "",
		},
		{
			name: "empty URI",
			uri:  "",
			err:  "sink uri is empty",
		},
		{
			name: "invalid IPv6 MySQL URI",
			uri:  "mysql://root:111@::1:3306/",
			err:  "sink uri host is not valid IPv6 address",
		},
		{
			name: "invalid IPv6 Kafka URI",
			uri:  "kafka://3333:10:9:101::204:9092/topic1",
			err:  "sink uri host is not valid IPv6 address",
		},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := preCheckSinkURI(test.uri)
			if test.err != "" {
				require.Contains(t, err.Error(), test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
