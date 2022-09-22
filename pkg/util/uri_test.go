// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
