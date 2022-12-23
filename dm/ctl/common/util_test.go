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

package common

import (
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/stretchr/testify/require"
)

func TestHTMLEscape(t *testing.T) {
	msg := &pb.ProcessResult{Errors: []*pb.ProcessError{
		{Message: "checksum mismatched remote vs local =>"},
	}}
	output, err := marshResponseToString(msg)
	require.NoError(t, err)
	// TODO: how can we turn it off? https://github.com/gogo/protobuf/issues/484
	require.Contains(t, output, "checksum mismatched remote vs local =\\u003e")
}
