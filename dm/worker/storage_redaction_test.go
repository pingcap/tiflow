// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestStorageStatusWithoutSourceWorker(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	log.SetRedactLog(false)
	t.Cleanup(func() { log.SetRedactLog(previous) })
	s := &Server{cfg: &Config{Name: "worker-one"}, sourceStatus: pb.SourceStatus{
		Source: "source-one", Result: &pb.ProcessResult{Errors: []*pb.ProcessError{{
			ErrCode: 32001, RawCause: "403 https://bucket/path?sig=private-signature",
		}}},
	}}
	before := s.sourceStatus.String()
	response, err := s.QueryStatus(context.Background(), &pb.QueryStatusRequest{})
	require.NoError(t, err)
	require.NotContains(t, response.String(), "private-signature")
	require.Equal(t, int32(32001), response.SourceStatus.Result.Errors[0].ErrCode)
	require.Equal(t, before, s.sourceStatus.String())
}
