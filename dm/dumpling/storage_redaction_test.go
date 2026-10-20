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

package dumpling

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCleanupStorageErrorRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>bucket access denied</Message><RequestId>test-request-id</RequestId></Error>"))
	}))
	defer server.Close()
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk&region=us-east-1&endpoint=" + url.QueryEscape(server.URL)
	cfg := &config.SubTaskConfig{
		Name: "storage-redaction", SourceID: "source-one",
		LoaderConfig: config.LoaderConfig{Dir: raw},
	}
	d := NewDumpling(cfg)
	d.metricProxies = defaultMetricProxies
	core, logs := observer.New(zap.ErrorLevel)
	d.logger = log.Logger{Logger: zap.New(core)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan pb.ProcessResult, 1)
	d.Process(ctx, results)
	result := <-results
	require.Len(t, result.Errors, 1)
	require.Equal(t, int32(32001), result.Errors[0].ErrCode)
	require.Contains(t, result.Errors[0].Message, "fail to remove output directory")
	require.Contains(t, result.Errors[0].RawCause, "403")
	require.NotContains(t, result.String(), "private-")
	require.Equal(t, raw, cfg.Dir)
	require.Len(t, logs.All(), 1)
	data, err := json.Marshal(logs.All()[0].ContextMap())
	require.NoError(t, err)
	require.NotContains(t, string(data), "private-")
}
