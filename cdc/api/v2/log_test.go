// Copyright 2023 PingCAP, Inc.
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

package v2

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/stretchr/testify/require"
)

func TestSetLogLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		body string
		code int
	}{
		{`{`, 400},
		{`{}`, 200},
		{`{"log_level":"debug"}`, 200},
		{`{"log_level":"info"}`, 200},
		{`{"log_level":"warn"}`, 200},
		{`{"log_level":"error"}`, 200},
		{`{"log_level":"dpanic"}`, 200},
		{`{"log_level":"panic"}`, 200},
		{`{"log_level":"fatal"}`, 200},
		{`{"log_level":"xxxx"}`, 400},
	}
	for _, c := range cases {
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"POST",
			"/api/v2/log",
			bytes.NewReader([]byte(c.body)),
		)
		router.ServeHTTP(w, req)
		require.Equal(t, c.code, w.Code)
		if c.code == 200 {
			require.Equal(t, "{}", w.Body.String())
		}
	}
}
