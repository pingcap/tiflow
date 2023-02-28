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

package util

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTopic(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		sinkURI   string
		wantTopic string
		wantErr   string
	}{
		"no topic": {
			sinkURI:   "kafka://localhost:9092/",
			wantTopic: "",
			wantErr:   "no topic is specified in sink-uri",
		},
		"valid topic": {
			sinkURI:   "kafka://localhost:9092/test",
			wantTopic: "test",
			wantErr:   "",
		},
		"topic with query": {
			sinkURI:   "kafka://localhost:9092/test?version=1.0.0",
			wantTopic: "test",
			wantErr:   "",
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sinkURI, err := url.Parse(tc.sinkURI)
			require.NoError(t, err)
			topic, err := GetTopic(sinkURI)
			if tc.wantErr != "" {
				require.Regexp(t, tc.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantTopic, topic)
			}
		})
	}
}
