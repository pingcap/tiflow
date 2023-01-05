//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestGetChangefeedFiles(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fileNames  []string
		changefeed model.ChangeFeedID
		want       []string
	}{
		{
			fileNames: []string{
				"captureID_test-2_uuid1.log",
				"captureID_test-3_uuid2.log",
				"captureID_test-1_uuid3.log",
			},
			changefeed: model.DefaultChangeFeedID("test-1"),
			want: []string{
				"captureID_test-1_uuid3.log",
			},
		},
		{
			fileNames: []string{
				"captureID_n1_test-2_uuid4.log",
				"captureID_n2_test-2_uuid5.log",
				"captureID_n1_test-1_uuid6.log",
			},
			changefeed: model.ChangeFeedID{Namespace: "n1", ID: "test-2"},
			want: []string{
				"captureID_n1_test-2_uuid4.log",
			},
		},
	}

	for _, c := range cases {
		got := FilterChangefeedFiles(c.fileNames, c.changefeed)
		require.Equal(t, c.want, got)
	}
}
