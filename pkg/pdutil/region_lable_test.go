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

package pdutil

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	testServer *httptest.Server
	url        string
}

func (m *mockPDClient) GetLeaderAddr() string {
	return m.url
}

func newMockPDClient(normal bool) *mockPDClient {
	mock := &mockPDClient{}
	status := http.StatusOK
	if !normal {
		status = http.StatusNotFound
	}
	mock.testServer = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		},
	))
	mock.url = mock.testServer.URL

	return mock
}

func TestMetaLabelNormal(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient := newMockPDClient(true)

	err := UpdateMetaLabel(ctx, mockClient)
	require.Nil(t, err)
	mockClient.testServer.Close()
}

func TestMetaLabelFail(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient := newMockPDClient(false)
	pc, err := newPDApiClient(mockClient)
	require.Nil(t, err)
	mockClient.url = "http://127.0.1.1:2345"

	// test url error
	err = pc.patchMetaLabel(ctx)
	require.NotNil(t, err)

	// test 404
	mockClient.url = mockClient.testServer.URL
	err = pc.patchMetaLabel(ctx)
	require.Regexp(t, ".*404.*", err)

	err = UpdateMetaLabel(ctx, mockClient)
	require.ErrorIs(t, err, cerror.ErrReachMaxTry)
	mockClient.testServer.Close()
}
<<<<<<< HEAD:pkg/pdutil/region_lable_test.go
=======

func TestListGcServiceSafePoint(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient := newMockPDClient(ctx, true)

	_, err := ListGcServiceSafePoint(ctx, mockClient, nil)
	require.Nil(t, err)
	mockClient.testServer.Close()
}

// LabelRulePatch is the patch to update the label rules.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// Copied from github.com/tikv/pd/server/schedule/labeler
type LabelRulePatch struct {
	SetRules    []*LabelRule `json:"sets"`
	DeleteRules []string     `json:"deletes"`
}

// LabelRule is the rule to assign labels to a region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// Copied from github.com/tikv/pd/server/schedule/labeler
type LabelRule struct {
	ID       string        `json:"id"`
	Index    int           `json:"index"`
	Labels   []RegionLabel `json:"labels"`
	RuleType string        `json:"rule_type"`
	Data     interface{}   `json:"data"`
}

// RegionLabel is the label of a region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// Copied from github.com/tikv/pd/server/schedule/labeler
type RegionLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestMetaLabelDecodeJSON(t *testing.T) {
	t.Parallel()

	meta := LabelRulePatch{}
	require.Nil(t, json.Unmarshal([]byte(addMetaJSON), &meta))
	require.Len(t, meta.SetRules, 2)
	keys := meta.SetRules[1].Data.([]interface{})[0].(map[string]interface{})
	startKey, err := hex.DecodeString(keys["start_key"].(string))
	require.Nil(t, err)
	endKey, err := hex.DecodeString(keys["end_key"].(string))
	require.Nil(t, err)

	_, startKey, err = codec.DecodeBytes(startKey, nil)
	require.Nil(t, err)
	require.EqualValues(
		t, regionspan.JobTableID, tablecodec.DecodeTableID(startKey), keys["start_key"].(string))

	_, endKey, err = codec.DecodeBytes(endKey, nil)
	require.Nil(t, err)
	require.EqualValues(
		t, regionspan.JobTableID+1, tablecodec.DecodeTableID(endKey), keys["end_key"].(string))
}
>>>>>>> c3a120488 (pdutil(ticdc): split `tidb_ddl_job` table (#6673)):pkg/pdutil/pd_api_client_test.go
