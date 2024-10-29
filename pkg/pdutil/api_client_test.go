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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	testServer *httptest.Server
	url        string
}

func (m *mockPDClient) GetLeaderURL() string {
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
			_, _ = w.Write([]byte("{}"))
		},
	))
	mock.url = mock.testServer.URL

	return mock
}

func TestMetaLabelNormal(t *testing.T) {
	t.Parallel()

	mockClient := newMockPDClient(true)

	pc, err := NewPDAPIClient(mockClient, nil)
	require.NoError(t, err)
	defer pc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = pc.UpdateMetaLabel(ctx)
	require.NoError(t, err)
	mockClient.testServer.Close()
}

func TestMetaLabelFail(t *testing.T) {
	t.Parallel()

	mockClient := newMockPDClient(false)
	pc, err := NewPDAPIClient(mockClient, nil)
	require.NoError(t, err)
	defer pc.Close()
	mockClient.url = "http://127.0.1.1:2345"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// test url error
	err = pc.(*pdAPIClient).patchMetaLabel(ctx)
	require.Error(t, err)

	// test 404
	mockClient.url = mockClient.testServer.URL
	err = pc.(*pdAPIClient).patchMetaLabel(ctx)
	require.Regexp(t, ".*404.*", err)

	err = pc.UpdateMetaLabel(ctx)
	require.ErrorIs(t, err, cerror.ErrReachMaxTry)
	mockClient.testServer.Close()
}

func TestListGcServiceSafePoint(t *testing.T) {
	t.Parallel()

	mockClient := newMockPDClient(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc, err := NewPDAPIClient(mockClient, nil)
	require.NoError(t, err)
	defer pc.Close()
	_, err = pc.ListGcServiceSafePoint(ctx)
	require.NoError(t, err)
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
	require.NoError(t, err)
	endKey, err := hex.DecodeString(keys["end_key"].(string))
	require.NoError(t, err)

	_, startKey, err = codec.DecodeBytes(startKey, nil)
	require.NoError(t, err)
	require.EqualValues(
		t, spanz.JobTableID, tablecodec.DecodeTableID(startKey), keys["start_key"].(string))

	_, endKey, err = codec.DecodeBytes(endKey, nil)
	require.NoError(t, err)
	require.EqualValues(
		t, spanz.JobTableID+1, tablecodec.DecodeTableID(endKey), keys["end_key"].(string))
}

func TestScanRegions(t *testing.T) {
	t.Parallel()

	regions := []RegionInfo{
		NewTestRegionInfo(2, []byte(""), []byte{0, 1}, 0),
		NewTestRegionInfo(3, []byte{0, 1}, []byte{0, 2}, 1),
		NewTestRegionInfo(4, []byte{0, 2}, []byte{0, 3}, 2),
		NewTestRegionInfo(5, []byte{0, 2}, []byte{0, 4}, 3), // a merged region.
		NewTestRegionInfo(6, []byte{0, 4}, []byte{1, 0}, 4),
		NewTestRegionInfo(7, []byte{1, 0}, []byte{1, 1}, 5),
		NewTestRegionInfo(8, []byte{1, 1}, []byte(""), 6),
	}
	var handler func() RegionsInfo
	mockPDServer := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			startKey, _ := hex.DecodeString(r.URL.Query()["key"][0])
			endKey, _ := hex.DecodeString(r.URL.Query()["end_key"][0])
			limit, _ := strconv.Atoi(r.URL.Query()["limit"][0])
			t.Log(startKey, endKey, limit)
			info := handler()
			info.Count = len(info.Regions)
			data, _ := json.Marshal(info)
			t.Logf("%s", string(data))
			_, _ = w.Write(data)
		},
	))
	defer mockPDServer.Close()

	httpcli, _ := httputil.NewClient(nil)
	pc := pdAPIClient{httpClient: httpcli}

	i := 0
	handler = func() RegionsInfo {
		start := i
		end := i + 1
		i++
		if end > len(regions) {
			return RegionsInfo{Regions: regions[start:]}
		}
		return RegionsInfo{Regions: regions[start:end]}
	}
	rs, err := pc.scanRegions(context.Background(), tablepb.Span{}, []string{mockPDServer.URL}, 1)
	require.NoError(t, err)
	require.Equal(t, 7, len(rs))

	handler = func() RegionsInfo {
		return RegionsInfo{Regions: regions}
	}
	rs, err = pc.scanRegions(context.Background(), tablepb.Span{}, []string{mockPDServer.URL}, 1024)
	require.NoError(t, err)
	require.Equal(t, 7, len(rs))

	i = 0
	handler = func() RegionsInfo {
		if i != 0 {
			require.FailNow(t, "must only request once")
		}
		i++
		return RegionsInfo{Regions: regions[2:3]}
	}
	rs, err = pc.scanRegions(
		context.Background(),
		tablepb.Span{StartKey: []byte{0, 2, 0}, EndKey: []byte{0, 3}},
		[]string{mockPDServer.URL}, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))

	i = 0
	handler = func() RegionsInfo {
		if i == 0 {
			i++
			return RegionsInfo{Regions: regions[2:3]}
		} else if i == 1 {
			i++
			return RegionsInfo{Regions: regions[3:4]}
		} else if i == 2 {
			i++
			return RegionsInfo{Regions: regions[4:5]}
		}

		require.FailNow(t, "must only request once")
		return RegionsInfo{}
	}
	rs, err = pc.scanRegions(
		context.Background(),
		tablepb.Span{StartKey: []byte{0, 2, 0}, EndKey: []byte{0, 4, 0}},
		[]string{mockPDServer.URL}, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(rs))
}
