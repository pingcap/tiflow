// Copyright 2020 PingCAP, Inc.
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

package version

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/tempurl"
)

type mockPDClient struct {
	pd.Client
	getAllStores  func() []*metapb.Store
	getVersion    func() string
	getStatusCode func() int
}

func (m *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.getAllStores != nil {
		return m.getAllStores(), nil
	}
	return []*metapb.Store{}, nil
}

func (m *mockPDClient) ServeHTTP(resp http.ResponseWriter, _ *http.Request) {
	// set status code at first, else will not work
	if m.getStatusCode != nil {
		resp.WriteHeader(m.getStatusCode())
	}

	if m.getVersion != nil {
		_, _ = resp.Write([]byte(fmt.Sprintf(`{"version":"%s"}`, m.getVersion())))
	}
}

func TestCheckClusterVersion(t *testing.T) {
	t.Parallel()
	mock := mockPDClient{
		Client: nil,
	}
	pdURL, _ := url.Parse(tempurl.Alloc())
	pdAddr := fmt.Sprintf("http://%s", pdURL.Host)
	pdAddrs := []string{pdAddr}
	srv := http.Server{Addr: pdURL.Host, Handler: &mock}
	go func() {
		//nolint:errcheck
		srv.ListenAndServe()
	}()
	defer srv.Close()
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := http.Get(pdAddr)
		if err == nil {
			break
		}

		assert.Failf(t, "http.Get fail, need retry", "%v", err)
		if i == 19 {
			require.FailNowf(t, "TestCheckClusterVersion fail", "http server timeout:%v", err)
		}
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Nil(t, err)
	}

	// Check invalid PD/TiKV version.
	{
		mock.getVersion = func() string {
			return "testPD"
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*invalid PD version.*", err)
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "testKV"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*invalid TiKV version.*", err)
	}

	{
		mock.getVersion = func() string {
			return `v1.0.0-alpha-271-g824ae7fd`
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*PD .* is not supported.*", err)
	}

	// Check maximum compatible PD.
	{
		mock.getVersion = func() string {
			return `v10000.0.0`
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*PD .* is not supported.*", err)
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			// TiKV does not include 'v'.
			return []*metapb.Store{{Version: `1.0.0-alpha-271-g824ae7fd`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*TiKV .* is not supported.*", err)
		err = CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, false)
		require.Nil(t, err)
	}

	// Check maximum compatible TiKV.
	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			// TiKV does not include 'v'.
			return []*metapb.Store{{Version: `10000.0.0`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*TiKV .* is not supported.*", err)
	}

	{
		mock.getStatusCode = func() int {
			return http.StatusBadRequest
		}

		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, false)
		require.Regexp(t, ".*400 Bad Request.*", err)
	}
}

func TestCompareVersion(t *testing.T) {
	require.Equal(t, semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), -1)
	require.Equal(t, semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0-rc.2")), -1)
	require.Equal(t, semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), -1)
	require.Equal(t, semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), 1)

	require.Equal(t, semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), -1)
	require.Equal(t, semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), 1)
	require.Equal(t, semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")), 0)
	require.Equal(t, semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")), 0)
	require.Equal(t, semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")), 0)
	require.Equal(t, semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")), 0)
}

func TestReleaseSemver(t *testing.T) {
	cases := []struct{ releaseVersion, releaseSemver string }{
		{"None", ""},
		{"HEAD", ""},
		{"v4.0.5", "4.0.5"},
		{"v4.0.2-152-g62d7075-dev", "4.0.2"},
	}

	for _, cs := range cases {
		ReleaseVersion = cs.releaseVersion
		require.Equal(t, ReleaseSemver(), cs.releaseSemver, "%v", cs)
	}
}

func TestGetTiCDCClusterVersion(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		captureVersions []string
		expected        TiCDCClusterVersion
	}{
		{
			captureVersions: []string{},
			expected:        TiCDCClusterVersionUnknown,
		},
		{
			captureVersions: []string{
				"",
				"",
				"",
			},
			expected: TiCDCClusterVersion{defaultTiCDCVersion},
		},
		{
			captureVersions: []string{
				"5.0.1",
				"4.0.7",
				"5.0.0-rc",
			},
			expected: TiCDCClusterVersion{semver.New("4.0.7")},
		},
		{
			captureVersions: []string{
				"5.0.0-rc",
			},
			expected: TiCDCClusterVersion{semver.New("5.0.0-rc")},
		},
		{
			captureVersions: []string{
				"5.0.0",
			},
			expected: TiCDCClusterVersion{semver.New("5.0.0")},
		},
		{
			captureVersions: []string{
				"4.1.0",
			},
			expected: TiCDCClusterVersion{semver.New("4.1.0")},
		},
		{
			captureVersions: []string{
				"4.0.10",
			},
			expected: TiCDCClusterVersion{semver.New("4.0.10")},
		},
	}
	for _, tc := range testCases {
		ver, err := GetTiCDCClusterVersion(tc.captureVersions)
		require.Nil(t, err)
		require.Equal(t, ver, tc.expected)
	}

	invalidTestCase := struct {
		captureVersions []string
		expected        TiCDCClusterVersion
	}{
		captureVersions: []string{
			"",
			"testCDC",
		},
		expected: TiCDCClusterVersionUnknown,
	}
	_, err := GetTiCDCClusterVersion(invalidTestCase.captureVersions)
	require.Regexp(t, ".*invalid CDC cluster version.*", err)
}

func TestTiCDCClusterVersionFeaturesCompatible(t *testing.T) {
	t.Parallel()
	ver := TiCDCClusterVersion{semver.New("4.0.10")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), false)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), false)

	ver = TiCDCClusterVersion{semver.New("4.0.12")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), false)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), false)

	ver = TiCDCClusterVersion{semver.New("4.0.13")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), false)

	ver = TiCDCClusterVersion{semver.New("4.0.13-hotfix")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), false)

	ver = TiCDCClusterVersion{semver.New("4.0.14")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), false)

	ver = TiCDCClusterVersion{semver.New("5.0.0-rc")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), false)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), true)

	ver = TiCDCClusterVersion{semver.New("5.0.0")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), true)

	ver = TiCDCClusterVersion{semver.New("5.1.0")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), true)

	ver = TiCDCClusterVersion{semver.New("5.2.0-alpha")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), true)

	ver = TiCDCClusterVersion{semver.New("5.2.0-master")}
	require.Equal(t, ver.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ver.ShouldEnableOldValueByDefault(), true)

	require.Equal(t, TiCDCClusterVersionUnknown.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, TiCDCClusterVersionUnknown.ShouldEnableOldValueByDefault(), true)
}

func TestCheckTiCDCClusterVersion(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		cdcClusterVersion TiCDCClusterVersion
		expectedErr       string
		expectedUnknown   bool
	}{
		{
			cdcClusterVersion: TiCDCClusterVersionUnknown,
			expectedErr:       "",
			expectedUnknown:   true,
		},
		{
			cdcClusterVersion: TiCDCClusterVersion{Version: MinTiCDCVersion},
			expectedErr:       "",
			expectedUnknown:   false,
		},
		{
			cdcClusterVersion: TiCDCClusterVersion{Version: semver.New("1.0.0")},
			expectedErr:       ".*minimal compatible version.*",
			expectedUnknown:   false,
		},
		{
			cdcClusterVersion: TiCDCClusterVersion{Version: semver.New("10000.0.0")},
			expectedErr:       ".*maximum compatible version.*",
			expectedUnknown:   false,
		},
	}

	for _, tc := range testCases {
		isUnknown, err := CheckTiCDCClusterVersion(tc.cdcClusterVersion)
		require.Equal(t, isUnknown, tc.expectedUnknown)
		if len(tc.expectedErr) != 0 {
			require.Regexp(t, tc.expectedErr, err)
		}
	}
}

func TestCheckPDVersionError(t *testing.T) {
	t.Parallel()

	var resp func(w http.ResponseWriter, r *http.Request)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp(w, r)
	}))
	defer ts.Close()

	resp = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}
	require.Contains(t, CheckPDVersion(context.TODO(), ts.URL, nil).Error(),
		"[CDC:ErrCheckClusterVersionFromPD]failed to request PD 500 Internal Server Error , please try again later",
	)
}
