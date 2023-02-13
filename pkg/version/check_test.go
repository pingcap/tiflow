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
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/utils/tempurl"
)

type mockPDClient struct {
	pd.Client
	getAllStores  func() []*metapb.Store
	getPDVersion  func() string
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

	if m.getPDVersion != nil {
		_, _ = resp.Write([]byte(fmt.Sprintf(`{"version":"%s"}`, m.getPDVersion())))
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
	cli, err := httputil.NewClient(nil)
	require.Nil(t, err)
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := cli.Get(context.Background(), pdAddr)
		if err == nil {
			_ = resp.Body.Close()
			break
		}

		assert.Failf(t, "http.Get fail, need retry", "%v", err)
		if i == 19 {
			require.FailNowf(t, "TestCheckClusterVersion fail", "http server timeout:%v", err)
		}
	}

	// Check min pd / tikv version
	{
		mock.getPDVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Nil(t, err)
	}

	// check pd / tikv with the maximum allowed version
	{
		mock.getPDVersion = func() string {
			return "7.9.9"
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "v7.9.9"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Nil(t, err)
	}

	// Check invalid PD/TiKV version.
	{
		mock.getPDVersion = func() string {
			return "testPD"
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*invalid PD version.*", err)
	}

	{
		mock.getPDVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: "testKV"}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*invalid TiKV version.*", err)
	}

	// pd version lower than the min supported
	{
		mock.getPDVersion = func() string {
			return `v1.0.0-alpha-271-g824ae7fd`
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*PD .* is not supported.*", err)
	}

	{
		mock.getPDVersion = func() string {
			return maxPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Regexp(t, ".*PD .* is not supported.*", err)
	}

	// tikv version lower than the min supported
	{
		mock.getPDVersion = func() string {
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

	// Skip checking TiFlash.
	{
		mock.getPDVersion = func() string {
			return minPDVersion.String()
		}

		tiflashStore := &metapb.Store{
			Version: maxTiKVVersion.String(),
			Labels:  []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
		}
		require.True(t, engine.IsTiFlash(tiflashStore))
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{tiflashStore}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdAddrs, nil, true)
		require.Nil(t, err)
	}

	// Check maximum supported TiKV version
	{
		mock.getPDVersion = func() string {
			return minPDVersion.String()
		}

		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: maxTiKVVersion.String()}}
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
	// build on master branch, `vx.y.z-master`
	masterVersion := semver.New(SanitizeVersion("v6.3.0-master"))
	require.Equal(t, 1, masterVersion.Compare(*MinTiCDCVersion))

	// pre-release version, `vx.y.z-alpha-nightly-yyyymmdd`
	alphaVersion := semver.New(SanitizeVersion("v6.3.0-alpha-nightly-20220202"))
	require.Equal(t, 1, alphaVersion.Compare(*MinTiCDCVersion))

	// release version, `vx.y.z.`
	releaseVersion := semver.New(SanitizeVersion("v6.3.0"))
	require.Equal(t, 1, releaseVersion.Compare(*MinTiCDCVersion))

	// build with uncommitted changes, `vx.y.z-dirty`
	dirtyVersion := semver.New(SanitizeVersion("v6.3.0-dirty"))
	require.Equal(t, 1, dirtyVersion.Compare(*MinTiCDCVersion))
	require.Equal(t, 0, dirtyVersion.Compare(*semver.New("6.3.0")))
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
			expected:        ticdcClusterVersionUnknown,
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
		expected: ticdcClusterVersionUnknown,
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

	require.Equal(t, ticdcClusterVersionUnknown.ShouldEnableUnifiedSorterByDefault(), true)
	require.Equal(t, ticdcClusterVersionUnknown.ShouldEnableOldValueByDefault(), true)
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
	require.Contains(t, checkPDVersion(context.TODO(), ts.URL, nil).Error(),
		"[CDC:ErrCheckClusterVersionFromPD]failed to request PD 500 Internal Server Error , please try again later",
	)
}

func TestCheckTiCDCVersion(t *testing.T) {
	t.Parallel()

	// all captures in the cluster in the same version which is the minimum supported one.
	versions := map[string]struct{}{
		"v6.3.0": {},
	}
	require.NoError(t, CheckTiCDCVersion(versions))

	// 2  different versions both within the range, it's ok
	versions = map[string]struct{}{
		"v6.3.0": {},
		"v6.4.0": {},
	}
	require.NoError(t, CheckTiCDCVersion(versions))

	versions = map[string]struct{}{
		"v6.3.0": {},
		"v7.9.9": {},
	}
	err := CheckTiCDCVersion(versions)
	require.NoError(t, err)

	versions = map[string]struct{}{
		"v6.3.0": {},
		"v6.4.0": {},
		"v6.5.0": {},
	}
	err = CheckTiCDCVersion(versions)
	require.Regexp(t, ".*all running cdc instance belong to 3 different versions.*", err)

	versions = map[string]struct{}{
		"v6.3.0":       {},
		"v8.0.0-alpha": {},
	}
	err = CheckTiCDCVersion(versions)
	require.Regexp(t, "TiCDC .* not supported, only support version less than.*", err)

	versions = map[string]struct{}{
		"v6.3.0":        {},
		"v8.0.0-master": {},
	}
	err = CheckTiCDCVersion(versions)
	require.Regexp(t, "TiCDC .* not supported, only support version less than.*", err)

	versions = map[string]struct{}{
		"v6.3.0": {},
		"v8.0.0": {},
	}
	err = CheckTiCDCVersion(versions)
	require.Regexp(t, "TiCDC .* not supported, only support version less than.*", err)

	versions = map[string]struct{}{
		"v6.3.0": {},
		"v6.2.9": {},
	}
	err = CheckTiCDCVersion(versions)
	require.Regexp(t, "TiCDC .* not supported, the minimal compatible version.*", err)

	versions = map[string]struct{}{
		"v6.3.0-master": {},
		"v7.0.0":        {},
	}
	err = CheckTiCDCVersion(versions)
	require.NoError(t, err)
}
