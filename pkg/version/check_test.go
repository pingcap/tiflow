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
	"net/url"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/tempurl"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type checkSuite struct{}

var _ = check.Suite(&checkSuite{})

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

func (s *checkSuite) TestCheckClusterVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	mock := mockPDClient{
		Client: nil,
	}
	pdURL, _ := url.Parse(tempurl.Alloc())
	pdHTTP := fmt.Sprintf("http://%s", pdURL.Host)
	srv := http.Server{Addr: pdURL.Host, Handler: &mock}
	go func() {
		//nolint:errcheck
		srv.ListenAndServe()
	}()
	defer srv.Close()
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := http.Get(pdHTTP)
		if err == nil {
			break
		}
		c.Error(err)
		if i == 19 {
			c.Fatal("http server timeout", err)
		}
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP, nil, true)
		c.Assert(err, check.IsNil)
	}

	{
		mock.getVersion = func() string {
			return `v1.0.0-alpha-271-g824ae7fd`
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: MinTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP, nil, true)
		c.Assert(err, check.ErrorMatches,
			".*PD .* is not supported.*")
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			// TiKV does not include 'v'.
			return []*metapb.Store{{Version: `1.0.0-alpha-271-g824ae7fd`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP, nil, true)
		c.Assert(err, check.ErrorMatches,
			".*TiKV .* is not supported.*")
		err = CheckClusterVersion(context.Background(), &mock, pdHTTP, nil, false)
		c.Assert(err, check.IsNil)
	}

	{
		mock.getStatusCode = func() int {
			return http.StatusBadRequest
		}

		err := CheckClusterVersion(context.Background(), &mock, pdHTTP, nil, false)
		c.Assert(err, check.ErrorMatches, ".*response status: .*")
	}
}

func (s *checkSuite) TestCompareVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), check.Equals, 1)

	c.Assert(semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), check.Equals, 1)
	c.Assert(semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")), check.Equals, 0)
}

func (s *checkSuite) TestReleaseSemver(c *check.C) {
	defer testleak.AfterTest(c)()
	cases := []struct{ releaseVersion, releaseSemver string }{
		{"None", ""},
		{"HEAD", ""},
		{"v4.0.5", "4.0.5"},
		{"v4.0.2-152-g62d7075-dev", "4.0.2"},
	}

	for _, cs := range cases {
		ReleaseVersion = cs.releaseVersion
		c.Assert(ReleaseSemver(), check.Equals, cs.releaseSemver, check.Commentf("%v", cs))
	}
}

func (s *checkSuite) TestGetTiCDCClusterVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		captureInfos []*model.CaptureInfo
		expected     TiCDCClusterVersion
	}{
		{
			captureInfos: []*model.CaptureInfo{},
			expected:     TiCDCClusterVersionUnknown,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: ""},
				{ID: "capture2", Version: ""},
				{ID: "capture3", Version: ""},
			},
			expected: TiCDCClusterVersion4_0,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: "5.0.1"},
				{ID: "capture2", Version: "4.0.7"},
				{ID: "capture3", Version: "5.0.0-rc"},
			},
			expected: TiCDCClusterVersion4_0,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: "5.0.0-rc"},
			},
			expected: TiCDCClusterVersion5_0,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: "5.0.0"},
			},
			expected: TiCDCClusterVersion5_0,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: "4.1.0"},
			},
			expected: TiCDCClusterVersion4_0,
		},
		{
			captureInfos: []*model.CaptureInfo{
				{ID: "capture1", Version: "4.0.10"},
			},
			expected: TiCDCClusterVersion4_0,
		},
	}
	for _, tc := range testCases {
		ver, err := GetTiCDCClusterVersion(tc.captureInfos)
		c.Assert(err, check.IsNil)
		c.Assert(ver, check.Equals, tc.expected)
	}
}
