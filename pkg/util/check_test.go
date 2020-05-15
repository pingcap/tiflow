package util

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/pkg/tempurl"
)

type checkSuite struct{}

var _ = check.Suite(&checkSuite{})

type mockPDClient struct {
	pd.Client
	getAllStores func() []*metapb.Store
	getVersion   func() string
}

func (m *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.getAllStores != nil {
		return m.getAllStores(), nil
	}
	return []*metapb.Store{}, nil
}

func (m *mockPDClient) ServeHTTP(resp http.ResponseWriter, _ *http.Request) {
	if m.getVersion != nil {
		_, _ = resp.Write([]byte(fmt.Sprintf(`{"version":"%s"}`, m.getVersion())))
	}
}

func (s *checkSuite) TestCheckClusterVersion(c *check.C) {
	mock := mockPDClient{
		Client: nil,
	}
	pdURL, _ := url.Parse(tempurl.Alloc())
	pdHTTP := fmt.Sprintf("http://%s", pdURL.Host)
	svr := http.Server{Addr: pdURL.Host, Handler: &mock}
	go func() {
		c.Assert(svr.ListenAndServe(), check.IsNil)
	}()
	defer svr.Close()
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := http.Get(pdHTTP)
		if err == nil {
			break
		}
		c.Error(err)
		if i == 199 {
			c.Fatal("http server timeout", err)
		}
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP)
		c.Assert(err, check.IsNil)
	}

	{
		mock.getVersion = func() string {
			return `v1.0.0-alpha-271-g824ae7fd`
		}
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP)
		c.Assert(err, check.ErrorMatches, "PD .* is not supported.*")
	}

	{
		mock.getVersion = func() string {
			return minPDVersion.String()
		}
		mock.getAllStores = func() []*metapb.Store {
			// TiKV does not include 'v'.
			return []*metapb.Store{{Version: `1.0.0-alpha-271-g824ae7fd`}}
		}
		err := CheckClusterVersion(context.Background(), &mock, pdHTTP)
		c.Assert(err, check.ErrorMatches, "TiKV .* is not supported.*")
	}
}

func (s *checkSuite) TestCompareVersion(c *check.C) {
	c.Assert(semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	// BUG it should be "<" instead of ">".
	// c.Assert(semver.New("4.0.0-rc-35-g31dae220").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
}
