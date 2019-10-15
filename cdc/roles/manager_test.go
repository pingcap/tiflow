package roles

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/phayes/freeport"
	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type managerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&managerSuite{})

// getFreeListenURLs get free ports and localhost as url.
func getFreeListenURLs(c *check.C, n int) (urls []*url.URL) {
	ports, err := freeport.GetFreePorts(n)
	if err != nil {
		c.Fatal(err)
	}

	c.Log("get free ports:", ports)

	for _, port := range ports {
		u, err := url.Parse("http://localhost:" + strconv.Itoa(port))
		if err != nil {
			c.Fatal(err)
		}
		urls = append(urls, u)
	}

	return
}

// Set up a embeded etcd using free ports.
func (s *managerSuite) SetUpTest(c *check.C) {
	cfg := embed.NewConfig()
	cfg.Dir = c.MkDir()

	urls := getFreeListenURLs(c, 2)
	cfg.LPUrls = []url.URL{*urls[0]}
	cfg.LCUrls = []url.URL{*urls[1]}
	s.clientURL = urls[1]

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		c.Fatal(err)
	}

	select {
	case <-e.Server.ReadyNotify():
		c.Log("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		c.Log("Server took too long to start!")
	}

	s.etcd = e
	go func() {
		c.Log(<-e.Err())
	}()
}

func (s *managerSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
}

func (s *managerSuite) TestManager(c *check.C) {
	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		c.Fatal(err)
	}
	defer cli.Close()

	m1Ctx, m1cancel := context.WithCancel(context.Background())
	m1 := NewOwnerManager(cli, "m1", "/test/owner")
	c.Assert(m1.ID(), check.Equals, "m1")
	m2 := NewOwnerManager(cli, "m2", "/test/owner")
	c.Assert(m2.ID(), check.Equals, "m2")

	go func() {
		err := m1.CampaignOwner(m1Ctx)
		c.Assert(err, check.IsNil)
	}()

	go func() {
		// let m1 be owner first
		time.Sleep(1 * time.Second)

		err := m2.CampaignOwner(context.Background())
		c.Assert(err, check.IsNil)
	}()

	time.Sleep(time.Second)
	c.Assert(m1.IsOwner(), check.IsTrue)
	c.Assert(m2.IsOwner(), check.IsFalse)

	// stop m1 and m2 become owner
	m1cancel()
	c.Assert(err, check.IsNil)
	time.Sleep(time.Second)
	c.Assert(m1.IsOwner(), check.IsFalse)
	c.Assert(m2.IsOwner(), check.IsTrue)
}
