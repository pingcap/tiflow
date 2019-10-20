package roles

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/pkg/etcd"
)

func Test(t *testing.T) { check.TestingT(t) }

type managerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&managerSuite{})

// Set up a embeded etcd using free ports.
func (s *managerSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	curl, e, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.clientURL = curl
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
