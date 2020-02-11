package roles

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

func Test(t *testing.T) { check.TestingT(t) }

type managerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&managerSuite{})

// Set up a embeded etcd using free ports.
func (s *managerSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.etcd, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.etcd.Err(), func(e error) { c.Log(e) })
}

func (s *managerSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

func (s *managerSuite) TestManager(c *check.C) {
	curl := s.clientURL.String()
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		c.Fatal(err)
	}
	defer etcdCli.Close()
	cli := kv.NewCDCEtcdClient(etcdCli)

	m1Ctx, m1cancel := context.WithCancel(context.Background())
	m1 := NewOwnerManager(cli, "m1", "/test/owner")
	c.Assert(m1.ID(), check.Equals, "m1")
	m2 := NewOwnerManager(cli, "m2", "/test/owner")
	c.Assert(m2.ID(), check.Equals, "m2")

	go func() {
		err := m1.CampaignOwner(m1Ctx)
		c.Assert(err, check.IsNil)
	}()
	// m1 worker
	m1DoneCh := make(chan struct{}, 1)
	go func() {
		w1ctx, w1cancel := context.WithCancel(context.Background())
		defer w1cancel()
		select {
		case <-w1ctx.Done():
		case <-m1.RetireNotify():
			w1cancel()
		}
		m1DoneCh <- struct{}{}
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

	// can't resign non-owner
	err = m2.ResignOwner(context.Background())
	c.Assert(errors.Cause(err), check.Equals, concurrency.ErrElectionNotLeader)

	// stop m1 and m2 become owner
	m1cancel()
	select {
	case <-m1DoneCh:
	case <-time.After(time.Millisecond * 10):
		c.Fatal("m1 worker not exits")
	}
	time.Sleep(time.Second)
	c.Assert(m1.IsOwner(), check.IsFalse)
	c.Assert(m2.IsOwner(), check.IsTrue)

	resp, err := cli.Client.Get(context.Background(), kv.CaptureOwnerKey+"/"+m2.ID())
	c.Assert(err, check.IsNil)
	rev1 := resp.Header.Revision

	// resign owner, it will re-campaign to be owner
	err = m2.ResignOwner(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return m2.IsOwner()
	}), check.IsTrue)

	// check m2 is re-campaigned
	resp, err = cli.Client.Get(context.Background(), kv.CaptureOwnerKey+"/"+m2.ID())
	c.Assert(err, check.IsNil)
	rev2 := resp.Header.Revision
	c.Assert(rev1, check.Less, rev2)
}
