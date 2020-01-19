package cdc

import (
	"context"
	"net/url"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type captureInfoSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&captureInfoSuite{})

func (ci *captureInfoSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	ci.clientURL, ci.etcd, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ci.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	ci.client = kv.NewCDCEtcdClient(client)
	ci.ctx, ci.cancel = context.WithCancel(context.Background())
	ci.errg = util.HandleErrWithErrGroup(ci.ctx, ci.etcd.Err(), func(e error) { c.Log(e) })
}

func (ci *captureInfoSuite) TearDownTest(c *check.C) {
	ci.etcd.Close()
	ci.cancel()
	err := ci.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

func (ci *captureInfoSuite) TestPutDeleteGet(c *check.C) {
	ctx := context.Background()

	id := "1"

	// get a not exist capture
	info, err := ci.client.GetCaptureInfo(ctx, id)
	c.Assert(err, check.Equals, model.ErrCaptureNotExist)
	c.Assert(info, check.IsNil)

	// create
	info = &model.CaptureInfo{
		ID: id,
	}
	err = ci.client.PutCaptureInfo(ctx, info)
	c.Assert(err, check.IsNil)

	// get again,
	getInfo, err := ci.client.GetCaptureInfo(ctx, id)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	// delete it
	err = ci.client.DeleteCaptureInfo(ctx, id)
	c.Assert(err, check.IsNil)
	// get again should not exist
	info, err = ci.client.GetCaptureInfo(ctx, id)
	c.Assert(err, check.Equals, model.ErrCaptureNotExist)
	c.Assert(info, check.IsNil)
}

func (ci *captureInfoSuite) TestWatch(c *check.C) {
	info1 := &model.CaptureInfo{ID: "1"}
	info2 := &model.CaptureInfo{ID: "2"}
	info3 := &model.CaptureInfo{ID: "3"}

	ctx := context.Background()
	var err error
	// put info1
	err = ci.client.PutCaptureInfo(ctx, info1)
	c.Assert(err, check.IsNil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	infos, watchC, err := newCaptureInfoWatch(watchCtx, ci.client)
	c.Assert(err, check.IsNil)
	// infos contains info1
	c.Assert(infos, check.HasLen, 1)
	c.Assert(infos[0], check.DeepEquals, info1)

	mustGetResp := func() *CaptureInfoWatchResp {
		select {
		case resp, ok := <-watchC:
			c.Assert(ok, check.IsTrue)
			return resp
		case <-time.After(time.Second * 5):
			c.Fatal("timeout to get resp from watchC")
			return nil
		}
	}

	mustClosed := func() {
		select {
		case _, ok := <-watchC:
			c.Assert(ok, check.IsFalse)
		case <-time.After(time.Second * 5):
			c.Fatal("timeout to get resp from watchC")

		}
	}

	// put info2 and info3
	err = ci.client.PutCaptureInfo(ctx, info2)
	c.Assert(err, check.IsNil)
	err = ci.client.PutCaptureInfo(ctx, info3)
	c.Assert(err, check.IsNil)

	resp := mustGetResp()
	c.Assert(resp.Err, check.IsNil)
	c.Assert(resp.IsDelete, check.IsFalse)
	c.Assert(resp.Info, check.DeepEquals, info2)

	resp = mustGetResp()
	c.Assert(resp.Err, check.IsNil)
	c.Assert(resp.IsDelete, check.IsFalse)
	c.Assert(resp.Info, check.DeepEquals, info3)

	// delete info2 and info3
	err = ci.client.DeleteCaptureInfo(ctx, info2.ID)
	c.Assert(err, check.IsNil)
	err = ci.client.DeleteCaptureInfo(ctx, info3.ID)
	c.Assert(err, check.IsNil)

	resp = mustGetResp()
	c.Assert(resp.Err, check.IsNil)
	c.Assert(resp.IsDelete, check.IsTrue)
	c.Assert(resp.Info, check.DeepEquals, info2)

	resp = mustGetResp()
	c.Assert(resp.Err, check.IsNil)
	c.Assert(resp.IsDelete, check.IsTrue)
	c.Assert(resp.Info, check.DeepEquals, info3)

	// cancel the watch
	watchCancel()
	mustClosed()
}
