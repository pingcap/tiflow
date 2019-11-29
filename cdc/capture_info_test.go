package cdc

import (
	"context"
	"net/url"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type captureInfoSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
	client    *clientv3.Client
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
	ci.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{ci.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
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
	info, err := GetCaptureInfo(ctx, id, ci.client)
	c.Assert(err, check.Equals, errCaptureNotExist)
	c.Assert(info, check.IsNil)

	// create
	info = &model.CaptureInfo{
		ID: id,
	}
	err = PutCaptureInfo(ctx, info, ci.client)
	c.Assert(err, check.IsNil)

	// get again,
	getInfo, err := GetCaptureInfo(ctx, id, ci.client)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	// delete it
	err = DeleteCaptureInfo(ctx, id, ci.client)
	c.Assert(err, check.IsNil)
	// get again should not exist
	info, err = GetCaptureInfo(ctx, id, ci.client)
	c.Assert(err, check.Equals, errCaptureNotExist)
	c.Assert(info, check.IsNil)
}

func (ci *captureInfoSuite) TestWatch(c *check.C) {
	info1 := &model.CaptureInfo{ID: "1"}
	info2 := &model.CaptureInfo{ID: "2"}
	info3 := &model.CaptureInfo{ID: "3"}

	ctx := context.Background()
	var err error
	// put info1
	err = PutCaptureInfo(ctx, info1, ci.client)
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
	err = PutCaptureInfo(ctx, info2, ci.client)
	c.Assert(err, check.IsNil)
	err = PutCaptureInfo(ctx, info3, ci.client)
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
	err = DeleteCaptureInfo(ctx, info2.ID, ci.client)
	c.Assert(err, check.IsNil)
	err = DeleteCaptureInfo(ctx, info3.ID, ci.client)
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
