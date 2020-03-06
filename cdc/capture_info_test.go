package cdc

import (
	"context"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/mvcc"
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
	err = ci.client.PutCaptureInfo(ctx, info, 0)
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
	watcherRetry := int64(0)
	info1 := &model.CaptureInfo{ID: "1"}
	info2 := &model.CaptureInfo{ID: "2"}
	info3 := &model.CaptureInfo{ID: "3"}

	owner := &ownerImpl{
		etcdClient: ci.client,
		captures:   make(map[model.CaptureID]*model.CaptureInfo),
	}

	ctx := context.Background()
	var err error
	// put info1
	err = ci.client.PutCaptureInfo(ctx, info1, 0)
	c.Assert(err, check.IsNil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	infos, watchC, err := newCaptureInfoWatch(watchCtx, ci.client)
	c.Assert(err, check.IsNil)
	owner.captureWatchC = watchC
	// infos contains info1
	c.Assert(infos, check.HasLen, 1)
	c.Assert(infos[0], check.DeepEquals, info1)

	mustGetResp := func() *CaptureInfoWatchResp {
		select {
		case resp := <-owner.captureWatchC:
			err := resp.Err
			if err != nil && errors.Cause(err) == mvcc.ErrCompacted {
				atomic.AddInt64(&watcherRetry, 1)
				err2 := owner.resetCaptureInfoWatcher(watchCtx)
				c.Assert(err2, check.IsNil)
				return nil
			}
			c.Assert(err, check.IsNil)
			if resp.IsDelete {
				owner.removeCapture(resp.Info)
			} else {
				owner.addCapture(resp.Info)
			}
			return resp
		case <-time.After(time.Second * 2):
			c.Fatal("timeout to get resp from watchC")
			return nil
		}
	}

	mustClosed := func() {
		select {
		case _, ok := <-owner.captureWatchC:
			c.Assert(ok, check.IsFalse)
		case <-time.After(time.Second * 2):
			c.Fatal("timeout to get resp from watchC")
		}
	}

	checkCaptureLen := func(expected int) {
		owner.l.RLock()
		defer owner.l.RUnlock()
		c.Assert(owner.captures, check.HasLen, expected)
	}

	// put info2 and info3
	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/cdc/WatchCaptureInfoCompactionErr", "1*return"), check.IsNil)
	err = ci.client.PutCaptureInfo(ctx, info2, 0)
	c.Assert(err, check.IsNil)
	resp := mustGetResp()
	c.Assert(resp, check.IsNil)
	c.Assert(atomic.LoadInt64(&watcherRetry), check.Equals, int64(1))
	checkCaptureLen(2)
	c.Assert(failpoint.Disable("github.com/pingcap/ticdc/cdc/WatchCaptureInfoCompactionErr"), check.IsNil)

	err = ci.client.PutCaptureInfo(ctx, info3, 0)
	c.Assert(err, check.IsNil)
	resp = mustGetResp()
	c.Assert(resp.IsDelete, check.IsFalse)
	c.Assert(resp.Info, check.DeepEquals, info3)
	checkCaptureLen(3)

	// delete info2 and info3
	err = ci.client.DeleteCaptureInfo(ctx, info2.ID)
	c.Assert(err, check.IsNil)
	err = ci.client.DeleteCaptureInfo(ctx, info3.ID)
	c.Assert(err, check.IsNil)

	resp = mustGetResp()
	c.Assert(resp.IsDelete, check.IsTrue)
	c.Assert(resp.Info, check.DeepEquals, info2)
	checkCaptureLen(2)

	resp = mustGetResp()
	c.Assert(resp.IsDelete, check.IsTrue)
	c.Assert(resp.Info, check.DeepEquals, info3)
	checkCaptureLen(1)

	// cancel the watch
	watchCancel()
	mustClosed()
}
