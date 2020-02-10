package cdc

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// CaptureInfoWatchResp represents the result of watching capture info
type CaptureInfoWatchResp struct {
	Info     *model.CaptureInfo
	IsDelete bool
	Err      error
}

// newCaptureInfoWatch return the existing CaptureInfo and continuous get update events from watchC.
// An error is returned if the underlay watchC from etcd return a error, or will closed normally withou
// returning an error when the ctx is Done.
func newCaptureInfoWatch(
	ctx context.Context, cli kv.CDCEtcdClient,
) (infos []*model.CaptureInfo, watchC <-chan *CaptureInfoWatchResp, err error) {
	resp, err := cli.Client.Get(ctx, kv.CaptureInfoKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, kv := range resp.Kvs {
		info := new(model.CaptureInfo)
		err := info.Unmarshal(kv.Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		infos = append(infos, info)
	}

	watchResp := make(chan *CaptureInfoWatchResp, 1)
	watchC = watchResp

	go func() {
		defer close(watchResp)

		revision := resp.Header.Revision
		etcdWatchC := cli.Client.Watch(ctx, kv.CaptureInfoKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(revision+1), clientv3.WithPrevKV())

		for resp := range etcdWatchC {
			failpoint.Inject("WatchCaptureInfoCompactionErr", func() {
				watchResp <- &CaptureInfoWatchResp{Err: errors.Trace(mvcc.ErrCompacted)}
				failpoint.Return()
			})
			if resp.Err() != nil {
				watchResp <- &CaptureInfoWatchResp{Err: errors.Trace(resp.Err())}
				return
			}
			for _, ev := range resp.Events {
				infoResp := new(CaptureInfoWatchResp)

				var data []byte
				switch ev.Type {
				case mvccpb.DELETE:
					infoResp.IsDelete = true
					data = ev.PrevKv.Value
				case mvccpb.PUT:
					data = ev.Kv.Value
				}
				infoResp.Info = new(model.CaptureInfo)
				err := infoResp.Info.Unmarshal(data)
				if err != nil {
					infoResp.Err = errors.Trace(err)
					watchResp <- infoResp
					return
				}
				watchResp <- infoResp
			}
		}
		log.Info("capture info watcher from etcd close normally")

	}()

	return
}
