package cdc

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/kv"
)

var captureEinfoKeyPrefix = kv.EtcdKeyBase + "/capture/info"
var errCaptureNotExist = errors.New("capture not exists")

func infoKey(id string) string {
	return captureEinfoKeyPrefix + "/" + id
}

// PutCaptureInfo put capture info into etcd.
func PutCaptureInfo(ctx context.Context, info *CaptureInfo, cli *clientv3.Client, opts ...clientv3.OpOption) error {
	var data []byte
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := infoKey(info.ID)
	_, err = cli.Put(ctx, key, string(data), opts...)
	return errors.Trace(err)
}

// DeleteCaptureInfo delete capture info from etcd.
func DeleteCaptureInfo(ctx context.Context, id string, cli *clientv3.Client, opts ...clientv3.OpOption) error {
	key := infoKey(id)
	_, err := cli.Delete(ctx, key, opts...)
	return errors.Trace(err)
}

// GetCaptureInfo get capture info from etcd.
// return errCaptureNotExist if the capture not exists.
func GetCaptureInfo(ctx context.Context, id string, cli *clientv3.Client, opts ...clientv3.OpOption) (info *CaptureInfo, err error) {
	key := infoKey(id)

	resp, err := cli.Get(ctx, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errCaptureNotExist
	}

	info = new(CaptureInfo)
	err = info.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// CaptureInfo store in etcd.
type CaptureInfo struct {
	ID string `json:"id"`
}

// Marshal using json.Marshal.
func (c *CaptureInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *CaptureInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

type CaptureInfoWatchResp struct {
	Info     *CaptureInfo
	IsDelete bool
	Err      error
}

func newCaptureInfoWatch(
	ctx context.Context, cli *clientv3.Client,
) (infos []*CaptureInfo, watchC <-chan *CaptureInfoWatchResp, err error) {
	resp, err := cli.Get(ctx, captureEinfoKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, kv := range resp.Kvs {
		info := new(CaptureInfo)
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
		etcdWatchC := cli.Watch(ctx, captureEinfoKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(revision+1), clientv3.WithPrevKV())

		for {
			select {
			case <-ctx.Done():
				watchResp <- &CaptureInfoWatchResp{Err: ctx.Err()}
				return
			case resp, ok := <-etcdWatchC:
				if !ok {
					// should meet resp.Err() before closed?
					return
				}

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
					infoResp.Info = new(CaptureInfo)
					err := infoResp.Info.Unmarshal(data)
					if err != nil {
						infoResp.Err = errors.Trace(err)
						watchResp <- infoResp
						return
					}
					watchResp <- infoResp
				}
			}
		}
	}()

	return
}
