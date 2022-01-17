package srvdiscovery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/atomic"
)

const defaultWatchChanSize = 8

// EtcdSrvDiscovery implements Discovery interface based on etcd as backend storage
// Note this struct is not thread-safe, and can be watched only once.
type EtcdSrvDiscovery struct {
	keyAdapter   adapter.KeyAdapter
	etcdCli      *clientv3.Client
	snapshot     map[UUID]ServiceResource
	snapshotRev  Revision
	watchTickDur time.Duration
	watched      *atomic.Bool
}

// NewEtcdSrvDiscovery creates a new EtcdSrvDiscovery
func NewEtcdSrvDiscovery(etcdCli *clientv3.Client, ka adapter.KeyAdapter, watchTickDur time.Duration) *EtcdSrvDiscovery {
	return &EtcdSrvDiscovery{
		keyAdapter:   ka,
		etcdCli:      etcdCli,
		watchTickDur: watchTickDur,
		watched:      atomic.NewBool(false),
	}
}

// Snapshot implements Discovery.Snapshot
func (d *EtcdSrvDiscovery) Snapshot(ctx context.Context, updateCache bool) (map[UUID]ServiceResource, error) {
	snapshot, revision, err := d.getSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if updateCache {
		d.snapshot = snapshot
		d.snapshotRev = revision
	}
	return snapshot, nil
}

// Watch implements Discovery.Watch, note when `Watch` starts, we should avoid
// to get Snapshot with `updateCache=true`
func (d *EtcdSrvDiscovery) Watch(ctx context.Context) <-chan WatchResp {
	notWatched := d.watched.CAS(false, true)
	if !notWatched {
		ch := make(chan WatchResp, 1)
		ch <- WatchResp{err: errors.ErrDiscoveryDuplicateWatch.GenWithStackByArgs()}
		return ch
	}
	ch := make(chan WatchResp, defaultWatchChanSize)
	go d.tickedWatch(ctx, ch)
	return ch
}

func (d *EtcdSrvDiscovery) tickedWatch(ctx context.Context, ch chan<- WatchResp) {
	ticker := time.NewTicker(d.watchTickDur)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ch <- WatchResp{err: ctx.Err()}
			return
		case <-ticker.C:
			addSet, delSet, err := d.delta(ctx)
			if err != nil {
				ch <- WatchResp{err: err}
				return
			}
			if len(addSet) > 0 || len(delSet) > 0 {
				ch <- WatchResp{
					addSet: addSet,
					delSet: delSet,
				}
			}
		}
	}
}

// delta returns snapshot diff compared with current snapshot, note delta will
// update current snapshot and revision.
func (d *EtcdSrvDiscovery) delta(ctx context.Context) (
	map[UUID]ServiceResource, map[UUID]ServiceResource, error,
) {
	snapshot, revision, err := d.getSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	addSet := make(map[UUID]ServiceResource)
	delSet := make(map[UUID]ServiceResource)
	for k, v := range snapshot {
		if _, ok := d.snapshot[k]; !ok {
			addSet[k] = v
		}
	}
	for k, v := range d.snapshot {
		if _, ok := snapshot[k]; !ok {
			delSet[k] = v
		}
	}
	d.snapshot = snapshot
	d.snapshotRev = revision
	return addSet, delSet, nil
}

// getSnapshot queries etcd and get a full set of service resource
func (d *EtcdSrvDiscovery) getSnapshot(ctx context.Context) (
	map[UUID]ServiceResource, Revision, error,
) {
	resp, err := d.etcdCli.Get(ctx, d.keyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, errors.Wrap(errors.ErrEtcdAPIError, err)
	}
	snapshot := make(map[UUID]ServiceResource, resp.Count)
	for _, kv := range resp.Kvs {
		uuid, resc, err := unmarshal(d.keyAdapter, kv.Key, kv.Value)
		if err != nil {
			return nil, 0, err
		}
		snapshot[uuid] = resc
	}
	return snapshot, resp.Header.Revision, nil
}

// unmarshal wraps the unmarshal processing for key/value used in service discovery
func unmarshal(ka adapter.KeyAdapter, k, v []byte) (uuid UUID, resc ServiceResource, err error) {
	keys, err1 := ka.Decode(string(k))
	if err1 != nil {
		err = errors.Wrap(errors.ErrDecodeEtcdKeyFail, err1, string(k))
		return
	}
	uuid = keys[len(keys)-1]
	resc = ServiceResource{}
	err = json.Unmarshal(v, &resc)
	if err != nil {
		err = errors.Wrap(errors.ErrDecodeEtcdValueFail, err, string(v))
		return
	}
	return
}
