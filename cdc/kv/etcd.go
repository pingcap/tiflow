// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	// EtcdKeyBase is the common prefix of the keys in CDC
	EtcdKeyBase = "/tidb/cdc"
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = EtcdKeyBase + "/capture/owner"
	// CaptureInfoKeyPrefix is the capture info path that is saved to etcd
	CaptureInfoKeyPrefix = EtcdKeyBase + "/capture/info"
)

// GetEtcdKeyChangeFeedList returns the prefix key of all changefeed config
func GetEtcdKeyChangeFeedList() string {
	return fmt.Sprintf("%s/changefeed/info", EtcdKeyBase)
}

// GetEtcdKeyChangeFeedInfo returns the key of a changefeed config
func GetEtcdKeyChangeFeedInfo(changefeedID string) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyChangeFeedList(), changefeedID)
}

// GetEtcdKeyChangeFeedStatus returns the key of a changefeed status
func GetEtcdKeyChangeFeedStatus(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/status/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyTaskStatusList returns the key of a task status without captureID part
func GetEtcdKeyTaskStatusList(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/task/status/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyTaskPositionList returns the key of a task position without captureID part
func GetEtcdKeyTaskPositionList(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/task/position/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyTaskStatus returns the key of a task status
func GetEtcdKeyTaskStatus(changefeedID, captureID string) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyTaskStatusList(changefeedID), captureID)
}

// GetEtcdKeyTaskPosition returns the key of a task position
func GetEtcdKeyTaskPosition(changefeedID, captureID string) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyTaskPositionList(changefeedID), captureID)
}

// GetEtcdKeyCaptureInfo returns the key of a capture info
func GetEtcdKeyCaptureInfo(id string) string {
	return CaptureInfoKeyPrefix + "/" + id
}

// CDCEtcdClient is a wrap of etcd client
type CDCEtcdClient struct {
	Client *clientv3.Client
}

//NewCDCEtcdClient returns a new CDCEtcdClient
func NewCDCEtcdClient(cli *clientv3.Client) CDCEtcdClient {
	return CDCEtcdClient{Client: cli}
}

// ClearAllCDCInfo delete all keys created by CDC
func (c CDCEtcdClient) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, EtcdKeyBase, clientv3.WithPrefix())
	return errors.Trace(err)
}

// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func (c CDCEtcdClient) GetChangeFeeds(ctx context.Context, opts ...clientv3.OpOption) (int64, map[string]*mvccpb.KeyValue, error) {
	key := GetEtcdKeyChangeFeedList()

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	revision := resp.Header.Revision
	details := make(map[string]*mvccpb.KeyValue, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := util.ExtractKeySuffix(string(kv.Key))
		if err != nil {
			return 0, nil, err
		}
		details[id] = kv
	}
	return revision, details, nil
}

// GetChangeFeedInfo queries the config of a given changefeed
func (c CDCEtcdClient) GetChangeFeedInfo(ctx context.Context, id string, opts ...clientv3.OpOption) (*model.ChangeFeedInfo, error) {
	key := GetEtcdKeyChangeFeedInfo(id)
	resp, err := c.Client.Get(ctx, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Count == 0 {
		return nil, errors.Annotatef(model.ErrChangeFeedNotExists, "query detail id %s", id)
	}
	detail := &model.ChangeFeedInfo{}
	err = detail.Unmarshal(resp.Kvs[0].Value)
	return detail, errors.Trace(err)
}

// DeleteChangeFeedInfo deletes a changefeed config from etcd
func (c CDCEtcdClient) DeleteChangeFeedInfo(ctx context.Context, id string, opts ...clientv3.OpOption) error {
	key := GetEtcdKeyChangeFeedInfo(id)
	_, err := c.Client.Delete(ctx, key, opts...)
	return errors.Trace(err)
}

// GetChangeFeedStatus queries the checkpointTs and resovledTs of a given changefeed
func (c CDCEtcdClient) GetChangeFeedStatus(ctx context.Context, id string, opts ...clientv3.OpOption) (*model.ChangeFeedStatus, error) {
	key := GetEtcdKeyChangeFeedStatus(id)
	resp, err := c.Client.Get(ctx, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Count == 0 {
		return nil, errors.Annotatef(model.ErrChangeFeedNotExists, "query status id %s", id)
	}
	info := &model.ChangeFeedStatus{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return info, errors.Trace(err)
}

// GetCaptures returns kv revision and CaptureInfo list
func (c CDCEtcdClient) GetCaptures(ctx context.Context, opts ...clientv3.OpOption) (int64, []*model.CaptureInfo, error) {
	key := CaptureInfoKeyPrefix

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	revision := resp.Header.Revision
	infos := make([]*model.CaptureInfo, 0, resp.Count)
	for _, kv := range resp.Kvs {
		info := &model.CaptureInfo{}
		err := info.Unmarshal(kv.Value)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		infos = append(infos, info)
	}
	return revision, infos, nil
}

// SaveChangeFeedInfo stores change feed info into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func (c CDCEtcdClient) SaveChangeFeedInfo(ctx context.Context, info *model.ChangeFeedInfo, changeFeedID string) error {
	key := GetEtcdKeyChangeFeedInfo(changeFeedID)
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, value)
	return errors.Trace(err)
}

// GetAllTaskPositions queries all task positions of a changefeed, and returns a map
// mapping from captureID to TaskPositions
func (c CDCEtcdClient) GetAllTaskPositions(ctx context.Context, changefeedID string, opts ...clientv3.OpOption) (map[string]*model.TaskPosition, error) {
	key := GetEtcdKeyTaskPositionList(changefeedID)
	resp, err := c.Client.Get(ctx, key, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	positions := make(map[string]*model.TaskPosition, resp.Count)
	for _, rawKv := range resp.Kvs {
		captureID, err := util.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		info := &model.TaskPosition{}
		err = info.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, err
		}
		positions[captureID] = info
	}
	return positions, nil
}

// GetAllTaskStatus queries all task status of a changefeed, and returns a map
// mapping from captureID to TaskStatus
func (c CDCEtcdClient) GetAllTaskStatus(ctx context.Context, changefeedID string, opts ...clientv3.OpOption) (model.ProcessorsInfos, error) {
	key := GetEtcdKeyTaskStatusList(changefeedID)
	resp, err := c.Client.Get(ctx, key, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pinfo := make(map[string]*model.TaskStatus, resp.Count)
	for _, rawKv := range resp.Kvs {
		captureID, err := util.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		info := &model.TaskStatus{}
		err = info.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, err
		}
		info.ModRevision = rawKv.ModRevision
		pinfo[captureID] = info
	}
	return pinfo, nil
}

// GetTaskStatus queries task status from etcd, returns
//  - ModRevision of the given key
//  - *model.TaskStatus unmarshaled from the value
//  - error if error happens
func (c CDCEtcdClient) GetTaskStatus(
	ctx context.Context,
	changefeedID string,
	captureID string,
	opts ...clientv3.OpOption,
) (int64, *model.TaskStatus, error) {
	key := GetEtcdKeyTaskStatus(changefeedID, captureID)
	resp, err := c.Client.Get(ctx, key, opts...)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if resp.Count == 0 {
		return 0, nil, errors.Annotatef(model.ErrTaskStatusNotExists, "changefeed: %s, capture: %s", changefeedID, captureID)
	}
	info := &model.TaskStatus{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return resp.Kvs[0].ModRevision, info, errors.Trace(err)
}

// PutTaskStatus puts task status into etcd.
func (c CDCEtcdClient) PutTaskStatus(
	ctx context.Context,
	changefeedID string,
	captureID string,
	info *model.TaskStatus,
	opts ...clientv3.OpOption,
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyTaskStatus(changefeedID, captureID)

	_, err = c.Client.Put(ctx, key, data)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// GetTaskPosition queries task process from etcd, returns
//  - ModRevision of the given key
//  - *model.TaskPosition unmarshaled from the value
//  - error if error happens
func (c CDCEtcdClient) GetTaskPosition(
	ctx context.Context,
	changefeedID string,
	captureID string,
	opts ...clientv3.OpOption,
) (int64, *model.TaskPosition, error) {
	key := GetEtcdKeyTaskPosition(changefeedID, captureID)
	resp, err := c.Client.Get(ctx, key, opts...)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if resp.Count == 0 {
		return 0, nil, errors.Annotatef(model.ErrTaskPositionNotExists, "changefeed: %s, capture: %s", changefeedID, captureID)
	}
	info := &model.TaskPosition{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return resp.Kvs[0].ModRevision, info, errors.Trace(err)
}

// PutTaskPosition puts task process into etcd.
func (c CDCEtcdClient) PutTaskPosition(
	ctx context.Context,
	changefeedID string,
	captureID string,
	info *model.TaskPosition,
	opts ...clientv3.OpOption,
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyTaskPosition(changefeedID, captureID)

	_, err = c.Client.Put(ctx, key, data)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// DeleteTaskPosition remove task position from etcd
func (c CDCEtcdClient) DeleteTaskPosition(ctx context.Context, changefeedID string, captureID string) error {
	key := GetEtcdKeyTaskPosition(changefeedID, captureID)
	_, err := c.Client.Delete(ctx, key)
	return errors.Trace(err)
}

// PutChangeFeedStatus puts changefeed synchronization status into etcd
func (c CDCEtcdClient) PutChangeFeedStatus(
	ctx context.Context,
	changefeedID string,
	status *model.ChangeFeedStatus,
	opts ...clientv3.OpOption,
) error {
	key := GetEtcdKeyChangeFeedStatus(changefeedID)
	value, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, value, opts...)
	return errors.Trace(err)
}

// DeleteTaskStatus deletes task status from etcd
func (c CDCEtcdClient) DeleteTaskStatus(
	ctx context.Context,
	cfID string,
	captureID string,
	opts ...clientv3.OpOption,
) error {
	key := GetEtcdKeyTaskStatus(cfID, captureID)
	_, err := c.Client.Delete(ctx, key)
	return errors.Trace(err)
}

// PutCaptureInfo put capture info into etcd.
func (c CDCEtcdClient) PutCaptureInfo(ctx context.Context, info *model.CaptureInfo, opts ...clientv3.OpOption) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyCaptureInfo(info.ID)
	_, err = c.Client.Put(ctx, key, string(data), opts...)
	return errors.Trace(err)
}

// DeleteCaptureInfo delete capture info from etcd.
func (c CDCEtcdClient) DeleteCaptureInfo(ctx context.Context, id string, opts ...clientv3.OpOption) error {
	key := GetEtcdKeyCaptureInfo(id)
	_, err := c.Client.Delete(ctx, key, opts...)
	return errors.Trace(err)
}

// GetCaptureInfo get capture info from etcd.
// return errCaptureNotExist if the capture not exists.
func (c CDCEtcdClient) GetCaptureInfo(ctx context.Context, id string, opts ...clientv3.OpOption) (info *model.CaptureInfo, err error) {
	key := GetEtcdKeyCaptureInfo(id)

	resp, err := c.Client.Get(ctx, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, model.ErrCaptureNotExist
	}

	info = new(model.CaptureInfo)
	err = info.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}
