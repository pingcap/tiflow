// Copyright 2020 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	// EtcdKeyBase is the common prefix of the keys in CDC
	EtcdKeyBase = "/tidb/cdc"
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = EtcdKeyBase + "/owner"
	// CaptureInfoKeyPrefix is the capture info path that is saved to etcd
	CaptureInfoKeyPrefix = EtcdKeyBase + "/capture"

	// TaskKeyPrefix is the prefix of task keys
	TaskKeyPrefix = EtcdKeyBase + "/task"

	// TaskWorkloadKeyPrefix is the prefix of task workload keys
	TaskWorkloadKeyPrefix = TaskKeyPrefix + "/workload"

	// TaskStatusKeyPrefix is the prefix of task status keys
	TaskStatusKeyPrefix = TaskKeyPrefix + "/status"

	// TaskPositionKeyPrefix is the prefix of task position keys
	TaskPositionKeyPrefix = TaskKeyPrefix + "/position"

	// JobKeyPrefix is the prefix of job keys
	JobKeyPrefix = EtcdKeyBase + "/job"
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
	return GetEtcdKeyJob(changefeedID)
}

// GetEtcdKeyTaskStatusList returns the key of a task status without captureID part
func GetEtcdKeyTaskStatusList(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/task/status/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyTaskPositionList returns the key of a task position without captureID part
func GetEtcdKeyTaskPositionList(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/task/position/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyTaskPosition returns the key of a task position
func GetEtcdKeyTaskPosition(changefeedID, captureID string) string {
	return TaskPositionKeyPrefix + "/" + captureID + "/" + changefeedID
}

// GetEtcdKeyCaptureInfo returns the key of a capture info
func GetEtcdKeyCaptureInfo(id string) string {
	return CaptureInfoKeyPrefix + "/" + id
}

// GetEtcdKeyTaskStatus returns the key for the task status
func GetEtcdKeyTaskStatus(changeFeedID, captureID string) string {
	return TaskStatusKeyPrefix + "/" + captureID + "/" + changeFeedID
}

// GetEtcdKeyTaskWorkload returns the key for the task workload
func GetEtcdKeyTaskWorkload(changeFeedID, captureID string) string {
	return TaskWorkloadKeyPrefix + "/" + captureID + "/" + changeFeedID
}

// GetEtcdKeyJob returns the key for a job status
func GetEtcdKeyJob(changeFeedID string) string {
	return JobKeyPrefix + "/" + changeFeedID
}

// CDCEtcdClient is a wrap of etcd client
type CDCEtcdClient struct {
	Client *etcd.Client
}

//NewCDCEtcdClient returns a new CDCEtcdClient
func NewCDCEtcdClient(cli *clientv3.Client) CDCEtcdClient {
	return CDCEtcdClient{Client: etcd.Wrap(cli)}
}

// ClearAllCDCInfo delete all keys created by CDC
func (c CDCEtcdClient) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, EtcdKeyBase, clientv3.WithPrefix())
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// RevokeAllLeases revokes all leases passed from parameter
func (c CDCEtcdClient) RevokeAllLeases(ctx context.Context, leases map[string]int64) error {
	for _, lease := range leases {
		_, err := c.Client.Revoke(ctx, clientv3.LeaseID(lease))
		if err != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return nil
}

// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func (c CDCEtcdClient) GetChangeFeeds(ctx context.Context) (int64, map[string]*mvccpb.KeyValue, error) {
	key := GetEtcdKeyChangeFeedList()

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	revision := resp.Header.Revision
	details := make(map[string]*mvccpb.KeyValue, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := model.ExtractKeySuffix(string(kv.Key))
		if err != nil {
			return 0, nil, err
		}
		details[id] = kv
	}
	return revision, details, nil
}

// GetChangeFeedInfo queries the config of a given changefeed
func (c CDCEtcdClient) GetChangeFeedInfo(ctx context.Context, id string) (*model.ChangeFeedInfo, error) {
	key := GetEtcdKeyChangeFeedInfo(id)
	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(key)
	}
	detail := &model.ChangeFeedInfo{}
	err = detail.Unmarshal(resp.Kvs[0].Value)
	return detail, errors.Trace(err)
}

// DeleteChangeFeedInfo deletes a changefeed config from etcd
func (c CDCEtcdClient) DeleteChangeFeedInfo(ctx context.Context, id string) error {
	key := GetEtcdKeyChangeFeedInfo(id)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetAllChangeFeedStatus queries all changefeed job status
func (c CDCEtcdClient) GetAllChangeFeedStatus(ctx context.Context) (map[string]*model.ChangeFeedStatus, error) {
	key := JobKeyPrefix
	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	statuses := make(map[string]*model.ChangeFeedStatus, resp.Count)
	for _, rawKv := range resp.Kvs {
		changefeedID, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		status := &model.ChangeFeedStatus{}
		err = status.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		statuses[changefeedID] = status
	}
	return statuses, nil
}

// GetChangeFeedStatus queries the checkpointTs and resovledTs of a given changefeed
func (c CDCEtcdClient) GetChangeFeedStatus(ctx context.Context, id string) (*model.ChangeFeedStatus, int64, error) {
	key := GetEtcdKeyJob(id)
	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return nil, 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, 0, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(key)
	}
	info := &model.ChangeFeedStatus{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return info, resp.Kvs[0].ModRevision, errors.Trace(err)
}

// GetCaptures returns kv revision and CaptureInfo list
func (c CDCEtcdClient) GetCaptures(ctx context.Context) (int64, []*model.CaptureInfo, error) {
	key := CaptureInfoKeyPrefix

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
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

// GetCaptureLeases returns a map mapping from capture ID to its lease
func (c CDCEtcdClient) GetCaptureLeases(ctx context.Context) (map[string]int64, error) {
	key := CaptureInfoKeyPrefix

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	leases := make(map[string]int64, resp.Count)
	for _, kv := range resp.Kvs {
		captureID, err := model.ExtractKeySuffix(string(kv.Key))
		if err != nil {
			return nil, err
		}
		leases[captureID] = kv.Lease
	}
	return leases, nil
}

// CreateChangefeedInfo creates a change feed info into etcd and fails if it is already exists.
func (c CDCEtcdClient) CreateChangefeedInfo(ctx context.Context, info *model.ChangeFeedInfo, changeFeedID string) error {
	if err := model.ValidateChangefeedID(changeFeedID); err != nil {
		return err
	}
	infoKey := GetEtcdKeyChangeFeedInfo(changeFeedID)
	jobKey := GetEtcdKeyJob(changeFeedID)
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := c.Client.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", 0),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", 0),
	).Then(
		clientv3.OpPut(infoKey, value),
	).Commit()
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if !resp.Succeeded {
		log.Warn("changefeed already exists, ignore create changefeed",
			zap.String("changefeedid", changeFeedID))
		return cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changeFeedID)
	}
	return errors.Trace(err)
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
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetAllTaskPositions queries all task positions of a changefeed, and returns a map
// mapping from captureID to TaskPositions
func (c CDCEtcdClient) GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error) {
	resp, err := c.Client.Get(ctx, TaskPositionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	positions := make(map[string]*model.TaskPosition, resp.Count)
	for _, rawKv := range resp.Kvs {
		changeFeed, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return nil, err
		}
		if changeFeed != changefeedID {
			continue
		}
		info := &model.TaskPosition{}
		err = info.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, cerror.ErrDecodeFailed.GenWithStackByArgs("failed to unmarshal task position: %s", err)
		}
		positions[captureID] = info
	}
	return positions, nil
}

// RemoveAllTaskPositions removes all task positions of a changefeed
func (c CDCEtcdClient) RemoveAllTaskPositions(ctx context.Context, changefeedID string) error {
	resp, err := c.Client.Get(ctx, TaskPositionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	for _, rawKv := range resp.Kvs {
		changeFeed, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return err
		}
		if changeFeed != changefeedID {
			continue
		}
		key := GetEtcdKeyTaskPosition(changefeedID, captureID)
		_, err = c.Client.Delete(ctx, key)
		if err != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return nil
}

// GetProcessors queries all processors of the cdc cluster,
// and returns a slice of ProcInfoSnap(without table info)
func (c CDCEtcdClient) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	resp, err := c.Client.Get(ctx, TaskStatusKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	infos := make([]*model.ProcInfoSnap, 0, resp.Count)
	for _, rawKv := range resp.Kvs {
		changefeedID, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changefeedID) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return nil, err
		}
		info := &model.ProcInfoSnap{
			CfID:      changefeedID,
			CaptureID: captureID,
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// GetAllTaskStatus queries all task status of a changefeed, and returns a map
// mapping from captureID to TaskStatus
func (c CDCEtcdClient) GetAllTaskStatus(ctx context.Context, changefeedID string) (model.ProcessorsInfos, error) {
	resp, err := c.Client.Get(ctx, TaskStatusKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	pinfo := make(map[string]*model.TaskStatus, resp.Count)
	for _, rawKv := range resp.Kvs {
		changeFeed, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return nil, err
		}
		if changeFeed != changefeedID {
			continue
		}
		info := &model.TaskStatus{}
		err = info.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, cerror.ErrDecodeFailed.GenWithStackByArgs("failed to unmarshal task status: %s", err)
		}
		info.ModRevision = rawKv.ModRevision
		pinfo[captureID] = info
	}
	return pinfo, nil
}

// RemoveAllTaskStatus removes all task status of a changefeed
func (c CDCEtcdClient) RemoveAllTaskStatus(ctx context.Context, changefeedID string) error {
	resp, err := c.Client.Get(ctx, TaskStatusKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	for _, rawKv := range resp.Kvs {
		changeFeed, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return err
		}
		if changeFeed != changefeedID {
			continue
		}
		key := GetEtcdKeyTaskStatus(changefeedID, captureID)
		_, err = c.Client.Delete(ctx, key)
		if err != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return nil
}

// GetTaskStatus queries task status from etcd, returns
//  - ModRevision of the given key
//  - *model.TaskStatus unmarshaled from the value
//  - error if error happens
func (c CDCEtcdClient) GetTaskStatus(
	ctx context.Context,
	changefeedID string,
	captureID string,
) (int64, *model.TaskStatus, error) {
	key := GetEtcdKeyTaskStatus(changefeedID, captureID)
	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return 0, nil, cerror.ErrTaskStatusNotExists.GenWithStackByArgs(key)
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
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyTaskStatus(changefeedID, captureID)

	_, err = c.Client.Put(ctx, key, data)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}

	return nil
}

// GetTaskWorkload queries task workload from etcd, returns
//  - model.TaskWorkload unmarshaled from the value
//  - error if error happens
func (c CDCEtcdClient) GetTaskWorkload(
	ctx context.Context,
	changefeedID string,
	captureID string,
) (model.TaskWorkload, error) {
	key := GetEtcdKeyTaskWorkload(changefeedID, captureID)
	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return make(model.TaskWorkload), nil
	}
	workload := make(model.TaskWorkload)
	err = workload.Unmarshal(resp.Kvs[0].Value)
	return workload, errors.Trace(err)
}

// PutTaskWorkload puts task workload into etcd.
func (c CDCEtcdClient) PutTaskWorkload(
	ctx context.Context,
	changefeedID string,
	captureID model.CaptureID,
	info *model.TaskWorkload,
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyTaskWorkload(changefeedID, captureID)

	_, err = c.Client.Put(ctx, key, data)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}

	return nil
}

// DeleteTaskWorkload deletes task workload from etcd
func (c CDCEtcdClient) DeleteTaskWorkload(
	ctx context.Context,
	changefeedID string,
	captureID string,
) error {
	key := GetEtcdKeyTaskWorkload(changefeedID, captureID)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetAllTaskWorkloads queries all task workloads of a changefeed, and returns a map
// mapping from captureID to TaskWorkloads
func (c CDCEtcdClient) GetAllTaskWorkloads(ctx context.Context, changefeedID string) (map[string]*model.TaskWorkload, error) {
	resp, err := c.Client.Get(ctx, TaskWorkloadKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	workloads := make(map[string]*model.TaskWorkload, resp.Count)
	for _, rawKv := range resp.Kvs {
		changeFeed, err := model.ExtractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := model.ExtractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return nil, err
		}
		if changeFeed != changefeedID {
			continue
		}
		info := &model.TaskWorkload{}
		err = info.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, cerror.ErrDecodeFailed.GenWithStackByArgs("failed to unmarshal task workload: %s", err)
		}
		workloads[captureID] = info
	}
	return workloads, nil
}

// UpdateTaskStatusFunc is a function that updates the task status
type UpdateTaskStatusFunc func(int64, *model.TaskStatus) (updated bool, err error)

// AtomicPutTaskStatus puts task status into etcd atomically.
func (c CDCEtcdClient) AtomicPutTaskStatus(
	ctx context.Context,
	changefeedID string,
	captureID string,
	updateFuncs ...UpdateTaskStatusFunc,
) (*model.TaskStatus, int64, error) {
	var status *model.TaskStatus
	var newModRevision int64
	err := retry.Run(100*time.Millisecond, 3, func() error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}
		var modRevision int64
		var err error
		modRevision, status, err = c.GetTaskStatus(ctx, changefeedID, captureID)
		key := GetEtcdKeyTaskStatus(changefeedID, captureID)
		var writeCmp clientv3.Cmp
		if err != nil {
			if cerror.ErrTaskStatusNotExists.NotEqual(err) {
				return errors.Trace(err)
			}
			status = new(model.TaskStatus)
			writeCmp = clientv3.Compare(clientv3.ModRevision(key), "=", 0)
		} else {
			writeCmp = clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)
		}
		updated := false
		for _, updateFunc := range updateFuncs {
			u, err := updateFunc(modRevision, status)
			if err != nil {
				return err
			}
			updated = updated || u
		}
		if !updated {
			return nil
		}
		value, err := status.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		resp, err := c.Client.Txn(ctx).If(writeCmp).Then(
			clientv3.OpPut(key, value),
		).Commit()

		if err != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}

		if !resp.Succeeded {
			log.Info("outdated table infos, ignore update taskStatus")
			return cerror.ErrWriteTsConflict.GenWithStackByArgs(key)
		}
		newModRevision = resp.Header.GetRevision()
		return nil
	})
	if err != nil {
		return nil, newModRevision, errors.Trace(err)
	}
	return status, newModRevision, nil
}

// GetTaskPosition queries task process from etcd, returns
//  - ModRevision of the given key
//  - *model.TaskPosition unmarshaled from the value
//  - error if error happens
func (c CDCEtcdClient) GetTaskPosition(
	ctx context.Context,
	changefeedID string,
	captureID string,
) (int64, *model.TaskPosition, error) {
	key := GetEtcdKeyTaskPosition(changefeedID, captureID)
	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return 0, nil, cerror.ErrTaskPositionNotExists.GenWithStackByArgs(key)
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
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyTaskPosition(changefeedID, captureID)

	_, err = c.Client.Put(ctx, key, data)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}

	return nil
}

// DeleteTaskPosition remove task position from etcd
func (c CDCEtcdClient) DeleteTaskPosition(ctx context.Context, changefeedID string, captureID string) error {
	key := GetEtcdKeyTaskPosition(changefeedID, captureID)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// RemoveChangeFeedStatus removes changefeed job status from etcd
func (c CDCEtcdClient) RemoveChangeFeedStatus(
	ctx context.Context,
	changefeedID string,
) error {
	key := GetEtcdKeyJob(changefeedID)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// PutChangeFeedStatus puts changefeed synchronization status into etcd
func (c CDCEtcdClient) PutChangeFeedStatus(
	ctx context.Context,
	changefeedID string,
	status *model.ChangeFeedStatus,
) error {
	key := GetEtcdKeyJob(changefeedID)
	value, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, value)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// SetChangeFeedStatusTTL sets the TTL of changefeed synchronization status
func (c CDCEtcdClient) SetChangeFeedStatusTTL(
	ctx context.Context,
	changefeedID string,
	ttl int64,
) error {
	key := GetEtcdKeyJob(changefeedID)
	leaseResp, err := c.Client.Grant(ctx, ttl)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	status, _, err := c.GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		return errors.Trace(err)
	}
	statusStr, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, statusStr, clientv3.WithLease(leaseResp.ID))
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// PutAllChangeFeedStatus puts ChangeFeedStatus of each changefeed into etcd
func (c CDCEtcdClient) PutAllChangeFeedStatus(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
	var (
		txn = c.Client.Txn(ctx)
		ops = make([]clientv3.Op, 0, embed.DefaultMaxTxnOps)
	)
	for changefeedID, info := range infos {
		storeVal, err := info.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		key := GetEtcdKeyJob(changefeedID)
		ops = append(ops, clientv3.OpPut(key, storeVal))
		if uint(len(ops)) >= embed.DefaultMaxTxnOps {
			_, err = txn.Then(ops...).Commit()
			if err != nil {
				return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
			}
			txn = c.Client.Txn(ctx)
			ops = ops[:0]
		}
	}
	if len(ops) > 0 {
		_, err := txn.Then(ops...).Commit()
		if err != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return nil
}

// DeleteTaskStatus deletes task status from etcd
func (c CDCEtcdClient) DeleteTaskStatus(
	ctx context.Context,
	cfID string,
	captureID string,
) error {
	key := GetEtcdKeyTaskStatus(cfID, captureID)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// PutCaptureInfo put capture info into etcd.
func (c CDCEtcdClient) PutCaptureInfo(ctx context.Context, info *model.CaptureInfo, leaseID clientv3.LeaseID) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyCaptureInfo(info.ID)
	_, err = c.Client.Put(ctx, key, string(data), clientv3.WithLease(leaseID))
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// DeleteCaptureInfo delete capture info from etcd.
func (c CDCEtcdClient) DeleteCaptureInfo(ctx context.Context, id string) error {
	key := GetEtcdKeyCaptureInfo(id)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetCaptureInfo get capture info from etcd.
// return errCaptureNotExist if the capture not exists.
func (c CDCEtcdClient) GetCaptureInfo(ctx context.Context, id string) (info *model.CaptureInfo, err error) {
	key := GetEtcdKeyCaptureInfo(id)

	resp, err := c.Client.Get(ctx, key)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, cerror.ErrCaptureNotExist.GenWithStackByArgs(key)
	}

	info = new(model.CaptureInfo)
	err = info.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// GetOwnerID returns the owner id by querying etcd
func (c CDCEtcdClient) GetOwnerID(ctx context.Context, key string) (string, error) {
	resp, err := c.Client.Get(ctx, key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if len(resp.Kvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}
