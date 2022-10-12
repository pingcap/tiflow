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

package etcd

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = EtcdKeyBase + "/owner"
	// CaptureInfoKeyPrefix is the capture info path that is saved to etcd
	CaptureInfoKeyPrefix = EtcdKeyBase + "/capture"
	// TaskKeyPrefix is the prefix of task keys
	TaskKeyPrefix = EtcdKeyBase + "/task"
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
func GetEtcdKeyChangeFeedInfo(changefeedID model.ChangeFeedID) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyChangeFeedList(), changefeedID.ID)
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

// GetEtcdKeyJob returns the key for a job status
func GetEtcdKeyJob(changeFeedID model.ChangeFeedID) string {
	return JobKeyPrefix + "/" + changeFeedID.ID
}

// CDCEtcdClient is a wrap of etcd client
type CDCEtcdClient struct {
	Client *Client
}

// NewCDCEtcdClient returns a new CDCEtcdClient
func NewCDCEtcdClient(ctx context.Context, cli *clientv3.Client) CDCEtcdClient {
	metrics := map[string]prometheus.Counter{
		EtcdPut:    etcdRequestCounter.WithLabelValues(EtcdPut),
		EtcdGet:    etcdRequestCounter.WithLabelValues(EtcdGet),
		EtcdDel:    etcdRequestCounter.WithLabelValues(EtcdDel),
		EtcdTxn:    etcdRequestCounter.WithLabelValues(EtcdTxn),
		EtcdGrant:  etcdRequestCounter.WithLabelValues(EtcdGrant),
		EtcdRevoke: etcdRequestCounter.WithLabelValues(EtcdRevoke),
	}
	return CDCEtcdClient{Client: Wrap(cli, metrics)}
}

// Close releases resources in CDCEtcdClient
func (c CDCEtcdClient) Close() error {
	return c.Client.Unwrap().Close()
}

// ClearAllCDCInfo delete all keys created by CDC
func (c CDCEtcdClient) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, EtcdKeyBase, clientv3.WithPrefix())
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetAllCDCInfo get all keys created by CDC
func (c CDCEtcdClient) GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	resp, err := c.Client.Get(ctx, EtcdKeyBase, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	return resp.Kvs, nil
}

// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func (c CDCEtcdClient) GetChangeFeeds(ctx context.Context) (
	int64,
	map[model.ChangeFeedID]*mvccpb.KeyValue, error,
) {
	key := GetEtcdKeyChangeFeedList()

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	revision := resp.Header.Revision
	details := make(map[model.ChangeFeedID]*mvccpb.KeyValue, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := extractKeySuffix(string(kv.Key))
		if err != nil {
			return 0, nil, err
		}
		details[model.DefaultChangeFeedID(id)] = kv
	}
	return revision, details, nil
}

// GetAllChangeFeedInfo queries all changefeed information
func (c CDCEtcdClient) GetAllChangeFeedInfo(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
) {
	_, details, err := c.GetChangeFeeds(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allFeedInfo := make(map[model.ChangeFeedID]*model.ChangeFeedInfo, len(details))
	for id, rawDetail := range details {
		info := &model.ChangeFeedInfo{}
		if err := info.Unmarshal(rawDetail.Value); err != nil {
			return nil, errors.Trace(err)
		}
		allFeedInfo[id] = info
	}

	return allFeedInfo, nil
}

// GetChangeFeedInfo queries the config of a given changefeed
func (c CDCEtcdClient) GetChangeFeedInfo(ctx context.Context,
	id model.ChangeFeedID,
) (*model.ChangeFeedInfo, error) {
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
func (c CDCEtcdClient) DeleteChangeFeedInfo(ctx context.Context,
	id model.ChangeFeedID,
) error {
	key := GetEtcdKeyChangeFeedInfo(id)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetAllChangeFeedStatus queries all changefeed job status
func (c CDCEtcdClient) GetAllChangeFeedStatus(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedStatus, error,
) {
	key := JobKeyPrefix
	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	statuses := make(map[model.ChangeFeedID]*model.ChangeFeedStatus, resp.Count)
	for _, rawKv := range resp.Kvs {
		changefeedID, err := extractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		status := &model.ChangeFeedStatus{}
		err = status.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		statuses[model.DefaultChangeFeedID(changefeedID)] = status
	}
	return statuses, nil
}

// GetChangeFeedStatus queries the checkpointTs and resovledTs of a given changefeed
func (c CDCEtcdClient) GetChangeFeedStatus(ctx context.Context,
	id model.ChangeFeedID,
) (*model.ChangeFeedStatus, int64, error) {
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

// GetCaptureInfo get capture info from etcd.
// return ErrCaptureNotExist if the capture not exists.
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

// GetCaptureLeases returns a map mapping from capture ID to its lease
func (c CDCEtcdClient) GetCaptureLeases(ctx context.Context) (map[string]int64, error) {
	key := CaptureInfoKeyPrefix

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	leases := make(map[string]int64, resp.Count)
	for _, kv := range resp.Kvs {
		captureID, err := extractKeySuffix(string(kv.Key))
		if err != nil {
			return nil, err
		}
		leases[captureID] = kv.Lease
	}
	return leases, nil
}

// RevokeAllLeases revokes all leases passed from parameter
func (c CDCEtcdClient) RevokeAllLeases(ctx context.Context, leases map[string]int64) error {
	for _, lease := range leases {
		_, err := c.Client.Revoke(ctx, clientv3.LeaseID(lease))
		if err == nil {
			continue
		} else if etcdErr := err.(rpctypes.EtcdError); etcdErr.Code() == codes.NotFound {
			// it means the etcd lease is already expired or revoked
			continue
		}
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	return nil
}

// CreateChangefeedInfo creates a change feed info into etcd and fails if it is already exists.
func (c CDCEtcdClient) CreateChangefeedInfo(ctx context.Context,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	infoKey := GetEtcdKeyChangeFeedInfo(changeFeedID)
	jobKey := GetEtcdKeyJob(changeFeedID)
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", 0),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", 0),
	}
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, value),
	}
	resp, err := c.Client.Txn(ctx, cmps, opsThen, TxnEmptyOpsElse)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if !resp.Succeeded {
		log.Warn("changefeed already exists, ignore create changefeed",
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID))
		return cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changeFeedID)
	}
	return errors.Trace(err)
}

// SaveChangeFeedInfo stores change feed info into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func (c CDCEtcdClient) SaveChangeFeedInfo(ctx context.Context,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	key := GetEtcdKeyChangeFeedInfo(changeFeedID)
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, value)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
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
		changefeedID, err := extractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changefeedID) - 1
		captureID, err := extractKeySuffix(string(rawKv.Key[0:endIndex]))
		if err != nil {
			return nil, err
		}
		info := &model.ProcInfoSnap{
			CfID:      model.DefaultChangeFeedID(changefeedID),
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
		changeFeed, err := extractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := extractKeySuffix(string(rawKv.Key[0:endIndex]))
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

// GetTaskStatus queries task status from etcd, returns
//   - ModRevision of the given key
//   - *model.TaskStatus unmarshalled from the value
//   - error if error happens
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

// GetAllTaskPositions queries all task positions of a changefeed, and returns a map
// mapping from captureID to TaskPositions
func (c CDCEtcdClient) GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error) {
	resp, err := c.Client.Get(ctx, TaskPositionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	positions := make(map[string]*model.TaskPosition, resp.Count)
	for _, rawKv := range resp.Kvs {
		changeFeed, err := extractKeySuffix(string(rawKv.Key))
		if err != nil {
			return nil, err
		}
		endIndex := len(rawKv.Key) - len(changeFeed) - 1
		captureID, err := extractKeySuffix(string(rawKv.Key[0:endIndex]))
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

// GetTaskPosition queries task process from etcd, returns
//   - ModRevision of the given key
//   - *model.TaskPosition unmarshaled from the value
//   - error if error happens
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

// PutCaptureInfo put capture info into etcd,
// this happens when the capture starts.
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
func (c CDCEtcdClient) DeleteCaptureInfo(ctx context.Context, captureID string) error {
	key := GetEtcdKeyCaptureInfo(captureID)
	_, err := c.Client.Delete(ctx, key)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}

	// we need to clean all task position related to this capture when the capture is offline
	// otherwise the task positions may leak
	// the taskKey format is /tidb/cdc/task/position/{captureID}
	taskKey := fmt.Sprintf("%s/%s", TaskPositionKeyPrefix, captureID)
	_, err = c.Client.Delete(ctx, taskKey, clientv3.WithPrefix())
	if err != nil {
		log.Warn("delete task position failed",
			zap.String("captureID", captureID),
			zap.String("key", key), zap.Error(err))
	}
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
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

// GetOwnerRevision gets the Etcd revision for the elected owner.
func (c CDCEtcdClient) GetOwnerRevision(ctx context.Context, captureID string) (rev int64, err error) {
	resp, err := c.Client.Get(ctx, CaptureOwnerKey, clientv3.WithFirstCreate()...)
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if len(resp.Kvs) == 0 {
		return 0, cerror.ErrOwnerNotFound.GenWithStackByArgs()
	}
	// Checks that the given capture is indeed the owner.
	if string(resp.Kvs[0].Value) != captureID {
		return 0, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return resp.Kvs[0].ModRevision, nil
}

// getFreeListenURLs get free ports and localhost as url.
func getFreeListenURLs(n int) (urls []*url.URL, retErr error) {
	for i := 0; i < n; i++ {
		u, err := url.Parse(tempurl.Alloc())
		if err != nil {
			retErr = errors.Trace(err)
			return
		}
		urls = append(urls, u)
	}

	return
}

// SetupEmbedEtcd starts an embed etcd server
func SetupEmbedEtcd(dir string) (clientURL *url.URL, e *embed.Etcd, err error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir

	urls, err := getFreeListenURLs(2)
	if err != nil {
		return
	}
	cfg.LPUrls = []url.URL{*urls[0]}
	cfg.LCUrls = []url.URL{*urls[1]}
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	clientURL = urls[1]

	e, err = embed.StartEtcd(cfg)
	if err != nil {
		return
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		err = errors.New("server took too long to start")
	}

	return
}

// extractKeySuffix extracts the suffix of an etcd key, such as extracting
// "6a6c6dd290bc8732" from /tidb/cdc/changefeed/info/6a6c6dd290bc8732
func extractKeySuffix(key string) (string, error) {
	subs := strings.Split(key, "/")
	if len(subs) < 2 {
		return "", cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	return subs[len(subs)-1], nil
}
