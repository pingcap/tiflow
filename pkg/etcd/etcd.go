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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/log"
<<<<<<< HEAD
=======
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
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

<<<<<<< HEAD
=======
// OwnerCaptureInfoClient is the sub interface of CDCEtcdClient that used for get owner capture information
type OwnerCaptureInfoClient interface {
	GetOwnerID(context.Context) (model.CaptureID, error)

	GetOwnerRevision(context.Context, model.CaptureID) (int64, error)

	GetCaptures(context.Context) (int64, []*model.CaptureInfo, error)
}

// CDCEtcdClient extracts CDCEtcdClients's method used for apiv2.
type CDCEtcdClient interface {
	OwnerCaptureInfoClient

	GetClusterID() string

	GetEtcdClient() *Client

	GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error)

	GetChangeFeedInfo(ctx context.Context,
		id model.ChangeFeedID,
	) (*model.ChangeFeedInfo, error)

	GetAllChangeFeedInfo(ctx context.Context) (
		map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
	)

	GetChangeFeedStatus(ctx context.Context,
		id model.ChangeFeedID,
	) (*model.ChangeFeedStatus, int64, error)

	GetUpstreamInfo(ctx context.Context,
		upstreamID model.UpstreamID,
		namespace string,
	) (*model.UpstreamInfo, error)

	GetGCServiceID() string

	GetEnsureGCServiceID(tag string) string

	SaveChangeFeedInfo(ctx context.Context,
		info *model.ChangeFeedInfo,
		changeFeedID model.ChangeFeedID,
	) error

	CreateChangefeedInfo(context.Context,
		*model.UpstreamInfo,
		*model.ChangeFeedInfo,
	) error

	UpdateChangefeedAndUpstream(ctx context.Context,
		upstreamInfo *model.UpstreamInfo,
		changeFeedInfo *model.ChangeFeedInfo,
	) error

	PutCaptureInfo(context.Context, *model.CaptureInfo, clientv3.LeaseID) error

	DeleteCaptureInfo(context.Context, model.CaptureID) error

	CheckMultipleCDCClusterExist(ctx context.Context) error
}

// CDCEtcdClientImpl is a wrap of etcd client
type CDCEtcdClientImpl struct {
	Client        *Client
	ClusterID     string
	etcdClusterID uint64
}

var _ CDCEtcdClient = (*CDCEtcdClientImpl)(nil)

>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
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
<<<<<<< HEAD
func (c CDCEtcdClient) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, EtcdKeyBase, clientv3.WithPrefix())
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
=======
func (c *CDCEtcdClientImpl) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, BaseKey(c.ClusterID), clientv3.WithPrefix())
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
}

// GetAllCDCInfo get all keys created by CDC
func (c CDCEtcdClient) GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	resp, err := c.Client.Get(ctx, EtcdKeyBase, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	return resp.Kvs, nil
}

<<<<<<< HEAD
=======
// CheckMultipleCDCClusterExist checks if other cdc clusters exists,
// and returns an error is so, and user should use --server instead
func (c *CDCEtcdClientImpl) CheckMultipleCDCClusterExist(ctx context.Context) error {
	resp, err := c.Client.Get(ctx, BaseKey(""),
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly())
	if err != nil {
		return errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.HasPrefix(key, BaseKey(DefaultCDCClusterID)) ||
			strings.HasPrefix(key, migrateBackupPrefix) {
			continue
		}
		// skip the reserved cluster id
		isReserved := false
		for _, reserved := range config.ReservedClusterIDs {
			if strings.HasPrefix(key, BaseKey(reserved)) {
				isReserved = true
				break
			}
		}
		if isReserved {
			continue
		}
		return errors.ErrMultipleCDCClustersExist.GenWithStackByArgs()
	}
	return nil
}

>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func (c CDCEtcdClient) GetChangeFeeds(ctx context.Context) (
	int64,
	map[model.ChangeFeedID]*mvccpb.KeyValue, error,
) {
	key := GetEtcdKeyChangeFeedList()

	resp, err := c.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
		return nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, errors.ErrChangeFeedNotExists.GenWithStackByArgs(key)
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
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
		return nil, 0, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, 0, errors.ErrChangeFeedNotExists.GenWithStackByArgs(key)
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
		return 0, nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
		return nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.ErrCaptureNotExist.GenWithStackByArgs(key)
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
		return nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
		return errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	return nil
}

// CreateChangefeedInfo creates a change feed info into etcd and fails if it is already exists.
<<<<<<< HEAD
func (c CDCEtcdClient) CreateChangefeedInfo(ctx context.Context,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	infoKey := GetEtcdKeyChangeFeedInfo(changeFeedID)
	jobKey := GetEtcdKeyJob(changeFeedID)
	value, err := info.Marshal()
=======
func (c *CDCEtcdClientImpl) CreateChangefeedInfo(
	ctx context.Context, upstreamInfo *model.UpstreamInfo, info *model.ChangeFeedInfo,
) error {
	return c.saveChangefeedAndUpstreamInfo(ctx, "Create", upstreamInfo, info)
}

// UpdateChangefeedAndUpstream updates the changefeed's info and its upstream info into etcd
func (c *CDCEtcdClientImpl) UpdateChangefeedAndUpstream(
	ctx context.Context, upstreamInfo *model.UpstreamInfo, changeFeedInfo *model.ChangeFeedInfo,
) error {
	return c.saveChangefeedAndUpstreamInfo(ctx, "Update", upstreamInfo, changeFeedInfo)
}

// saveChangefeedAndUpstreamInfo stores changefeed info and its upstream info into etcd
func (c *CDCEtcdClientImpl) saveChangefeedAndUpstreamInfo(
	ctx context.Context, operation string,
	upstreamInfo *model.UpstreamInfo, info *model.ChangeFeedInfo,
) error {
	cmps := []clientv3.Cmp{}
	opsThen := []clientv3.Op{}

	if upstreamInfo != nil {
		if info.UpstreamID != upstreamInfo.ID {
			return errors.ErrUpstreamMissMatch.GenWithStackByArgs(info.UpstreamID, upstreamInfo.ID)
		}
		upstreamInfoKey := CDCKey{
			Tp:         CDCKeyTypeUpStream,
			ClusterID:  c.ClusterID,
			UpstreamID: upstreamInfo.ID,
			Namespace:  info.Namespace,
		}
		upstreamEtcdKeyStr := upstreamInfoKey.String()
		upstreamResp, err := c.Client.Get(ctx, upstreamEtcdKeyStr)
		if err != nil {
			return errors.WrapError(errors.ErrPDEtcdAPIError, err)
		}
		upstreamData, err := upstreamInfo.Marshal()
		if err != nil {
			return errors.WrapError(errors.ErrPDEtcdAPIError, err)
		}

		if len(upstreamResp.Kvs) == 0 {
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(upstreamEtcdKeyStr), "=", 0))
			opsThen = append(opsThen, clientv3.OpPut(upstreamEtcdKeyStr, string(upstreamData)))
		} else {
			cmps = append(cmps,
				clientv3.Compare(clientv3.ModRevision(upstreamEtcdKeyStr), "=", upstreamResp.Kvs[0].ModRevision))
			if !bytes.Equal(upstreamResp.Kvs[0].Value, upstreamData) {
				opsThen = append(opsThen, clientv3.OpPut(upstreamEtcdKeyStr, string(upstreamData)))
			}
		}
	}

	changeFeedID := model.ChangeFeedID{
		Namespace: info.Namespace,
		ID:        info.ID,
	}
	infoKey := GetEtcdKeyChangeFeedInfo(c.ClusterID, changeFeedID)
	jobKey := GetEtcdKeyJob(c.ClusterID, changeFeedID)
	infoData, err := info.Marshal()
>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
	if err != nil {
		return errors.Trace(err)
	}

<<<<<<< HEAD
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", 0),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", 0),
	}
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, value),
	}
=======
	var infoModRevsion, jobModRevision int64
	if operation == "Update" {
		infoResp, err := c.Client.Get(ctx, infoKey)
		if err != nil {
			return errors.WrapError(errors.ErrPDEtcdAPIError, err)
		}
		if len(infoResp.Kvs) == 0 {
			return errors.ErrChangeFeedNotExists.GenWithStackByArgs(infoKey)
		}
		infoModRevsion = infoResp.Kvs[0].ModRevision

		jobResp, err := c.Client.Get(ctx, jobKey)
		if err != nil {
			return errors.WrapError(errors.ErrPDEtcdAPIError, err)
		}
		if len(jobResp.Kvs) == 0 {
			// Note that status may not exist, so we don't check it here.
			log.Debug("job status not exists", zap.Stringer("changefeed", changeFeedID))
		} else {
			jobModRevision = jobResp.Kvs[0].ModRevision
		}
	}

	cmps = append(cmps,
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", infoModRevsion),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", jobModRevision),
	)
	opsThen = append(opsThen, clientv3.OpPut(infoKey, infoData))

>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
	resp, err := c.Client.Txn(ctx, cmps, opsThen, TxnEmptyOpsElse)
	if err != nil {
		return errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	if !resp.Succeeded {
		log.Warn(fmt.Sprintf("unexpected etcd transaction failure, operation: %s", operation),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID))
		errMsg := fmt.Sprintf("%s changefeed %s", operation, changeFeedID)
		return errors.ErrMetaOpFailed.GenWithStackByArgs(errMsg)
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
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
}

// DeleteCaptureInfo delete capture info from etcd.
func (c CDCEtcdClient) DeleteCaptureInfo(ctx context.Context, captureID string) error {
	key := GetEtcdKeyCaptureInfo(captureID)
	_, err := c.Client.Delete(ctx, key)
	if err != nil {
		return errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
}

// GetOwnerID returns the owner id by querying etcd
func (c CDCEtcdClient) GetOwnerID(ctx context.Context, key string) (string, error) {
	resp, err := c.Client.Get(ctx, key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.WrapError(errors.ErrPDEtcdAPIError, err)
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
		return 0, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	if len(resp.Kvs) == 0 {
		return 0, errors.ErrOwnerNotFound.GenWithStackByArgs()
	}
	// Checks that the given capture is indeed the owner.
	if string(resp.Kvs[0].Value) != captureID {
		return 0, errors.ErrNotOwner.GenWithStackByArgs()
	}
	return resp.Kvs[0].ModRevision, nil
}

<<<<<<< HEAD
=======
// GetGCServiceID returns the cdc gc service ID
func (c *CDCEtcdClientImpl) GetGCServiceID() string {
	return fmt.Sprintf("ticdc-%s-%d", c.ClusterID, c.etcdClusterID)
}

// GetEnsureGCServiceID return the prefix for the gc service id when changefeed is creating
func (c *CDCEtcdClientImpl) GetEnsureGCServiceID(tag string) string {
	return c.GetGCServiceID() + tag
}

// GetUpstreamInfo get a upstreamInfo from etcd server
func (c *CDCEtcdClientImpl) GetUpstreamInfo(ctx context.Context,
	upstreamID model.UpstreamID,
	namespace string,
) (*model.UpstreamInfo, error) {
	Key := CDCKey{
		Tp:         CDCKeyTypeUpStream,
		ClusterID:  c.ClusterID,
		UpstreamID: upstreamID,
		Namespace:  namespace,
	}
	KeyStr := Key.String()
	resp, err := c.Client.Get(ctx, KeyStr)
	if err != nil {
		return nil, errors.WrapError(errors.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, errors.ErrUpstreamNotFound.GenWithStackByArgs(KeyStr)
	}
	info := &model.UpstreamInfo{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return info, errors.Trace(err)
}

// GcServiceIDForTest returns the gc service ID for tests
func GcServiceIDForTest() string {
	return fmt.Sprintf("ticdc-%s-%d", "default", 0)
}

>>>>>>> c5d5eff1f2 (api(ticdc): only update upstreamInfo that has changed (#10422))
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
		return "", errors.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	return subs[len(subs)-1], nil
}
