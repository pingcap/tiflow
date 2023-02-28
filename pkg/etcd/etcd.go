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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

// DefaultCDCClusterID is the default value of cdc cluster id
const DefaultCDCClusterID = "default"

// CaptureOwnerKey is the capture owner path that is saved to etcd
func CaptureOwnerKey(clusterID string) string {
	return BaseKey(clusterID) + metaPrefix + "/owner"
}

// CaptureInfoKeyPrefix is the capture info path that is saved to etcd
func CaptureInfoKeyPrefix(clusterID string) string {
	return BaseKey(clusterID) + metaPrefix + captureKey
}

// TaskPositionKeyPrefix is the prefix of task position keys
func TaskPositionKeyPrefix(clusterID, namespace string) string {
	return NamespacedPrefix(clusterID, namespace) + taskPositionKey
}

// ChangefeedStatusKeyPrefix is the prefix of changefeed status keys
func ChangefeedStatusKeyPrefix(clusterID, namespace string) string {
	return NamespacedPrefix(clusterID, namespace) + ChangefeedStatusKey
}

// GetEtcdKeyChangeFeedList returns the prefix key of all changefeed config
func GetEtcdKeyChangeFeedList(clusterID, namespace string) string {
	return fmt.Sprintf("%s/changefeed/info", NamespacedPrefix(clusterID, namespace))
}

// GetEtcdKeyChangeFeedInfo returns the key of a changefeed config
func GetEtcdKeyChangeFeedInfo(clusterID string, changefeedID model.ChangeFeedID) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyChangeFeedList(clusterID,
		changefeedID.Namespace), changefeedID.ID)
}

// GetEtcdKeyTaskPosition returns the key of a task position
func GetEtcdKeyTaskPosition(clusterID string,
	changefeedID model.ChangeFeedID,
	captureID string,
) string {
	return TaskPositionKeyPrefix(clusterID, changefeedID.Namespace) +
		"/" + captureID + "/" + changefeedID.ID
}

// GetEtcdKeyCaptureInfo returns the key of a capture info
func GetEtcdKeyCaptureInfo(clusterID, id string) string {
	return CaptureInfoKeyPrefix(clusterID) + "/" + id
}

// GetEtcdKeyJob returns the key for a job status
func GetEtcdKeyJob(clusterID string, changeFeedID model.ChangeFeedID) string {
	return ChangefeedStatusKeyPrefix(clusterID, changeFeedID.Namespace) + "/" + changeFeedID.ID
}

// MigrateBackupKey is the key of backup data during a migration.
func MigrateBackupKey(version int, backupKey string) string {
	if strings.HasPrefix(backupKey, "/") {
		return fmt.Sprintf("%s/%d%s", migrateBackupPrefix, version, backupKey)
	}
	return fmt.Sprintf("%s/%d/%s", migrateBackupPrefix, version, backupKey)
}

// CDCEtcdClient extracts CDCEtcdClients's method used for apiv2.
type CDCEtcdClient interface {
	GetClusterID() string

	GetEtcdClient() *Client

	GetOwnerID(context.Context) (model.CaptureID, error)

	GetOwnerRevision(context.Context, model.CaptureID) (int64, error)

	GetCaptures(context.Context) (int64, []*model.CaptureInfo, error)

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
		model.ChangeFeedID,
	) error

	UpdateChangefeedAndUpstream(ctx context.Context,
		upstreamInfo *model.UpstreamInfo,
		changeFeedInfo *model.ChangeFeedInfo,
		changeFeedID model.ChangeFeedID,
	) error

	PutCaptureInfo(context.Context, *model.CaptureInfo, clientv3.LeaseID) error

	DeleteCaptureInfo(context.Context, model.CaptureID) error

	CheckMultipleCDCClusterExist(ctx context.Context) error

	Close() error
}

// CDCEtcdClientImpl is a wrap of etcd client
type CDCEtcdClientImpl struct {
	Client        *Client
	ClusterID     string
	etcdClusterID uint64
}

var _ CDCEtcdClient = (*CDCEtcdClientImpl)(nil)

// NewCDCEtcdClient returns a new CDCEtcdClient
func NewCDCEtcdClient(ctx context.Context,
	cli *clientv3.Client,
	clusterID string,
) (*CDCEtcdClientImpl, error) {
	metrics := map[string]prometheus.Counter{
		EtcdPut:    etcdRequestCounter.WithLabelValues(EtcdPut),
		EtcdGet:    etcdRequestCounter.WithLabelValues(EtcdGet),
		EtcdDel:    etcdRequestCounter.WithLabelValues(EtcdDel),
		EtcdTxn:    etcdRequestCounter.WithLabelValues(EtcdTxn),
		EtcdGrant:  etcdRequestCounter.WithLabelValues(EtcdGrant),
		EtcdRevoke: etcdRequestCounter.WithLabelValues(EtcdRevoke),
	}
	resp, err := cli.MemberList(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CDCEtcdClientImpl{
		etcdClusterID: resp.Header.ClusterId,
		Client:        Wrap(cli, metrics),
		ClusterID:     clusterID,
	}, nil
}

// Close releases resources in CDCEtcdClient
func (c *CDCEtcdClientImpl) Close() error {
	return c.Client.Unwrap().Close()
}

// ClearAllCDCInfo delete all keys created by CDC
func (c *CDCEtcdClientImpl) ClearAllCDCInfo(ctx context.Context) error {
	_, err := c.Client.Delete(ctx, BaseKey(c.ClusterID), clientv3.WithPrefix())
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetClusterID gets CDC cluster ID.
func (c *CDCEtcdClientImpl) GetClusterID() string {
	return c.ClusterID
}

// GetEtcdClient gets Client.
func (c *CDCEtcdClientImpl) GetEtcdClient() *Client {
	return c.Client
}

// GetAllCDCInfo get all keys created by CDC
func (c *CDCEtcdClientImpl) GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	resp, err := c.Client.Get(ctx, BaseKey(c.ClusterID), clientv3.WithPrefix())
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	return resp.Kvs, nil
}

// CheckMultipleCDCClusterExist checks if other cdc clusters exists,
// and returns an error is so, and user should use --server instead
func (c *CDCEtcdClientImpl) CheckMultipleCDCClusterExist(ctx context.Context) error {
	resp, err := c.Client.Get(ctx, BaseKey(""),
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly())
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
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
		return cerror.ErrMultipleCDCClustersExist.GenWithStackByArgs()
	}
	return nil
}

// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func (c *CDCEtcdClientImpl) GetChangeFeeds(ctx context.Context) (
	int64,
	map[model.ChangeFeedID]*mvccpb.KeyValue, error,
) {
	// todo: support namespace
	key := GetEtcdKeyChangeFeedList(c.ClusterID, model.DefaultNamespace)

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
func (c *CDCEtcdClientImpl) GetAllChangeFeedInfo(ctx context.Context) (
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
func (c *CDCEtcdClientImpl) GetChangeFeedInfo(ctx context.Context,
	id model.ChangeFeedID,
) (*model.ChangeFeedInfo, error) {
	key := GetEtcdKeyChangeFeedInfo(c.ClusterID, id)
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
func (c *CDCEtcdClientImpl) DeleteChangeFeedInfo(ctx context.Context,
	id model.ChangeFeedID,
) error {
	key := GetEtcdKeyChangeFeedInfo(c.ClusterID, id)
	_, err := c.Client.Delete(ctx, key)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetChangeFeedStatus queries the checkpointTs and resovledTs of a given changefeed
func (c *CDCEtcdClientImpl) GetChangeFeedStatus(ctx context.Context,
	id model.ChangeFeedID,
) (*model.ChangeFeedStatus, int64, error) {
	key := GetEtcdKeyJob(c.ClusterID, id)
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
func (c *CDCEtcdClientImpl) GetCaptures(ctx context.Context) (int64, []*model.CaptureInfo, error) {
	key := CaptureInfoKeyPrefix(c.ClusterID)

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
func (c *CDCEtcdClientImpl) GetCaptureInfo(
	ctx context.Context, id string,
) (info *model.CaptureInfo, err error) {
	key := GetEtcdKeyCaptureInfo(c.ClusterID, id)

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
func (c *CDCEtcdClientImpl) GetCaptureLeases(ctx context.Context) (map[string]int64, error) {
	key := CaptureInfoKeyPrefix(c.ClusterID)

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
func (c *CDCEtcdClientImpl) RevokeAllLeases(ctx context.Context, leases map[string]int64) error {
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
func (c *CDCEtcdClientImpl) CreateChangefeedInfo(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	infoKey := GetEtcdKeyChangeFeedInfo(c.ClusterID, changeFeedID)
	jobKey := GetEtcdKeyJob(c.ClusterID, changeFeedID)
	upstreamInfoKey := CDCKey{
		Tp:         CDCKeyTypeUpStream,
		ClusterID:  c.ClusterID,
		UpstreamID: upstreamInfo.ID,
		Namespace:  changeFeedID.Namespace,
	}
	upstreamEtcdKeyStr := upstreamInfoKey.String()
	info.UpstreamID = upstreamInfo.ID
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	upstreamResp, err := c.Client.Get(ctx, upstreamEtcdKeyStr)
	if err != nil {
		return errors.Trace(err)
	}
	upstreamData, err := upstreamInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(infoKey), "=", 0),
		clientv3.Compare(clientv3.ModRevision(jobKey), "=", 0),
	}
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, value),
		clientv3.OpPut(upstreamEtcdKeyStr, string(upstreamData)),
	}
	if len(upstreamResp.Kvs) == 0 {
		cmps = append(cmps,
			clientv3.Compare(clientv3.ModRevision(upstreamEtcdKeyStr),
				"=", 0))
	} else {
		cmps = append(cmps,
			clientv3.Compare(clientv3.ModRevision(upstreamInfoKey.String()),
				"=", upstreamResp.Kvs[0].ModRevision))
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

// UpdateChangefeedAndUpstream updates the changefeed's info and its upstream info into etcd
func (c *CDCEtcdClientImpl) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	infoKey := GetEtcdKeyChangeFeedInfo(c.ClusterID, changeFeedID)
	changeFeedInfoStr, err := changeFeedInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	upstreamKey := CDCKey{
		Tp:         CDCKeyTypeUpStream,
		ClusterID:  c.ClusterID,
		UpstreamID: upstreamInfo.ID,
		Namespace:  changeFeedID.Namespace,
	}
	upstreamKeyStr := upstreamKey.String()
	upstreamInfoStr, err := upstreamInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, changeFeedInfoStr),
		clientv3.OpPut(upstreamKeyStr, string(upstreamInfoStr)),
	}

	resp, err := c.Client.Txn(ctx, txnEmptyCmps, opsThen, TxnEmptyOpsElse)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if !resp.Succeeded {
		log.Warn("unexpected etcd transaction failure",
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID))
		return cerror.ErrChangefeedUpdateFailedTransaction.GenWithStackByArgs(changeFeedID)
	}
	return nil
}

// SaveChangeFeedInfo stores change feed info into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func (c *CDCEtcdClientImpl) SaveChangeFeedInfo(ctx context.Context,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	key := GetEtcdKeyChangeFeedInfo(c.ClusterID, changeFeedID)
	value, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = c.Client.Put(ctx, key, value)
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// PutCaptureInfo put capture info into etcd,
// this happens when the capture starts.
func (c *CDCEtcdClientImpl) PutCaptureInfo(
	ctx context.Context, info *model.CaptureInfo, leaseID clientv3.LeaseID,
) error {
	data, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	key := GetEtcdKeyCaptureInfo(c.ClusterID, info.ID)
	_, err = c.Client.Put(ctx, key, string(data), clientv3.WithLease(leaseID))
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// DeleteCaptureInfo delete all capture related info from etcd.
func (c *CDCEtcdClientImpl) DeleteCaptureInfo(ctx context.Context, captureID string) error {
	key := GetEtcdKeyCaptureInfo(c.ClusterID, captureID)
	_, err := c.Client.Delete(ctx, key)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	// we need to clean all task position related to this capture when the capture is offline
	// otherwise the task positions may leak
	// FIXME (dongmen 2022.9.28): find a way to use changefeed's namespace
	taskKey := TaskPositionKeyPrefix(c.ClusterID, model.DefaultNamespace)
	// the taskKey format is /tidb/cdc/{clusterID}/{namespace}/task/position/{captureID}
	taskKey = fmt.Sprintf("%s/%s", taskKey, captureID)
	_, err = c.Client.Delete(ctx, taskKey, clientv3.WithPrefix())
	if err != nil {
		log.Warn("delete task position failed",
			zap.String("clusterID", c.ClusterID),
			zap.String("captureID", captureID),
			zap.String("key", key), zap.Error(err))
	}
	return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
}

// GetOwnerID returns the owner id by querying etcd
func (c *CDCEtcdClientImpl) GetOwnerID(ctx context.Context) (string, error) {
	resp, err := c.Client.Get(ctx, CaptureOwnerKey(c.ClusterID),
		clientv3.WithFirstCreate()...)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if len(resp.Kvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}

// GetOwnerRevision gets the Etcd revision for the elected owner.
func (c *CDCEtcdClientImpl) GetOwnerRevision(
	ctx context.Context, captureID string,
) (rev int64, err error) {
	resp, err := c.Client.Get(ctx, CaptureOwnerKey(c.ClusterID), clientv3.WithFirstCreate()...)
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
		return nil, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if resp.Count == 0 {
		return nil, cerror.ErrUpstreamNotFound.GenWithStackByArgs(KeyStr)
	}
	info := &model.UpstreamInfo{}
	err = info.Unmarshal(resp.Kvs[0].Value)
	return info, errors.Trace(err)
}

// GcServiceIDForTest returns the gc service ID for tests
func GcServiceIDForTest() string {
	return fmt.Sprintf("ticdc-%s-%d", "default", 0)
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
// "6a6c6dd290bc8732" from /tidb/cdc/cluster/namespace/changefeed/info/6a6c6dd290bc8732
func extractKeySuffix(key string) (string, error) {
	subs := strings.Split(key, "/")
	if len(subs) < 2 {
		return "", cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	return subs[len(subs)-1], nil
}
