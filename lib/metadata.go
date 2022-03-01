package lib

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

const JobManagerUUID = "dataflow-engine-job-manager"

type MasterMetadataClient struct {
	masterID     MasterID
	metaKVClient metadata.MetaKV
}

func NewMasterMetadataClient(masterID MasterID, metaKVClient metadata.MetaKV) *MasterMetadataClient {
	return &MasterMetadataClient{
		masterID:     masterID,
		metaKVClient: metaKVClient,
	}
}

func (c *MasterMetadataClient) Load(ctx context.Context) (*MasterMetaKVData, error) {
	key := adapter.MasterMetaKey.Encode(c.masterID)
	rawResp, err := c.metaKVClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := rawResp.(*clientv3.GetResponse)
	if len(resp.Kvs) == 0 {
		// TODO refine handling the situation where the mata key does not exist at this point
		masterMeta := &MasterMetaKVData{
			ID: c.masterID,
		}
		return masterMeta, nil
	}
	masterMetaBytes := resp.Kvs[0].Value
	var masterMeta MasterMetaKVData
	if err := json.Unmarshal(masterMetaBytes, &masterMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}
	return &masterMeta, nil
}

func (c *MasterMetadataClient) Store(ctx context.Context, data *MasterMetaKVData) error {
	key := adapter.MasterMetaKey.Encode(c.masterID)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = c.metaKVClient.Put(ctx, key, string(dataBytes))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// LoadAllMasters loads all job masters from metastore
func (c *MasterMetadataClient) LoadAllMasters(ctx context.Context) ([]*MasterMetaKVData, error) {
	raw, err := c.metaKVClient.Get(ctx, adapter.MasterMetaKey.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := raw.(*clientv3.GetResponse)
	meta := make([]*MasterMetaKVData, 0, resp.Count)
	for _, kv := range resp.Kvs {
		masterMeta := &MasterMetaKVData{}
		if err := json.Unmarshal(kv.Value, masterMeta); err != nil {
			return nil, errors.Trace(err)
		}
		if masterMeta.Tp != JobManager {
			meta = append(meta, masterMeta)
		}
	}
	return meta, nil
}

func (c *MasterMetadataClient) GenerateEpoch(ctx context.Context) (Epoch, error) {
	rawResp, err := c.metaKVClient.Get(ctx, "/fake-key")
	if err != nil {
		return 0, errors.Trace(err)
	}

	resp := rawResp.(*clientv3.GetResponse)
	return resp.Header.Revision, nil
}

type WorkerMetadataClient struct {
	masterID     MasterID
	metaKVClient metadata.MetaKV
}

func NewWorkerMetadataClient(
	masterID MasterID,
	metaClient metadata.MetaKV,
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		masterID:     masterID,
		metaKVClient: metaClient,
	}
}

func (c *WorkerMetadataClient) LoadAllWorkers(ctx context.Context) (map[WorkerID]*WorkerStatus, error) {
	loadPrefix := adapter.WorkerKeyAdapter.Encode(c.masterID)
	raw, err := c.metaKVClient.Get(ctx, loadPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := raw.(*clientv3.GetResponse)
	ret := make(map[WorkerID]*WorkerStatus, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		decoded, err := adapter.WorkerKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(decoded) != 2 {
			// TODO add an error type
			return nil, errors.Errorf("unexpected key: %s", string(kv.Key))
		}

		// NOTE decoded[0] is the master ID.
		workerID := decoded[1]

		workerMetaBytes := kv.Value
		var workerMeta WorkerStatus
		if err := json.Unmarshal(workerMetaBytes, &workerMeta); err != nil {
			// TODO wrap the error
			return nil, errors.Trace(err)
		}
		ret[workerID] = &workerMeta
	}
	return ret, nil
}

func (c *WorkerMetadataClient) Load(ctx context.Context, workerID WorkerID) (*WorkerStatus, error) {
	rawResp, err := c.metaKVClient.Get(ctx, c.workerMetaKey(workerID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := rawResp.(*clientv3.GetResponse)
	if len(resp.Kvs) == 0 {
		return nil, derror.ErrWorkerNoMeta.GenWithStackByArgs()
	}
	workerMetaBytes := resp.Kvs[0].Value
	var workerMeta WorkerStatus
	if err := json.Unmarshal(workerMetaBytes, &workerMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}

	return &workerMeta, nil
}

func (c *WorkerMetadataClient) Remove(ctx context.Context, id WorkerID) (bool, error) {
	raw, err := c.metaKVClient.Delete(ctx, c.workerMetaKey(id))
	if err != nil {
		return false, errors.Trace(err)
	}
	if raw == nil {
		// This is in order to be compatible with MetaMock.
		// TODO remove this check when we migrate to the new metaclient.
		return true, nil
	}
	resp := raw.(*clientv3.DeleteResponse)
	if resp.Deleted != 1 {
		return false, nil
	}
	return true, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, workerID WorkerID, data *WorkerStatus) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = c.metaKVClient.Put(ctx, c.workerMetaKey(workerID), string(dataBytes))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *WorkerMetadataClient) MasterID() MasterID {
	return c.masterID
}

func (c *WorkerMetadataClient) workerMetaKey(id WorkerID) string {
	return adapter.WorkerKeyAdapter.Encode(c.masterID, id)
}

// StoreMasterMeta is exposed to job manager for job master meta persistence
func StoreMasterMeta(
	ctx context.Context,
	metaKVClient metadata.MetaKV,
	meta *MasterMetaKVData,
) error {
	metaClient := NewMasterMetadataClient(meta.ID, metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		if !derror.ErrMasterNotFound.Equal(err) {
			return err
		}
	} else {
		log.L().Warn("master meta exits, will be overwritten", zap.Any("meta", masterMeta))
	}

	epoch, err := metaClient.GenerateEpoch(ctx)
	if err != nil {
		return err
	}
	meta.Epoch = epoch

	return metaClient.Store(ctx, meta)
}
