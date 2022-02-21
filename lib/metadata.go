package lib

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"

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
		if masterMeta.MasterMetaExt.Tp != JobManager {
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
	extTpi       interface{}
}

func NewWorkerMetadataClient(
	masterID MasterID,
	metaClient metadata.MetaKV,
	extTpi interface{},
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		masterID:     masterID,
		metaKVClient: metaClient,
		extTpi:       extTpi,
	}
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

	if err := workerMeta.fillExt(c.extTpi); err != nil {
		return nil, errors.Trace(err)
	}
	return &workerMeta, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, workerID WorkerID, data *WorkerStatus) error {
	if err := data.marshalExt(); err != nil {
		return errors.Trace(err)
	}

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
