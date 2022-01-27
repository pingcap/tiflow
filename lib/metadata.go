package lib

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

type MetadataClient struct {
	masterID     MasterID
	metaKVClient metadata.MetaKV
}

func NewMetadataClient(masterID MasterID, metaKVClient metadata.MetaKV) *MetadataClient {
	return &MetadataClient{
		masterID:     masterID,
		metaKVClient: metaKVClient,
	}
}

func (c *MetadataClient) Load(ctx context.Context) (*MasterMetaKVData, error) {
	key := adapter.MasterMetaKey.Encode(string(c.masterID))
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

func (c *MetadataClient) Store(ctx context.Context, data *MasterMetaKVData) error {
	key := adapter.MasterMetaKey.Encode(string(c.masterID))
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

func (c *MetadataClient) GenerateEpoch(ctx context.Context) (Epoch, error) {
	rawResp, err := c.metaKVClient.Get(ctx, "/fake-key")
	if err != nil {
		return 0, errors.Trace(err)
	}

	resp := rawResp.(*clientv3.GetResponse)
	return resp.Header.Revision, nil
}
