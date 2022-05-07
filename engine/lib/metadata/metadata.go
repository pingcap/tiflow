package metadata

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
)

const JobManagerUUID = "dataflow-engine-job-manager"

type MasterMetadataClient struct {
	masterID   libModel.MasterID
	metaClient pkgOrm.Client
}

func NewMasterMetadataClient(
	masterID libModel.MasterID,
	metaClient pkgOrm.Client,
) *MasterMetadataClient {
	return &MasterMetadataClient{
		masterID:   masterID,
		metaClient: metaClient,
	}
}

func (c *MasterMetadataClient) Load(ctx context.Context) (*libModel.MasterMetaKVData, error) {
	masterMeta, err := c.metaClient.GetJobByID(ctx, c.masterID)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			// TODO refine handling the situation where the mata key does not exist at this point
			masterMeta := &libModel.MasterMetaKVData{
				// TODO: projectID
				ID:         c.masterID,
				StatusCode: libModel.MasterStatusUninit,
			}
			return masterMeta, nil
		}

		return nil, errors.Trace(err)
	}
	return masterMeta, nil
}

// Store upsert the data
func (c *MasterMetadataClient) Store(ctx context.Context, data *libModel.MasterMetaKVData) error {
	return errors.Trace(c.metaClient.UpsertJob(ctx, data))
}

// Update update the data
func (c *MasterMetadataClient) Update(ctx context.Context, data *libModel.MasterMetaKVData) error {
	return errors.Trace(c.metaClient.UpdateJob(ctx, data))
}

func (c *MasterMetadataClient) Delete(ctx context.Context) error {
	_, err := c.metaClient.DeleteJob(ctx, c.masterID)
	return errors.Trace(err)
}

// LoadAllMasters loads all job masters from metastore
func (c *MasterMetadataClient) LoadAllMasters(ctx context.Context) ([]*libModel.MasterMetaKVData, error) {
	meta, err := c.metaClient.QueryJobs(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

type WorkerMetadataClient struct {
	masterID   libModel.MasterID
	metaClient pkgOrm.Client
}

func NewWorkerMetadataClient(
	masterID libModel.MasterID,
	metaClient pkgOrm.Client,
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		masterID:   masterID,
		metaClient: metaClient,
	}
}

func (c *WorkerMetadataClient) LoadAllWorkers(ctx context.Context) (map[libModel.WorkerID]*libModel.WorkerStatus, error) {
	resp, err := c.metaClient.QueryWorkersByMasterID(ctx, c.masterID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res := make(map[libModel.WorkerID]*libModel.WorkerStatus, len(resp))
	for _, m := range resp {
		res[m.ID] = m
	}
	return res, nil
}

func (c *WorkerMetadataClient) Load(ctx context.Context, workerID libModel.WorkerID) (*libModel.WorkerStatus, error) {
	resp, err := c.metaClient.GetWorkerByID(ctx, c.masterID, workerID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return resp, nil
}

func (c *WorkerMetadataClient) Remove(ctx context.Context, id libModel.WorkerID) (bool, error) {
	_, err := c.metaClient.DeleteWorker(ctx, c.masterID, id)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, data *libModel.WorkerStatus) error {
	return errors.Trace(c.metaClient.UpsertWorker(ctx, data))
}

func (c *WorkerMetadataClient) Update(ctx context.Context, data *libModel.WorkerStatus) error {
	return errors.Trace(c.metaClient.UpdateWorker(ctx, data))
}

func (c *WorkerMetadataClient) MasterID() libModel.MasterID {
	return c.masterID
}

// StoreMasterMeta is exposed to job manager for job master meta persistence
func StoreMasterMeta(
	ctx context.Context,
	metaClient pkgOrm.Client,
	meta *libModel.MasterMetaKVData,
) error {
	metaCli := NewMasterMetadataClient(meta.ID, metaClient)
	masterMeta, err := metaCli.Load(ctx)
	if err != nil {
		return err
	}
	log.L().Warn("master meta exits, will be overwritten", zap.Any("old-meta", masterMeta), zap.Any("meta", meta))

	return metaCli.Store(ctx, meta)
}

func DeleteMasterMeta(
	ctx context.Context,
	metaClient pkgOrm.Client,
	masterID libModel.MasterID,
) error {
	metaCli := NewMasterMetadataClient(masterID, metaClient)
	return metaCli.Delete(ctx)
}
