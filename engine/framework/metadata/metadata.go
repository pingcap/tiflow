// Copyright 2022 PingCAP, Inc.
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

package metadata

import (
	"context"

	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// JobManagerUUID defines the global unique id for job manager
const JobManagerUUID = "dataflow-engine-job-manager"

// MasterMetadataClient provides all ways to manage the master metadata
type MasterMetadataClient struct {
	masterID   frameModel.MasterID
	metaClient pkgOrm.Client
}

// NewMasterMetadataClient creates a new MasterMetadataClient
func NewMasterMetadataClient(
	masterID frameModel.MasterID,
	metaClient pkgOrm.Client,
) *MasterMetadataClient {
	return &MasterMetadataClient{
		masterID:   masterID,
		metaClient: metaClient,
	}
}

// Load queries master metadata from metastore, if the metadata does not exist,
// create a new one and return it.
func (c *MasterMetadataClient) Load(ctx context.Context) (*frameModel.MasterMeta, error) {
	masterMeta, err := c.metaClient.GetJobByID(ctx, c.masterID)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			// TODO refine handling the situation where the mata key does not exist at this point
			masterMeta := &frameModel.MasterMeta{
				// TODO: projectID
				ID:    c.masterID,
				State: frameModel.MasterStateUninit,
			}
			return masterMeta, nil
		}

		return nil, errors.Trace(err)
	}
	return masterMeta, nil
}

// Insert inserts the metadata
func (c *MasterMetadataClient) Insert(ctx context.Context, data *frameModel.MasterMeta) error {
	return errors.Trace(c.metaClient.InsertJob(ctx, data))
}

// Store upsert the data
func (c *MasterMetadataClient) Store(ctx context.Context, data *frameModel.MasterMeta) error {
	return errors.Trace(c.metaClient.UpsertJob(ctx, data))
}

// Update update the data
func (c *MasterMetadataClient) Update(
	ctx context.Context, values ormModel.KeyValueMap,
) error {
	return errors.Trace(c.metaClient.UpdateJob(ctx, c.masterID, values))
}

// Delete deletes the metadata of this master
func (c *MasterMetadataClient) Delete(ctx context.Context) error {
	_, err := c.metaClient.DeleteJob(ctx, c.masterID)
	return errors.Trace(err)
}

// LoadAllMasters loads all job masters from metastore
func (c *MasterMetadataClient) LoadAllMasters(ctx context.Context) ([]*frameModel.MasterMeta, error) {
	meta, err := c.metaClient.QueryJobs(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

// WorkerStatusClient provides all ways to manage metadata of all workers
// belonging to a given master
type WorkerStatusClient struct {
	masterID   frameModel.MasterID
	metaClient pkgOrm.Client
}

// NewWorkerStatusClient creates a new WorkerStatusClient instance
func NewWorkerStatusClient(
	masterID frameModel.MasterID,
	metaClient pkgOrm.Client,
) *WorkerStatusClient {
	return &WorkerStatusClient{
		masterID:   masterID,
		metaClient: metaClient,
	}
}

// LoadAllWorkers queries all workers of this master
func (c *WorkerStatusClient) LoadAllWorkers(ctx context.Context) (map[frameModel.WorkerID]*frameModel.WorkerStatus, error) {
	resp, err := c.metaClient.QueryWorkersByMasterID(ctx, c.masterID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res := make(map[frameModel.WorkerID]*frameModel.WorkerStatus, len(resp))
	for _, m := range resp {
		res[m.ID] = m
	}
	return res, nil
}

// Load queries a worker by its worker id
func (c *WorkerStatusClient) Load(ctx context.Context, workerID frameModel.WorkerID) (*frameModel.WorkerStatus, error) {
	resp, err := c.metaClient.GetWorkerByID(ctx, c.masterID, workerID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return resp, nil
}

// Remove deletes a given worker from metastore
func (c *WorkerStatusClient) Remove(ctx context.Context, id frameModel.WorkerID) (bool, error) {
	_, err := c.metaClient.DeleteWorker(ctx, c.masterID, id)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// Store stores a worker metadata into metastore
func (c *WorkerStatusClient) Store(ctx context.Context, data *frameModel.WorkerStatus) error {
	return errors.Trace(c.metaClient.UpsertWorker(ctx, data))
}

// Update updates a worker metadata
func (c *WorkerStatusClient) Update(ctx context.Context, data *frameModel.WorkerStatus) error {
	return errors.Trace(c.metaClient.UpdateWorker(ctx, data))
}

// MasterID returns the master id of this metadata client
func (c *WorkerStatusClient) MasterID() frameModel.MasterID {
	return c.masterID
}

// StoreMasterMeta is exposed to job manager for job master meta persistence
func StoreMasterMeta(
	ctx context.Context,
	metaClient pkgOrm.Client,
	meta *frameModel.MasterMeta,
) error {
	metaCli := NewMasterMetadataClient(meta.ID, metaClient)
	masterMeta, err := metaCli.Load(ctx)
	if err != nil {
		return err
	}
	log.Warn("master meta exists, will be overwritten", zap.Any("old-meta", masterMeta), zap.Any("meta", meta))

	return metaCli.Store(ctx, meta)
}

// DeleteMasterMeta deletes given maste meta from meta store
func DeleteMasterMeta(
	ctx context.Context,
	metaClient pkgOrm.Client,
	masterID frameModel.MasterID,
) error {
	metaCli := NewMasterMetadataClient(masterID, metaClient)
	return metaCli.Delete(ctx)
}
