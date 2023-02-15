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

package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/bucket"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultTimeout                 = 10 * time.Second
	defaultClosedWorkerChannelSize = 10000
)

type closedWorker struct {
	workerID resModel.WorkerID
	jobID    resModel.JobID
}

// DefaultBroker must implement Broker.
var _ Broker = (*DefaultBroker)(nil)

// DefaultBroker implements the Broker interface
type DefaultBroker struct {
	executorID resModel.ExecutorID
	client     client.ResourceManagerClient

	fileManagers      map[resModel.ResourceType]internal.FileManager
	bucketFileManager internal.FileManager
	// TODO: add monitor for closedWorkerCh
	closedWorkerCh chan closedWorker

	// If S3 is configured, a dummy resource will be persisted by broker to indicate
	// that its temporary files have not been cleaned, which is useful to prevent
	// resource leaks.
	//
	// Normally a broker will attempt to clean up temporary files and dummy resources
	// before exiting. If this step fails, the dummy record is stored in Meta, which
	// will be cleaned up by GCCoordinator eventually.
	s3dummyHandler Handle
	cancel         context.CancelFunc

	// storage config
	config *resModel.Config
}

// NewBroker creates a new Impl instance.
func NewBroker(
	ctx context.Context,
	executorID resModel.ExecutorID,
	client client.ServerMasterClient,
) (*DefaultBroker, error) {
	resp, err := client.QueryStorageConfig(ctx, &pb.QueryStorageConfigRequest{})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("query storage config failed: %v, %v", err, resp))
	}
	var storageConfig resModel.Config
	err = json.Unmarshal(resp.Config, &storageConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// adjust and check config
	storageConfig.Adjust(executorID)
	if err := PreCheckConfig(&storageConfig); err != nil {
		return nil, err
	}
	return NewBrokerWithConfig(&storageConfig, executorID, client)
}

// NewBrokerWithConfig creates a new Impl instance based on the given config.
func NewBrokerWithConfig(
	config *resModel.Config,
	executorID resModel.ExecutorID,
	client client.ResourceManagerClient,
) (*DefaultBroker, error) {
	log.Info("Create new resource broker",
		zap.String("executor-id", string(executorID)),
		zap.Any("config", config))

	broker := &DefaultBroker{
		executorID:     executorID,
		client:         client,
		fileManagers:   make(map[resModel.ResourceType]internal.FileManager),
		closedWorkerCh: make(chan closedWorker, defaultClosedWorkerChannelSize),
		config:         config,
	}
	if err := broker.initStorage(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go broker.tick(ctx)
	broker.cancel = cancel

	return broker, nil
}

func (b *DefaultBroker) initStorage() error {
	if b.config == nil || !b.config.LocalEnabled() {
		log.Panic("local file manager must be supported by resource broker")
	}
	b.fileManagers[resModel.ResourceTypeLocalFile] = local.NewLocalFileManager(b.executorID, b.config.Local)

	if !b.config.S3Enabled() && !b.config.GCSEnabled() {
		log.Info("broker will not use s3/gcs as external storage since s3/gcs are both not configured")
		return nil
	}

	if b.config.S3Enabled() {
		log.Info("broker will use s3 as external storage since s3 is configured")
		b.bucketFileManager = bucket.NewFileManagerWithConfig(b.executorID, b.config)
		b.fileManagers[resModel.ResourceTypeS3] = b.bucketFileManager
		return b.createDummyResource()
	}

	if b.config.GCSEnabled() {
		log.Info("broker will use gcs as external storage since gcs is configured")
		b.bucketFileManager = bucket.NewFileManagerWithConfig(b.executorID, b.config)
		b.fileManagers[resModel.ResourceTypeGCS] = b.bucketFileManager
		return b.createDummyResource()
	}

	return nil
}

// OpenStorage implements Broker.OpenStorage
func (b *DefaultBroker) OpenStorage(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resID resModel.ResourceID,
	opts ...OpenStorageOption,
) (Handle, error) {
	// Note the semantics of PasreResourceID:
	// If resourceID is `/local/my-resource`, then tp == resModel.ResourceTypeLocalFile
	// and resName == "my-resource".
	tp, resName, err := resModel.ParseResourceID(resID)
	if err != nil {
		return nil, err
	}

	fm, ok := b.fileManagers[tp]
	if !ok {
		log.Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	options := &openStorageOptions{}
	for _, o := range opts {
		o(options)
	}

	record, exists, err := b.checkForExistingResource(ctx,
		resModel.ResourceKey{JobID: jobID, ID: resID})
	if err != nil {
		return nil, err
	}

	var desc internal.ResourceDescriptor
	if !exists {
		desc, err = b.createResource(ctx, fm, projectInfo, workerID, resName)
	} else if !options.cleanBeforeOpen {
		desc, err = b.getPersistResource(ctx, fm, record, resName)
	} else {
		desc, err = b.cleanOrRecreatePersistResource(ctx, fm, record, resName)
	}
	if err != nil {
		return nil, err
	}

	log.Info(fmt.Sprintf("Using %s storage with path", string(tp)),
		zap.String("path", desc.URI()))
	return newResourceHandle(jobID, b.executorID, fm, desc, exists, b.client)
}

func (b *DefaultBroker) createResource(
	ctx context.Context, fm internal.FileManager,
	projectInfo tenant.ProjectInfo, workerID resModel.WorkerID,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: projectInfo,
			Executor:    b.executorID, /* executor id where resource is created */
			WorkerID:    workerID,     /* creator id */
		},
	}
	desc, err := fm.CreateResource(ctx, ident)
	if err != nil {
		//nolint:errcheck
		_ = fm.RemoveResource(ctx, ident)
		return nil, err
	}
	return desc, nil
}

// OnWorkerClosed implements Broker.OnWorkerClosed
func (b *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	select {
	case <-ctx.Done():
		return
	case b.closedWorkerCh <- closedWorker{workerID: workerID, jobID: jobID}:
		return
	case <-time.After(defaultTimeout):
		log.Error("closed worker channel is full, broker may be stuck")
	}
}

// tick periodically cleans up resources created by closed worker.
func (b *DefaultBroker) tick(ctx context.Context) {
	// We run a gc loop at the max frequency of once per second.
	rl := ratelimit.New(1 /* once per second */)
	for {
		rl.Take()
		select {
		case <-ctx.Done():
			return
		case w := <-b.closedWorkerCh:
			scope := internal.ResourceScope{
				Executor: b.executorID,
				WorkerID: w.workerID,
			}
			for _, fm := range b.fileManagers {
				err := fm.RemoveTemporaryFiles(ctx, scope)
				if err != nil {
					// TODO when we have a cloud-based error collection service, we need
					// to report this.
					// However, since an error here is unlikely to indicate a correctness
					// problem, we do not take further actions.
					log.Warn("Failed to remove temporary files for worker",
						zap.String("worker-id", w.workerID),
						zap.String("job-id", w.jobID),
						zap.Error(err))
					// Handle this worker later
					// Note that if the cleanup operation continues to fail, some requests
					// will be discarded after the channel is full, and they will be cleaned
					// when broker exits.
					b.OnWorkerClosed(ctx, w.workerID, w.jobID)
				}
			}
		}
	}
}

// RemoveResource implements pb.BrokerServiceServer.
func (b *DefaultBroker) RemoveResource(
	ctx context.Context,
	request *pb.RemoveLocalResourceRequest,
) (*pb.RemoveLocalResourceResponse, error) {
	tp, resName, err := resModel.ParseResourceID(request.GetResourceId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if tp != resModel.ResourceTypeLocalFile {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("unexpected resource type %s", tp))
	}

	fm := b.fileManagers[tp]
	if request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty WorkerId")
	}

	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			Executor: b.executorID,
			WorkerID: request.GetWorkerId(),
		},
	}
	err = fm.RemoveResource(ctx, ident)
	if err != nil {
		if errors.Is(err, errors.ErrResourceDoesNotExist) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.RemoveLocalResourceResponse{}, nil
}

func (b *DefaultBroker) checkForExistingResource(
	ctx context.Context,
	resourceKey resModel.ResourceKey,
) (*resModel.ResourceMeta, bool, error) {
	request := &pb.QueryResourceRequest{
		ResourceKey: &pb.ResourceKey{
			JobId:      resourceKey.JobID,
			ResourceId: resourceKey.ID,
		},
	}
	resp, err := b.client.QueryResource(ctx, request)
	if err == nil {
		return &resModel.ResourceMeta{
			ID:       resourceKey.ID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resModel.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	if errors.Is(err, errors.ErrResourceDoesNotExist) {
		err = nil
	}
	return nil, false, err
}

func (b *DefaultBroker) getPersistResource(
	ctx context.Context, fm internal.FileManager,
	record *resModel.ResourceMeta,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("", record.ProjectID),
			Executor:    record.Executor, /* executor id where the resource is persisted */
			WorkerID:    record.Worker,   /* creator id*/
		},
	}
	desc, err := fm.GetPersistedResource(ctx, ident)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

func (b *DefaultBroker) cleanOrRecreatePersistResource(
	ctx context.Context, fm internal.FileManager,
	record *resModel.ResourceMeta,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("", record.ProjectID),
			Executor:    record.Executor, /* executor id where the resource is persisted */
			WorkerID:    record.Worker,   /* creator id*/
		},
	}
	desc, err := fm.CleanOrRecreatePersistedResource(ctx, ident)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

func (b *DefaultBroker) createDummyResource() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	desc, err := b.bucketFileManager.CreateResource(ctx, bucket.GetDummyIdent(b.executorID))
	if err != nil {
		return err
	}

	handler, err := newResourceHandle(bucket.GetDummyJobID(b.executorID), b.executorID,
		b.bucketFileManager, desc, false, b.client)
	if err != nil {
		return err
	}

	err = handler.Persist(ctx)
	if err != nil {
		return err
	}

	b.s3dummyHandler = handler
	return nil
}

// Close cleans up the broker.
func (b *DefaultBroker) Close() {
	b.cancel()

	// Try to clean up temporary files created by current executor
	if b.bucketFileManager != nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		err := b.bucketFileManager.RemoveTemporaryFiles(ctx, internal.ResourceScope{
			Executor: b.executorID,
			WorkerID: "", /* empty workID means remove all temp files in executor */
		})
		if err != nil {
			// Ignore this error since gcCoordinator will clean up this temp files.
			log.Warn("failed to remove temporary files in executor",
				zap.String("executorID", string(b.executorID)), zap.Error(err))
			return
		}

		// Remove s3 dummy file meta
		if b.s3dummyHandler != nil {
			_ = b.s3dummyHandler.Discard(ctx)
		}
	}
}

// GetEnabledBucketStorage returns true and the corresponding resource type if bucket storage is enabled.
func (b *DefaultBroker) GetEnabledBucketStorage() (bool, resModel.ResourceType) {
	if _, ok := b.fileManagers[resModel.ResourceTypeS3]; ok {
		return true, resModel.ResourceTypeS3
	}
	if _, ok := b.fileManagers[resModel.ResourceTypeGCS]; ok {
		return true, resModel.ResourceTypeGCS
	}

	return false, resModel.ResourceTypeNone
}

// PreCheckConfig checks the configuration of external storage.
func PreCheckConfig(config *resModel.Config) error {
	if config.LocalEnabled() {
		if err := local.PreCheckConfig(config.Local); err != nil {
			return err
		}
	}
	if config.S3Enabled() || config.GCSEnabled() {
		if err := bucket.PreCheckConfig(config); err != nil {
			return err
		}
	}
	return nil
}
