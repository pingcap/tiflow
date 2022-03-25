package resourcemeta

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/dataset"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

const (
	metadataQPSLimit = 1024
)

type MetadataAccessor struct {
	// rl limits the frequency the metastore is written to.
	// It helps to prevent cascading failures after a fail-over
	// where a large number of resources are created.
	rl      ratelimit.Limiter
	dataset *dataset.DataSet[ResourceMeta, *ResourceMeta]
}

func NewMetadataAccessor(client metaclient.KV) *MetadataAccessor {
	return &MetadataAccessor{
		rl:      ratelimit.New(metadataQPSLimit),
		dataset: dataset.NewDataSet[ResourceMeta, *ResourceMeta](client, adapter.ResourceKeyAdapter),
	}
}

func (m *MetadataAccessor) GetResource(ctx context.Context, resourceID ResourceID) (*ResourceMeta, bool, error) {
	rec, err := m.dataset.Get(ctx, resourceID)
	if derror.ErrDatasetEntryNotFound.Equal(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *ResourceMeta) (bool, error) {
	_, err := m.dataset.Get(ctx, resource.ID)
	if err == nil {
		// A duplicate exists
		return false, nil
	}
	if !derror.ErrDatasetEntryNotFound.Equal(err) {
		// An unexpected error
		return false, err
	}

	m.rl.Take()
	if err := m.dataset.Upsert(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *ResourceMeta) (bool, error) {
	_, err := m.dataset.Get(ctx, resource.ID)
	if derror.ErrDatasetEntryNotFound.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	m.rl.Take()
	if err := m.dataset.Upsert(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) DeleteResource(ctx context.Context, resourceID ResourceID) (bool, error) {
	_, err := m.dataset.Get(ctx, resourceID)
	if derror.ErrDatasetEntryNotFound.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := m.dataset.Delete(ctx, resourceID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *MetadataAccessor) GetAllResources(ctx context.Context) (ret []*ResourceMeta, err error) {
	startTime := time.Now()
	defer func() {
		timeCost := time.Since(startTime)
		log.L().Info("Scanned all resources",
			zap.Int("count", len(ret)),
			zap.Duration("duration", timeCost),
			zap.Error(err))
	}()
	return m.dataset.LoadAll(ctx)
}

func (m *MetadataAccessor) GetResourcesForExecutor(
	ctx context.Context,
	executorID model.ExecutorID,
) (records []*ResourceMeta, err error) {
	all, err := m.GetAllResources(ctx)
	if err != nil {
		return nil, err
	}

	// TODO optimize this when the backend store supports conditional queries.
	for _, resource := range all {
		if resource.Executor != executorID {
			continue
		}
		records = append(records, resource)
		log.L().Info("Found resource for executor",
			zap.String("executor-id", string(executorID)),
			zap.Any("resource", resource))
	}
	return
}
