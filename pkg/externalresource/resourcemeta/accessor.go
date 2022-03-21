package resourcemeta

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

const (
	metadataQPSLimit = 1024
)

type MetadataAccessor struct {
	rl     ratelimit.Limiter
	client metaclient.KV
}

func NewMetadataAccessor(client metaclient.KV) *MetadataAccessor {
	return &MetadataAccessor{
		rl:     ratelimit.New(metadataQPSLimit),
		client: client,
	}
}

func (m *MetadataAccessor) GetResource(ctx context.Context, resourceID ResourceID) (*ResourceMeta, bool, error) {
	key := adapter.ResourceKeyAdapter.Encode(resourceID)
	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}

	if len(resp.Kvs) == 0 {
		// Resource does not exist.
		return nil, false, nil
	}
	if len(resp.Kvs) > 1 {
		log.L().Panic("unreachable", zap.Any("resp", resp))
	}

	var ret ResourceMeta
	if err := json.Unmarshal(resp.Kvs[0].Value, &ret); err != nil {
		return nil, false, errors.Trace(err)
	}

	return &ret, true, nil
}

func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *ResourceMeta) (bool, error) {
	key := adapter.ResourceKeyAdapter.Encode(resource.ID)
	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) == 1 {
		// Resource already exists
		return false, nil
	}
	if len(resp.Kvs) > 1 {
		log.L().Panic("unreachable", zap.Any("resp", resp))
	}

	str, err1 := json.Marshal(resource)
	if err1 != nil {
		return false, errors.Trace(err)
	}

	m.rl.Take()
	if _, err := m.client.Put(ctx, key, string(str)); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *ResourceMeta) (bool, error) {
	key := adapter.ResourceKeyAdapter.Encode(resource.ID)
	m.rl.Take()
	resp, err := m.client.Get(ctx, key)
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) == 0 {
		// Resource does not exist
		return false, nil
	}
	if len(resp.Kvs) > 1 {
		log.L().Panic("unreachable", zap.Any("resp", resp))
	}

	str, err1 := json.Marshal(resource)
	if err1 != nil {
		return false, errors.Trace(err)
	}

	m.rl.Take()
	if _, err := m.client.Put(ctx, key, string(str)); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) DeleteResource(ctx context.Context, resourceID ResourceID) (bool, error) {
	key := adapter.ResourceKeyAdapter.Encode(resourceID)
	// TODO check whether the delete count is 0 or 1.
	// The current API of the metaclient DOES NOT support this.
	m.rl.Take()
	_, err := m.client.Delete(ctx, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *MetadataAccessor) GetResourcesForExecutor(
	ctx context.Context,
	executorID model.ExecutorID,
) (records []*ResourceMeta, err error) {
	keyPrefix := adapter.ResourceKeyAdapter.Path()

	m.rl.Take()
	startTime := time.Now()
	resp, err := m.client.Get(ctx, keyPrefix, metaclient.WithPrefix())
	if err != nil {
		return nil, err
	}
	timeCost := time.Since(startTime)
	log.L().Info("Scanned all resources",
		zap.Int("count", len(resp.Kvs)),
		zap.Duration("duration", timeCost))

	// TODO optimize this when the backend store supports conditional queries.
	for _, kv := range resp.Kvs {
		var resource ResourceMeta
		if err := json.Unmarshal(kv.Value, &resource); err != nil {
			return nil, errors.Trace(err)
		}
		if resource.Executor != executorID {
			continue
		}
		records = append(records, &resource)
		log.L().Info("Found resource for executor",
			zap.String("executor-id", string(executorID)),
			zap.Any("resource", resource))
	}
	return
}
