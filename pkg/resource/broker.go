package resource

import (
	"context"
	"path/filepath"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type ID string

type FileWriter interface {
	storage.ExternalFileWriter
}

// Proxy is assigned to a worker so worker can create the resource files and notify
// the dataflow engine.
type Proxy interface {
	// ID identifies the resources that one worker created, to provide cross-worker
	// constrains.
	ID() ID
	// CreateFile can create a FileWriter for writing at the given path under current
	// ID.
	CreateFile(ctx context.Context, path string) (FileWriter, error)
}

type broker struct {
	// TODO: report it to ResourceManager
	allocated map[ID]struct{}
	// TODO: make it configurable
	pathPrefix string
}

var DefaultBroker = broker{
	allocated:  make(map[ID]struct{}),
	pathPrefix: "resources",
}

type proxy struct {
	id      ID
	storage storage.ExternalStorage
}

func (p proxy) ID() ID {
	return p.id
}

func (p proxy) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	return p.storage.Create(ctx, path)
}

func newProxy(ctx context.Context, pathPrefix, id string) (Proxy, error) {
	storagePath := filepath.Join(pathPrefix, id)
	// only support local disk now
	backend, err := storage.ParseBackend(storagePath, nil)
	if err != nil {
		return nil, err
	}
	s, err := storage.New(ctx, backend, nil)
	if err != nil {
		return nil, err
	}
	return &proxy{
		id:      ID(id),
		storage: s,
	}, nil
}

func (b *broker) NewProxyForWorker(ctx context.Context, id string) (Proxy, error) {
	p, err := newProxy(ctx, b.pathPrefix, id)
	if err != nil {
		return nil, err
	}
	b.allocated[ID(id)] = struct{}{}
	return p, nil
}

func NewMockProxy(id string) Proxy {
	p, err := newProxy(context.TODO(), "unit_test_resources", id)
	if err != nil {
		log.L().Panic("failed in NewMockProxy",
			zap.String("id", id),
			zap.Error(err))
	}
	return p
}
