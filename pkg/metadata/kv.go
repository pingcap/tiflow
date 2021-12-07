package metadata

import (
	"context"
)

// MetaKV defines a key-value like metastore API, it keeps the same API style
// as etcd.
type MetaKV interface {
	Put(ctx context.Context, key, value string, opts ...interface{}) (interface{}, error)
	Get(ctx context.Context, key string, opts ...interface{}) (interface{}, error)
	Delete(ctx context.Context, key string, opts ...interface{}) (interface{}, error)
	Txn(ctx context.Context) interface{}
	Watch(ctx context.Context, key string, opts ...interface{}) interface{}
}
