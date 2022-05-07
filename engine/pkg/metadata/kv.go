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
