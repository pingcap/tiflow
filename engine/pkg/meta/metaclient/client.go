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

package metaclient

import "context"

type ClientType int

const (
	TypeKVClient ClientType = iota /* KV client style，like etcd/consul/TiKV/redis or even SQL backend*/
)

type Client interface {
	// Close is the method to close the client and release inner resources
	Close() error

	// GenEpoch generate the increasing epoch for user
	GenEpoch(ctx context.Context) (int64, error)
}

type KVClient interface {
	Client
	KV
}
