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

package model

import (
	"context"
)

// Txn doesn't support nested txn
type Txn interface {
	// Do cache Ops in the Txn
	// Same op limit with KV Put/Get/Delete interface
	// Using snapshot isolation
	Do(ops ...Op) Txn

	// Commit tries to commit the transaction.
	// Any Op fail will cause entire txn rollback and return error
	Commit() (*TxnResponse, Error)
}

// KV defines a key value access interface, which is quite similar to etcd KV API
type KV interface {
	// Put puts a key-value pair into metastore.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	// or do nothing on vice verse.
	// Length of key is restricted to 2KB
	Put(ctx context.Context, key, val string) (*PutResponse, Error)

	// Get retrieves keys with newest revision.
	// By default, Get will return the value for "key", if any.
	// When WithRange(end) is passed, Get will return the keys in the range [key, end).
	// When WithFromKey() is passed, Get returns keys greater than or equal to key.
	// When WithPrefix() is passed, Get returns keys with prefix.
	// WARN: WithRange(), WithFromKey(), WithPrefix() can't be used at the same time
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, Error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	// WARN: WithRange(end), WithFromKey(), WithPrefix() can't be used at the same time
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, Error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn
}

// Error defines the interface used in KV interface
type Error interface {
	error
	// IsRetryable returns true if this error may be gone if retried.
	IsRetryable() bool
}

// Client defines some basic method used as a meta client
type Client interface {
	// Close is the method to close the client and release inner resources
	Close() error

	// GenEpoch generate the increasing epoch for user
	GenEpoch(ctx context.Context) (int64, error)
}

// KVClient combines Client interface and KV interface
type KVClient interface {
	Client
	KV
}

// ClientConn is the common method for different connection
// HOPE to reuse the common underlying connection pool
type ClientConn interface {
	// StoreType returns the type of connection
	StoreType() StoreType

	// GetConn gets the underlying connection object
	// For the fisrt return param if no error happens:
	// For StoreTypeEtcd, it returns *clientv3.Client
	// For StoreTypeMySQL/StoreTypeSQLite, it returns *sql.DB
	GetConn() (interface{}, error)

	// Close closes the underlying connection and releases some resources
	Close() error
}
