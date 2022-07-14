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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Reference]:https://github.com/etcd-io/etcd/blob/20c89df5e5e2d738efb9c276d954d754eb86918b/client/v3/op.go

package model

import (
	"github.com/pingcap/tiflow/pkg/errors"
)

type opType int

const (
	// A default Op has opType 0, which is invalid.
	tGet opType = iota + 1
	tPut
	tDelete
	tTxn
)

var noPrefixEnd = []byte{0}

// Op represents an Operation that kv can execute.
// Support Key Range/From Key/Key Prefix attributes
type Op struct {
	T   opType
	key []byte
	end []byte

	// for put
	val []byte

	// txn
	ops []Op

	isOptsWithPrefix  bool
	isOptsWithFromKey bool
	isOptsWithRange   bool
}

// EmptyOp creates a global empty op
var EmptyOp Op = Op{}

// accessors/mutators

// IsTxn returns true if the "Op" type is transaction.
func (op Op) IsTxn() bool { return op.T == tTxn }

// IsPut returns true if the operation is a Put.
func (op Op) IsPut() bool { return op.T == tPut }

// IsGet returns true if the operation is a Get.
func (op Op) IsGet() bool { return op.T == tGet }

// IsDelete returns true if the operation is a Delete.
func (op Op) IsDelete() bool { return op.T == tDelete }

// IsOptsWithPrefix returns true if WithPrefix option is called in the given opts.
func (op Op) IsOptsWithPrefix() bool { return op.isOptsWithPrefix }

// IsOptsWithFromKey returns true if WithFromKey option is called in the given opts.
func (op Op) IsOptsWithFromKey() bool { return op.isOptsWithFromKey }

// IsOptsWithRange returns true if WithRange option is called in the given opts.
func (op Op) IsOptsWithRange() bool { return op.isOptsWithRange }

// Txn returns the  operations.
func (op Op) Txn() []Op { return op.ops }

// KeyBytes returns the byte slice holding the Op's key.
func (op Op) KeyBytes() []byte { return op.key }

// WithKeyBytes set the byte slice to the Op's key.
func (op *Op) WithKeyBytes(key []byte) { op.key = key }

// RangeBytes returns the byte slice holding with the Op's range end, if any.
func (op Op) RangeBytes() []byte { return op.end }

// WithRangeBytes set the byte slice to  the Op's range end
func (op *Op) WithRangeBytes(end []byte) { op.end = end }

// ValueBytes returns the byte slice holding the Op's value, if any.
func (op Op) ValueBytes() []byte { return op.val }

// NewOp creates a new op instance
func NewOp() *Op {
	return &Op{key: []byte("")}
}

// IsOptsWithRange returns true if WithRange option is called in the given opts.
func IsOptsWithRange(opts []OpOption) bool {
	op := NewOp()
	for _, opt := range opts {
		opt(op)
	}

	return op.isOptsWithRange
}

// IsOptsWithPrefix returns true if WithPrefix option is called in the given opts.
func IsOptsWithPrefix(opts []OpOption) bool {
	op := NewOp()
	for _, opt := range opts {
		opt(op)
	}

	return op.isOptsWithPrefix
}

// IsOptsWithFromKey returns true if WithFromKey option is called in the given opts.
func IsOptsWithFromKey(opts []OpOption) bool {
	op := NewOp()
	for _, opt := range opts {
		opt(op)
	}

	return op.isOptsWithFromKey
}

// CheckValidOp checks whether op is valid
func (op Op) CheckValidOp() error {
	// [TODO] forbit WithPrefix() + ""
	if !(op.IsOptsWithRange() || op.IsOptsWithPrefix() || op.IsOptsWithFromKey()) {
		return nil
	}

	if op.IsOptsWithRange() && !(op.IsOptsWithPrefix() || op.IsOptsWithFromKey()) {
		return nil
	}
	if op.IsOptsWithPrefix() && !(op.IsOptsWithRange() || op.IsOptsWithFromKey()) {
		return nil
	}
	if op.IsOptsWithFromKey() && !(op.IsOptsWithPrefix() || op.IsOptsWithRange()) {
		return nil
	}
	return errors.ErrMetaOptionConflict.GenWithStackByArgs()
}

// OpGet returns "get" operation based on given key and operation options.
func OpGet(key string, opts ...OpOption) Op {
	op := Op{T: tGet, key: []byte(key)}
	op.ApplyOpts(opts)
	return op
}

// OpDelete returns "delete" operation based on given key and operation options.
func OpDelete(key string, opts ...OpOption) Op {
	op := Op{T: tDelete, key: []byte(key)}
	op.ApplyOpts(opts)
	return op
}

// OpPut returns "put" operation based on given key-value.
func OpPut(key, val string) Op {
	op := Op{T: tPut, key: []byte(key), val: []byte(val)}
	// [TODO]add some restriction
	return op
}

// OpTxn returns "txn" operation based on given transaction conditions.
func OpTxn(ops []Op) Op {
	return Op{T: tTxn, ops: ops}
}

// GetPrefixRangeEnd gets the range end of the prefix.
// 'Get(foo, WithPrefix())' is equal to 'Get(foo, WithRange(GetPrefixRangeEnd(foo))'.
func GetPrefixRangeEnd(prefix string) string {
	return string(getPrefix([]byte(prefix)))
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return noPrefixEnd
}

// ApplyOpts calls given option function one by one
func (op *Op) ApplyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

// WithPrefix enables 'Get', 'Delete' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key, op.end = []byte{0}, []byte{0}
			return
		}
		op.end = getPrefix(op.key)
		op.isOptsWithPrefix = true
	}
}

// WithRange specifies the range of 'Get', 'Delete' requests.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
// endKey must be lexicographically greater than start key.
func WithRange(endKey string) OpOption {
	return func(op *Op) {
		op.end = []byte(endKey)
		op.isOptsWithRange = true
	}
}

// WithFromKey specifies the range of 'Get', 'Delete' requests
// to be equal or greater than the key in the argument.
func WithFromKey() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key = []byte{0}
		}
		op.end = []byte("\x00")
		op.isOptsWithFromKey = true
	}
}
