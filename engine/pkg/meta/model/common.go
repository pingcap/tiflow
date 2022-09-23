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
	"fmt"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

type (
	// ProjectID is the alia of tenant.ProjectID
	ProjectID = tenant.ProjectID
	// JobID is the alias of model.JobID
	JobID = model.JobID
)

// ClientType indicates the kvclient type
type ClientType int

// define client type
const (
	UnknownKVClientType = iota
	EtcdKVClientType
	SQLKVClientType
	MockKVClientType
)

// String implements the Stringer interface
func (t ClientType) String() string {
	switch t {
	case UnknownKVClientType:
		return "unknown-kvclient"
	case EtcdKVClientType:
		return "etcd-kvclient"
	case SQLKVClientType:
		return "sql-kvclient"
	case MockKVClientType:
		return "mock-kvclient"
	}

	return fmt.Sprintf("unexpect client type:%d", int(t))
}

// ResponseHeader is common response header
type ResponseHeader struct {
	// ClusterId is the ID of the cluster which sent the response.
	// Framework will generate uuid for every newcoming metastore
	ClusterID string
}

// String only for debug
func (h *ResponseHeader) String() string {
	return fmt.Sprintf("clusterID:%s;", h.ClusterID)
}

// PutResponse .
type PutResponse struct {
	Header *ResponseHeader
}

// GetResponse .
type GetResponse struct {
	Header *ResponseHeader
	// kvs is the list of key-value pairs matched by the range request.
	Kvs []*KeyValue
}

// String only for debug
func (resp *GetResponse) String() string {
	s := fmt.Sprintf("header:[%s];kvs:[", resp.Header)
	for _, kv := range resp.Kvs {
		s += kv.String()
	}

	s += "];"
	return s
}

// DeleteResponse .
type DeleteResponse struct {
	Header *ResponseHeader
}

// TxnResponse .
type TxnResponse struct {
	Header *ResponseHeader
	// Responses is a list of responses corresponding to the results from applying
	// success if succeeded is true or failure if succeeded is false.
	Responses []ResponseOp
}

// ResponseOp defines a response operation, the op is one of get/put/delete/txn
type ResponseOp struct {
	// response is a union of response types returned by a transaction.
	//
	// Types that are valid to be assigned to Response:
	//	*ResponseOp_ResponseRange
	//	*ResponseOp_ResponsePut
	//	*ResponseOp_ResponseDeleteRange
	//	*ResponseOp_ResponseTxn
	Response isResponseOpResponse
}

// Using interface to make union
type isResponseOpResponse interface {
	isResponseOp()
}

// ResponseOpResponseGet defines an op that wraps GetResponse
type ResponseOpResponseGet struct {
	ResponseGet *GetResponse
}

// ResponseOpResponsePut defines an op that wraps PutResponse
type ResponseOpResponsePut struct {
	ResponsePut *PutResponse
}

// ResponseOpResponseDelete defines an op that wraps DeleteResponse
type ResponseOpResponseDelete struct {
	ResponseDelete *DeleteResponse
}

// ResponseOpResponseTxn defines an op that wraps TxnResponse
type ResponseOpResponseTxn struct {
	ResponseTxn *TxnResponse
}

func (*ResponseOpResponseGet) isResponseOp()    {}
func (*ResponseOpResponsePut) isResponseOp()    {}
func (*ResponseOpResponseDelete) isResponseOp() {}
func (*ResponseOpResponseTxn) isResponseOp()    {}

// GetResponse returns an isResponseOpResponse interface
func (m *ResponseOp) GetResponse() isResponseOpResponse {
	if m != nil {
		return m.Response
	}
	return nil
}

// GetResponseGet returns a ResponseGet if it matches
func (m *ResponseOp) GetResponseGet() *GetResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseGet); ok {
		return x.ResponseGet
	}
	return nil
}

// GetResponsePut returns a ResponsePut if it matches
func (m *ResponseOp) GetResponsePut() *PutResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponsePut); ok {
		return x.ResponsePut
	}
	return nil
}

// GetResponseDelete returns a ResponseDelete if it matches
func (m *ResponseOp) GetResponseDelete() *DeleteResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseDelete); ok {
		return x.ResponseDelete
	}
	return nil
}

// GetResponseTxn returns a ResponseTxn if it matches
func (m *ResponseOp) GetResponseTxn() *TxnResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseTxn); ok {
		return x.ResponseTxn
	}
	return nil
}

// OpResponse contains a list of put/get/del/txn response
type OpResponse struct {
	put *PutResponse
	get *GetResponse
	del *DeleteResponse
	txn *TxnResponse
}

// Put returns a PutResponse
func (op OpResponse) Put() *PutResponse { return op.put }

// Get returns a GetResponse
func (op OpResponse) Get() *GetResponse { return op.get }

// Del returns a DelResponse
func (op OpResponse) Del() *DeleteResponse { return op.del }

// Txn returns a TxnResponse
func (op OpResponse) Txn() *TxnResponse { return op.txn }

// OpResponse generates a put OpResponse from PutResponse
func (resp *PutResponse) OpResponse() OpResponse {
	return OpResponse{put: resp}
}

// OpResponse generates a get OpResponse from GetResponse
func (resp *GetResponse) OpResponse() OpResponse {
	return OpResponse{get: resp}
}

// OpResponse generates a delete OpResponse from DeleteResponse
func (resp *DeleteResponse) OpResponse() OpResponse {
	return OpResponse{del: resp}
}

// OpResponse generates a txn OpResponse from TxnResponse
func (resp *TxnResponse) OpResponse() OpResponse {
	return OpResponse{txn: resp}
}

// KeyValue defines a key value byte slice pair
type KeyValue struct {
	// Key is the key in bytes. An empty key is not allowed.
	Key []byte `gorm:"column:meta_key;type:varbinary(2048) not null;uniqueIndex:uidx_jk,priority:2"`
	// Value is the value held by the key, in bytes.
	Value []byte `gorm:"column:meta_value;type:longblob"`
}

// String only for debug
func (kv *KeyValue) String() string {
	return fmt.Sprintf("key:%s, value:%s;", string(kv.Key), string(kv.Value))
}
