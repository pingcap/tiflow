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

import "fmt"

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

// Put Response
type PutResponse struct {
	Header *ResponseHeader
}

// Get Response
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

// Delete Response
type DeleteResponse struct {
	Header *ResponseHeader
}

// Txn Response
type TxnResponse struct {
	Header *ResponseHeader
	// Responses is a list of responses corresponding to the results from applying
	// success if succeeded is true or failure if succeeded is false.
	Responses []ResponseOp
}

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

type ResponseOpResponseGet struct {
	ResponseGet *GetResponse
}

type ResponseOpResponsePut struct {
	ResponsePut *PutResponse
}

type ResponseOpResponseDelete struct {
	ResponseDelete *DeleteResponse
}

type ResponseOpResponseTxn struct {
	ResponseTxn *TxnResponse
}

func (*ResponseOpResponseGet) isResponseOp()    {}
func (*ResponseOpResponsePut) isResponseOp()    {}
func (*ResponseOpResponseDelete) isResponseOp() {}
func (*ResponseOpResponseTxn) isResponseOp()    {}

func (m *ResponseOp) GetResponse() isResponseOpResponse {
	if m != nil {
		return m.Response
	}
	return nil
}

func (m *ResponseOp) GetResponseGet() *GetResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseGet); ok {
		return x.ResponseGet
	}
	return nil
}

func (m *ResponseOp) GetResponsePut() *PutResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponsePut); ok {
		return x.ResponsePut
	}
	return nil
}

func (m *ResponseOp) GetResponseDelete() *DeleteResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseDelete); ok {
		return x.ResponseDelete
	}
	return nil
}

func (m *ResponseOp) GetResponseTxn() *TxnResponse {
	if x, ok := m.GetResponse().(*ResponseOpResponseTxn); ok {
		return x.ResponseTxn
	}
	return nil
}

type OpResponse struct {
	put *PutResponse
	get *GetResponse
	del *DeleteResponse
	txn *TxnResponse
}

func (op OpResponse) Put() *PutResponse    { return op.put }
func (op OpResponse) Get() *GetResponse    { return op.get }
func (op OpResponse) Del() *DeleteResponse { return op.del }
func (op OpResponse) Txn() *TxnResponse    { return op.txn }

func (resp *PutResponse) OpResponse() OpResponse {
	return OpResponse{put: resp}
}

func (resp *GetResponse) OpResponse() OpResponse {
	return OpResponse{get: resp}
}

func (resp *DeleteResponse) OpResponse() OpResponse {
	return OpResponse{del: resp}
}

func (resp *TxnResponse) OpResponse() OpResponse {
	return OpResponse{txn: resp}
}

type KeyValue struct {
	// Key is the key in bytes. An empty key is not allowed.
	Key []byte
	// Value is the value held by the key, in bytes.
	Value []byte
}

// String only for debug
func (kv *KeyValue) String() string {
	return fmt.Sprintf("key:%s, value:%s;", string(kv.Key), string(kv.Value))
}
