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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseOpUnion(t *testing.T) {
	t.Parallel()

	get := ResponseOp{
		Response: &ResponseOpResponseGet{
			ResponseGet: &GetResponse{
				Header: &ResponseHeader{
					ClusterID: "1111",
				},
			},
		},
	}
	require.IsType(t, &GetResponse{}, get.GetResponseGet())
	require.Nil(t, get.GetResponsePut())

	put := ResponseOp{
		Response: &ResponseOpResponsePut{
			ResponsePut: &PutResponse{
				Header: &ResponseHeader{
					ClusterID: "1111",
				},
			},
		},
	}
	require.IsType(t, &PutResponse{}, put.GetResponsePut())
	require.Nil(t, put.GetResponseDelete())

	delet := ResponseOp{
		Response: &ResponseOpResponseDelete{
			ResponseDelete: &DeleteResponse{
				Header: &ResponseHeader{
					ClusterID: "1111",
				},
			},
		},
	}
	require.IsType(t, &DeleteResponse{}, delet.GetResponseDelete())
	require.Nil(t, delet.GetResponseTxn())

	txn := ResponseOp{
		Response: &ResponseOpResponseTxn{
			ResponseTxn: &TxnResponse{
				Header: &ResponseHeader{
					ClusterID: "1111",
				},
			},
		},
	}
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn())
	require.Nil(t, txn.GetResponseGet())
}

func TestNestedTxnResponse(t *testing.T) {
	txn := ResponseOp{
		Response: &ResponseOpResponseTxn{
			ResponseTxn: &TxnResponse{
				Header: &ResponseHeader{
					ClusterID: "1111",
				},
				Responses: []ResponseOp{
					{
						Response: &ResponseOpResponseGet{
							ResponseGet: &GetResponse{
								Header: &ResponseHeader{
									ClusterID: "1111",
								},
							},
						},
					},
					{
						Response: &ResponseOpResponsePut{
							ResponsePut: &PutResponse{
								Header: &ResponseHeader{
									ClusterID: "1111",
								},
							},
						},
					},
					{
						Response: &ResponseOpResponseTxn{
							ResponseTxn: &TxnResponse{
								Header: &ResponseHeader{
									ClusterID: "1111",
								},
							},
						},
					},
				},
			},
		},
	}
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn())
	require.IsType(t, &GetResponse{}, txn.GetResponseTxn().Responses[0].GetResponseGet())
	require.IsType(t, &PutResponse{}, txn.GetResponseTxn().Responses[1].GetResponsePut())
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn().Responses[2].GetResponseTxn())
}
