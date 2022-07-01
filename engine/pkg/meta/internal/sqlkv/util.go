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

package sqlkv

import (
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/pkg/errorutil"
)

// sqlError wraps IsRetryable to etcd error.
type sqlError struct {
	displayed error
	cause     error
}

func (e *sqlError) IsRetryable() bool {
	if e.cause != nil {
		return errorutil.IsRetryableEtcdError(e.cause)
	}
	// TODO define retryable error
	return false
}

func (e *sqlError) Error() string {
	return e.displayed.Error()
}

func sqlErrorFromOpFail(err error) *sqlError {
	return &sqlError{
		cause:     err,
		displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs(err),
	}
}

func makeGetResponseOp(rsp *metaclient.GetResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponseGet{
			ResponseGet: rsp,
		},
	}
}

func makePutResponseOp(rsp *metaclient.PutResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponsePut{
			ResponsePut: rsp,
		},
	}
}

func makeDelResponseOp(rsp *metaclient.DeleteResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponseDelete{
			ResponseDelete: rsp,
		},
	}
}
