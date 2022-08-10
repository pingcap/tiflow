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
	"github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// sqlError wraps IsRetryable to etcd error.
type sqlError struct {
	displayed error
	cause     error
}

func (e *sqlError) IsRetryable() bool {
	// TODO define retryable error
	return false
}

func (e *sqlError) Error() string {
	return e.displayed.Error()
}

func sqlErrorFromOpFail(err error) *sqlError {
	return &sqlError{
		cause:     err,
		displayed: errors.ErrMetaOpFail.Wrap(err),
	}
}

func makeGetResponseOp(rsp *model.GetResponse) model.ResponseOp {
	return model.ResponseOp{
		Response: &model.ResponseOpResponseGet{
			ResponseGet: rsp,
		},
	}
}

func makePutResponseOp(rsp *model.PutResponse) model.ResponseOp {
	return model.ResponseOp{
		Response: &model.ResponseOpResponsePut{
			ResponsePut: rsp,
		},
	}
}

func makeDelResponseOp(rsp *model.DeleteResponse) model.ResponseOp {
	return model.ResponseOp{
		Response: &model.ResponseOpResponseDelete{
			ResponseDelete: rsp,
		},
	}
}
