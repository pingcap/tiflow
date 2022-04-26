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

package shardddl

import (
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
)

// IsEtcdPutOpRetryable returns true if the etcd error is retryable
// https://github.com/etcd-io/etcd/blob/v3.5.2/client/v3/retry.go#L53
func IsEtcdPutOpRetryable(_ int, err error) bool {
	if etcdutil.IsRetryableError(err) {
		return true
	}
	eErr := rpctypes.Error(err)
	if serverErr, ok := eErr.(rpctypes.EtcdError); ok && serverErr.Code() != codes.Unavailable {

		return false
	}
	// only retry if unavailable
	return status.Code(err) == codes.Unavailable
}
