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

package etcd

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// CDCEtcdClientForAPI extracts CDCEtcdClients's method used for apiv2
// TODO: refactor CDCEtcdClient as interface, and use it for v1
type CDCEtcdClientForAPI interface {
	CreateChangefeedInfo(context.Context, *model.UpstreamInfo,
		*model.ChangeFeedInfo, model.ChangeFeedID) error
	UpdateChangefeedAndUpstream(ctx context.Context, upstreamInfo *model.UpstreamInfo,
		changeFeedInfo *model.ChangeFeedInfo, changeFeedID model.ChangeFeedID,
	) error
	GetUpstreamInfo(ctx context.Context, upstreamID model.UpstreamID,
		namespace string) (*model.UpstreamInfo, error)
	GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error)
	GetGCServiceID() string
	GetEnsureGCServiceID() string
	SaveChangeFeedInfo(ctx context.Context, info *model.ChangeFeedInfo,
		changeFeedID model.ChangeFeedID) error
}
