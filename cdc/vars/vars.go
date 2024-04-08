// Copyright 2020 PingCAP, Inc.
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

package vars

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter/factory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/tikv/client-go/v2/oracle"
)

// GlobalVars contains some vars which can be used anywhere in a pipeline
// the lifecycle of vars in the GlobalVars should be aligned with the ticdc server process.
// All field in Vars should be READ-ONLY and THREAD-SAFE
type GlobalVars struct {
	CaptureInfo *model.CaptureInfo
	EtcdClient  etcd.CDCEtcdClient

	// SortEngineManager is introduced for pull-based sinks.
	SortEngineFactory *factory.SortEngineFactory

	// OwnerRevision is the Etcd revision when the owner got elected.
	OwnerRevision int64

	// MessageServer and MessageRouter are for peer-messaging
	MessageServer *p2p.MessageServer
	MessageRouter p2p.MessageRouter

	// ChangefeedThreadPool is the thread pool for changefeed initialization
	ChangefeedThreadPool workerpool.AsyncPool
}

// NewGlobalVars4Test returns a GlobalVars for test,
func NewGlobalVars4Test() *GlobalVars {
	return &GlobalVars{
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-test",
			AdvertiseAddr: "127.0.0.1:0000",
			// suppose the current version is `v6.3.0`
			Version: "v6.3.0",
		},
		EtcdClient: &etcd.CDCEtcdClientImpl{
			ClusterID: etcd.DefaultCDCClusterID,
		},
		ChangefeedThreadPool: &NonAsyncPool{},
	}
}

// NewGlobalVarsAndChangefeedInfo4Test returns GlobalVars and model.ChangeFeedInfo for ut
func NewGlobalVarsAndChangefeedInfo4Test() (*GlobalVars, *model.ChangeFeedInfo) {
	return NewGlobalVars4Test(),
		&model.ChangeFeedInfo{
			ID:      "changefeed-id-test",
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		}
}

// NonAsyncPool is a dummy implementation of workerpool.AsyncPool, which runs tasks synchronously.
// It is used in tests to avoid the overhead of asynchronous task scheduling.
type NonAsyncPool struct{}

// Go runs the task synchronously.
func (f *NonAsyncPool) Go(_ context.Context, fn func()) error {
	fn()
	return nil
}

// Run does nothing.
func (f *NonAsyncPool) Run(_ context.Context) error {
	return nil
}
