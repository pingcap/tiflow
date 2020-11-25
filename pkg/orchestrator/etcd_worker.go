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

package orchestrator

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
)

type EtcdWorker struct {
	client *etcd.Client
	reactor Reactor
}

func NewEtcdWorker(client *etcd.Client, reactor Reactor) (*EtcdWorker, error) {
	return &EtcdWorker{
		client:  client,
		reactor: reactor,
	}, nil
}

func (worker *EtcdWorker) Run(ctx context.Context, prefix string) error {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()

	watchCh := worker.client.Watch(ctx1, prefix, clientv3.WithPrefix())
	for {
		var response clientv3.WatchResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response = <-watchCh:
		}

		if err := response.Err() ; err != nil {
			return errors.Trace(err)
		}

		if response.IsProgressNotify() {
			continue
		}

		for _, event := range response.Events {
			err := worker.handleEvent(ctx, event)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (worker *EtcdWorker) handleEvent(ctx context.Context, event *clientv3.Event) error {

}
