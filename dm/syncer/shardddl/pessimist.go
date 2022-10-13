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

package shardddl

import (
	"context"
	"sync"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Pessimist used to coordinate the shard DDL migration in pessimism mode.
type Pessimist struct {
	mu sync.RWMutex

	logger log.Logger
	cli    *clientv3.Client
	task   string
	source string

	// the shard DDL info which is pending to handle.
	pendingInfo *pessimism.Info
	// the shard DDL lock operation which is pending to handle.
	pendingOp *pessimism.Operation
}

// NewPessimist creates a new Pessimist instance.
func NewPessimist(pLogger *log.Logger, cli *clientv3.Client, task, source string) *Pessimist {
	return &Pessimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL pessimist")),
		cli:    cli,
		task:   task,
		source: source,
	}
}

// Reset resets the internal state of the pessimist.
func (p *Pessimist) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pendingInfo = nil
	p.pendingOp = nil
}

// ConstructInfo constructs a shard DDL info.
func (p *Pessimist) ConstructInfo(schema, table string, ddls []string) pessimism.Info {
	return pessimism.NewInfo(p.task, p.source, schema, table, ddls)
}

// PutInfo puts the shard DDL info into etcd and returns the revision.
func (p *Pessimist) PutInfo(ctx context.Context, info pessimism.Info) (int64, error) {
	// put info only no previous operation exists or not done.
	rev, putted, err := pessimism.PutInfoIfOpNotDone(p.cli, info)
	if err != nil {
		return 0, err
	} else if putted {
		p.mu.Lock()
		p.pendingInfo = &info
		p.mu.Unlock()

		return rev, nil
	}

	p.logger.Warn("the previous shard DDL operation still exists, waiting for it to be deleted", zap.Stringer("info", info))

	// wait for the operation to be deleted.
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	ch := make(chan pessimism.Operation, 1)
	errCh := make(chan error, 1)
	go pessimism.WatchOperationDelete(ctx2, p.cli, info.Task, info.Source, rev, ch, errCh)

	select {
	case <-ch: // deleted.
	case err = <-errCh:
		return 0, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	rev, err = pessimism.PutInfo(p.cli, info)
	if err != nil {
		return 0, err
	}

	p.mu.Lock()
	p.pendingInfo = &info
	p.mu.Unlock()

	return rev, nil
}

// GetOperation gets the shard DDL lock operation relative to the shard DDL info.
func (p *Pessimist) GetOperation(ctx context.Context, info pessimism.Info, rev int64) (pessimism.Operation, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	ch := make(chan pessimism.Operation, 1)
	errCh := make(chan error, 1)
	go pessimism.WatchOperationPut(ctx2, p.cli, info.Task, info.Source, rev, ch, errCh)

	select {
	case op := <-ch:
		p.mu.Lock()
		p.pendingOp = &op
		p.mu.Unlock()
		return op, nil
	case err := <-errCh:
		return pessimism.Operation{}, err
	case <-ctx.Done():
		return pessimism.Operation{}, ctx.Err()
	}
}

// DoneOperationDeleteInfo marks the shard DDL lock operation as done and delete the shard DDL info.
func (p *Pessimist) DoneOperationDeleteInfo(op pessimism.Operation, info pessimism.Info) error {
	op.Done = true // mark the operation as `done`.

	_, _, err := etcdutil.DoTxnWithRepeatable(p.cli, func(_ *tcontext.Context, cli *clientv3.Client) (interface{}, error) {
		done, _, err := pessimism.PutOperationDeleteExistInfo(cli, op, info)
		if err != nil {
			return nil, err
		} else if !done {
			return nil, terror.ErrWorkerDDLLockInfoNotFound.Generatef("DDL info for (%s, %s) not found", info.Task, info.Source)
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.pendingInfo = nil
	p.pendingOp = nil
	p.mu.Unlock()

	return nil
}

// PendingInfo returns the shard DDL info which is pending to handle.
func (p *Pessimist) PendingInfo() *pessimism.Info {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.pendingInfo == nil {
		return nil
	}
	info := *p.pendingInfo
	return &info
}

// PendingOperation returns the shard DDL lock operation which is pending to handle.
func (p *Pessimist) PendingOperation() *pessimism.Operation {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.pendingOp == nil {
		return nil
	}
	op := *p.pendingOp
	return &op
}
