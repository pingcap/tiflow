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

package capture

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
)

// election wraps the owner election methods.
// It is useful in testing.
type election interface {
	campaign(ctx context.Context, key string) error
	resign(ctx context.Context) error
}

type electionImpl struct {
	election *concurrency.Election
}

func newElection(sess *concurrency.Session, key string) election {
	return &electionImpl{
		election: concurrency.NewElection(sess, key),
	}
}

func (e *electionImpl) campaign(ctx context.Context, val string) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return e.election.Campaign(ctx, val)
}

func (e *electionImpl) resign(ctx context.Context) error {
	return e.election.Resign(ctx)
}
