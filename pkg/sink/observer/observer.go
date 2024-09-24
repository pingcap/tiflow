// Copyright 2023 PingCAP, Inc.
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

package observer

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
)

// Observer defines an interface of downstream performance observer.
type Observer interface {
	// Tick is called periodically, Observer fetches performance metrics and
	// records them in each Tick.
	// Tick and Close must be concurrent safe.
	Tick(ctx context.Context) error
	Close() error
}

// NewObserverOpt represents available options when creating a new observer.
type NewObserverOpt struct {
	dbConnFactory pmysql.ConnectionFactory
}

// NewObserverOption configures NewObserverOpt.
type NewObserverOption func(*NewObserverOpt)

// WithDBConnFactory specifies factory to create db connection.
func WithDBConnFactory(factory pmysql.ConnectionFactory) NewObserverOption {
	return func(opt *NewObserverOpt) {
		opt.dbConnFactory = factory
	}
}

// NewObserver creates a new Observer
func NewObserver(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURIStr string,
	replCfg *config.ReplicaConfig,
	opts ...NewObserverOption,
) (Observer, error) {
	creator := func() (Observer, error) {
		options := &NewObserverOpt{dbConnFactory: pmysql.CreateMySQLDBConn}
		for _, opt := range opts {
			opt(options)
		}

		sinkURI, err := url.Parse(sinkURIStr)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		scheme := strings.ToLower(sinkURI.Scheme)
		if !sink.IsMySQLCompatibleScheme(scheme) {
			return NewDummyObserver(), nil
		}

		cfg := pmysql.NewConfig()
		err = cfg.Apply(config.GetGlobalServerConfig().TZ, changefeedID, sinkURI, replCfg)
		if err != nil {
			return nil, err
		}

		dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, options.dbConnFactory)
		if err != nil {
			return nil, err
		}
		db, err := options.dbConnFactory(ctx, dsnStr)
		if err != nil {
			return nil, err
		}
		db.SetMaxIdleConns(2)
		db.SetMaxOpenConns(2)

		isTiDB := pmysql.CheckIsTiDB(ctx, db)
		if isTiDB {
			return NewTiDBObserver(db), nil
		}
		_ = db.Close()
		return NewDummyObserver(), nil
	}
	return &observerAgent{creator: creator}, nil
}

type observerAgent struct {
	creator func() (Observer, error)

	mu struct {
		sync.Mutex
		inner  Observer
		closed bool
	}
}

// Tick implements Observer interface.
func (o *observerAgent) Tick(ctx context.Context) error {
	o.mu.Lock()
	if o.mu.inner != nil {
		defer o.mu.Unlock()
		return o.mu.inner.Tick(ctx)
	}
	if o.mu.closed {
		defer o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	inner, err := o.creator()
	if err != nil {
		return errors.Trace(err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.mu.closed {
		o.mu.inner = inner
		return o.mu.inner.Tick(ctx)
	}
	return nil
}

// Close implements Observer interface.
func (o *observerAgent) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.inner != nil {
		o.mu.closed = true
		return o.mu.inner.Close()
	}
	return nil
}
