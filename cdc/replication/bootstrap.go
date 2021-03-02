// Copyright 2021 PingCAP, Inc.
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

package replication

import (
	"context"

	"github.com/pingcap/ticdc/pkg/security"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	defaultSinkErrChSize = 1024
)

type changeFeedBootstrapper interface {
	bootstrapChangeFeed(ctx context.Context, cfID model.ChangeFeedID, cfInfo *model.ChangeFeedInfo, startTs uint64) (changeFeedRunner, error)
}

type changeFeedBootstrapperImpl struct {
	pdClient   pd.Client
	credential *security.Credential
}

func newBootstrapper(pdClient pd.Client, credential *security.Credential) changeFeedBootstrapper {
	return &changeFeedBootstrapperImpl{
		pdClient:   pdClient,
		credential: credential,
	}
}

func (c *changeFeedBootstrapperImpl) bootstrapChangeFeed(
	ctx context.Context,
	cfID model.ChangeFeedID,
	cfInfo *model.ChangeFeedInfo,
	startTs uint64) (changeFeedRunner, error) {
	log.Info("Bootstrapping changefeed", zap.Stringer("info", cfInfo),
		zap.String("changefeed", cfID), zap.Uint64("checkpoint ts", startTs))

	if cfInfo.Config.CheckGCSafePoint {
		err := util.CheckSafetyOfStartTs(ctx, c.pdClient, startTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(nil, nil, cerror.ErrStartTsBeforeGC.GenWithStackByArgs(startTs-300, startTs))
	})

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(nil, nil, errors.New("failpoint injected retriable error"))
	})

	kvStore, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, startTs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, cfInfo.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	eventFilter, err := filter.NewFilter(cfInfo.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sinkErrCh := make(chan error, defaultSinkErrChSize)
	ddlSink, err := sink.NewSink(ctx, cfID, cfInfo.SinkURI, eventFilter, cfInfo.Config, cfInfo.Opts, sinkErrCh)

	ddlHandler := newDDLHandler(ctx, c.pdClient, c.credential, kvStore, startTs)

	ddlErrCh := make(chan error, 1)
	ddlCtx, ddlCancel := context.WithCancel(ctx)
	go func() {
		err := ddlHandler.Run(ddlCtx)
		if err != nil {
			log.Warn("ddhHandler returned error", zap.Error(err))
			ddlErrCh <- err
		}
		close(ddlErrCh)
	}()

	return &changeFeedRunnerImpl{
		cfID:          cfID,
		config:        cfInfo.Config,
		sink:          ddlSink,
		sinkErrCh:     sinkErrCh,
		ddlHandler:    ddlHandler,
		ddlCancel:     ddlCancel,
		ddlErrCh:      ddlErrCh,
		schemaManager: newSchemaManager(schemaSnap, eventFilter, cfInfo.Config.Cyclic),
		filter:        eventFilter,
	}, nil
}
