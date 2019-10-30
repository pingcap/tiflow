// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/sink"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type CancellablePuller struct {
	*Puller

	Cancel context.CancelFunc
}

// SubChangeFeed is a SubChangeFeed task on capture
type SubChangeFeed struct {
	pdEndpoints  []string
	pdCli        pd.Client
	etcdCli      *clientv3.Client
	changefeedID string
	captureID    string
	detail       model.ChangeFeedDetail

	ddlPuller  *Puller
	tblLock    sync.RWMutex
	tblPullers map[int64]CancellablePuller

	schema  *schema.Schema
	mounter *txn.Mounter

	// sink is the Sink to write rows to.
	// Resolved timestamps are never written by Capture
	sink sink.Sink
}

func NewSubChangeFeed(pdEndpoints []string, detail model.ChangeFeedDetail, changefeedID, captureID string) (*SubChangeFeed, error) {
	pdCli, err := pd.NewClient(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdEndpoints)
	}
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
	})
	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}

	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, err
	}
	schema, err := schema.NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := sink.NewMySQLSink(detail.SinkURI, schema, detail.Opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO: get time zone from config
	mounter, err := txn.NewTxnMounter(schema, time.UTC)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SubChangeFeed{
		pdEndpoints:  pdEndpoints,
		changefeedID: changefeedID,
		captureID:    captureID,
		detail:       detail,
		pdCli:        pdCli,
		etcdCli:      etcdCli,
		schema:       schema,
		sink:         sink,
		mounter:      mounter,
		tblPullers:   make(map[int64]CancellablePuller),
	}, nil
}

func (c *SubChangeFeed) Start(ctx context.Context, result chan<- error) {
	errCh := make(chan error, 1)

	ddlSpan := util.Span{
		Start: []byte{'m'},
		End:   []byte{'m' + 1},
	}
	c.ddlPuller = c.startOnSpan(ctx, ddlSpan, errCh)

	info, err := kv.GetSubChangeFeedInfo(ctx, c.etcdCli, c.changefeedID, c.captureID)
	if err != nil {
		result <- err
		return
	}
	for _, tblInfo := range info.TableInfos {
		c.AddTable(ctx, int64(tblInfo.ID), errCh)
	}

	go func() {
		c.watchTables(ctx, errCh)
	}()

	// TODO: Set up a way to notify the pullers of new resolved ts
	var lastTS uint64
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != context.Canceled {
				result <- err
			}
			return
		case e := <-errCh:
			result <- e
			return
		case <-time.After(10 * time.Millisecond):
			ts := c.GetResolvedTs()
			// NOTE: prevent too much noisy log now, refine it later
			if ts != lastTS {
				log.Info("Min ResolvedTs", zap.Uint64("ts", ts))
			}
			lastTS = ts
		}
	}
}

func (c *SubChangeFeed) watchTables(ctx context.Context, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != context.Canceled {
				errCh <- err
			}
			return
		case <-time.After(5 * time.Second):
			info, err := kv.GetSubChangeFeedInfo(ctx, c.etcdCli, c.changefeedID, c.captureID)
			if err != nil && err != context.Canceled {
				errCh <- err
				return
			}
			for _, tblInfo := range info.TableInfos {
				if _, ok := c.tblPullers[int64(tblInfo.ID)]; !ok {
					c.AddTable(ctx, int64(tblInfo.ID), errCh)
				}
			}
		}
	}
}

func (c *SubChangeFeed) GetResolvedTs() uint64 {
	c.tblLock.RLock()
	defer c.tblLock.RUnlock()
	if len(c.tblPullers) == 0 {
		return 0
	}
	var minResolvedTs uint64 = math.MaxUint64
	for _, p := range c.tblPullers {
		ts := p.GetResolvedTs()
		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}
	ddlResolvedTs := c.ddlPuller.GetResolvedTs()
	if ddlResolvedTs < minResolvedTs {
		minResolvedTs = ddlResolvedTs
	}
	return minResolvedTs
}

func (c *SubChangeFeed) AddTable(ctx context.Context, tableID int64, errCh chan<- error) {
	log.Debug("add table", zap.Int64("tableID", tableID))
	c.tblLock.Lock()
	defer c.tblLock.Unlock()
	// TODO: the resolvedTs for the newly added table puller is 0 initially,
	//		 how do we keep the global resolvedTs invariant?
	if _, ok := c.tblPullers[tableID]; ok {
		log.Info("Adding existing table", zap.Int64("ID", tableID))
		return
	}
	span := util.GetTableSpan(tableID)
	ctx, cancel := context.WithCancel(ctx)
	puller := c.startOnSpan(ctx, span, errCh)
	c.tblPullers[tableID] = CancellablePuller{Puller: puller, Cancel: cancel}
}

func (c *SubChangeFeed) RemoveTable(ctx context.Context, tableID int64) error {
	c.tblLock.Lock()
	defer c.tblLock.Unlock()
	puller, ok := c.tblPullers[tableID]
	if !ok {
		return errors.Errorf("remove non-exist tbl: %d", tableID)
	}
	puller.Cancel()
	delete(c.tblPullers, tableID)
	return nil
}

func (c *SubChangeFeed) startOnSpan(ctx context.Context, span util.Span, errCh chan<- error) *Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	checkpointTS := c.detail.StartTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(c.detail.CreateTime.Unix() * 1000)
	}

	puller := NewPuller(c.pdCli, checkpointTS, []util.Span{span}, c.detail)

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		err := puller.CollectRawTxns(ctx, c.writeToSink)
		if err != nil {
			return errors.Annotatef(err, "span: %v", span)
		}
		return nil
	})

	go func() {
		err := errg.Wait()
		errCh <- err
	}()

	return puller
}

func (c *SubChangeFeed) writeToSink(context context.Context, rawTxn txn.RawTxn) error {
	log.Info("RawTxn", zap.Reflect("RawTxn", rawTxn.Entries))
	txn, err := c.mounter.Mount(rawTxn)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.sink.Emit(context, *txn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Output Txn", zap.Reflect("Txn", txn))
	return nil
}
