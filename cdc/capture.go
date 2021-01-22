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

package cdc

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/orchestrator"

	"github.com/pingcap/ticdc/pkg/processor"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	captureSessionTTL = 3
)

// processorOpts records options for processor
type processorOpts struct {
	flushCheckpointInterval time.Duration
}

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules Task on it.
type Capture struct {
	etcdClient kv.CDCEtcdClient
	pdCli      pd.Client
	credential *security.Credential

	processorManager *processor.Manager

	info *model.CaptureInfo

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election *concurrency.Election

	opts *processorOpts
}

// NewCapture returns a new Capture instance
func NewCapture(
	ctx context.Context,
	pdEndpoints []string,
	pdCli pd.Client,
	credential *security.Credential,
	advertiseAddr string,
	opts *processorOpts,
) (c *Capture, err error) {
	tlsConfig, err := credential.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		TLS:         tlsConfig,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "new etcd client")
	}
	sess, err := concurrency.NewSession(etcdCli,
		concurrency.WithTTL(captureSessionTTL))
	if err != nil {
		return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "create capture session")
	}
	elec := concurrency.NewElection(sess, kv.CaptureOwnerKey)
	cli := kv.NewCDCEtcdClient(ctx, etcdCli)
	id := uuid.New().String()
	info := &model.CaptureInfo{
		ID:            id,
		AdvertiseAddr: advertiseAddr,
	}
	processorManager := processor.NewManager(pdCli, credential, info)
	log.Info("creating capture", zap.String("capture-id", id), util.ZapFieldCapture(ctx))

	c = &Capture{
		etcdClient:       cli,
		credential:       credential,
		session:          sess,
		election:         elec,
		info:             info,
		opts:             opts,
		pdCli:            pdCli,
		processorManager: processorManager,
	}

	return
}

// Run runs the Capture mainloop
func (c *Capture) Run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	// TODO: we'd better to add some wait mechanism to ensure no routine is blocked
	defer cancel()
	err = c.register(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	sessionCli := c.session.Client()
	etcdWorker, err := orchestrator.NewEtcdWorker(kv.NewCDCEtcdClient(ctx, sessionCli).Client, kv.EtcdKeyBase, c.processorManager, processor.NewGlobalState(c.info.ID))
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, 200*time.Millisecond); err != nil {
		// We check ttl of lease instead of check `session.Done`, because
		// `session.Done` is only notified when etcd client establish a
		// new keepalive request, there could be a time window as long as
		// 1/3 of session ttl that `session.Done` can't be triggered even
		// the lease is already revoked.
		lease, inErr := c.etcdClient.Client.TimeToLive(ctx, c.session.Lease())
		if inErr != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, inErr)
		}
		if lease.TTL == int64(-1) {
			log.Warn("handle task event failed because session is disconnected", zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		return errors.Trace(err)
	}
	return nil
}

// Campaign to be an owner
func (c *Capture) Campaign(ctx context.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.Campaign(ctx, c.info.ID))
}

// Resign lets a owner start a new election.
func (c *Capture) Resign(ctx context.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.Resign(ctx))
}

// Close closes the capture by unregistering it from etcd
func (c *Capture) Close() error {
	return c.session.Close()
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	err := c.etcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease())
	return cerror.WrapError(cerror.ErrCaptureRegister, err)
}
