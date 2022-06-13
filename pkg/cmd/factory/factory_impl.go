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

package factory

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	pd "github.com/tikv/pd/client"
	etcdlogutil "go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/version"
)

type factoryImpl struct {
	clientGetter      ClientGetter
	fetchedServerAddr string
}

// NewFactory creates a client build factory.
func NewFactory(clientGetter ClientGetter) Factory {
	if clientGetter == nil {
		panic("attempt to instantiate factory with nil clientGetter")
	}
	f := &factoryImpl{
		clientGetter: clientGetter,
	}

	return f
}

// ToTLSConfig returns the configuration of tls.
func (f *factoryImpl) ToTLSConfig() (*tls.Config, error) {
	return f.clientGetter.ToTLSConfig()
}

// ToGRPCDialOption returns the option of GRPC dial.
func (f *factoryImpl) ToGRPCDialOption() (grpc.DialOption, error) {
	return f.clientGetter.ToGRPCDialOption()
}

// GetPdAddr returns pd address.
func (f *factoryImpl) GetPdAddr() string {
	return f.clientGetter.GetPdAddr()
}

// GetServerAddr returns CDC server address.
func (f *factoryImpl) GetServerAddr() string {
	return f.clientGetter.GetServerAddr()
}

// GetLogLevel returns log level.
func (f *factoryImpl) GetLogLevel() string {
	return f.clientGetter.GetLogLevel()
}

// GetCredential returns security credentials.
func (f *factoryImpl) GetCredential() *security.Credential {
	return f.clientGetter.GetCredential()
}

// EtcdClient creates new cdc etcd client.
func (f *factoryImpl) EtcdClient() (*etcd.CDCEtcdClient, error) {
	ctx := cmdconetxt.GetDefaultContext()
	tlsConfig, err := f.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	grpcTLSOption, err := f.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	logConfig := etcdlogutil.DefaultZapLoggerConfig
	logLevel := zap.NewAtomicLevel()
	err = logLevel.UnmarshalText([]byte(f.GetLogLevel()))
	if err != nil {
		return nil, err
	}
	logConfig.Level = logLevel

	pdAddr := f.GetPdAddr()
	pdEndpoints := strings.Split(pdAddr, ",")

	etcdClient, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   pdEndpoints,
		TLS:         tlsConfig,
		LogConfig:   &logConfig,
		DialTimeout: 30 * time.Second,
		// TODO(hi-rustin): add gRPC metrics to Options.
		// See also: https://github.com/pingcap/tiflow/pull/2341#discussion_r673018537.
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
		return nil, errors.Annotatef(err,
			"fail to open PD client, please check pd address \"%s\"", pdAddr)
	}

	client, err := etcd.NewCDCEtcdClient(ctx, etcdClient, "default")
	return &client, err
}

// PdClient creates new pd client.
func (f factoryImpl) PdClient() (pd.Client, error) {
	ctx := cmdconetxt.GetDefaultContext()

	credential := f.GetCredential()
	grpcTLSOption, err := f.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	pdAddr := f.GetPdAddr()
	pdEndpoints := strings.Split(pdAddr, ",")

	pdClient, err := pd.NewClientWithContext(
		ctx, pdEndpoints, credential.PDSecurityOption(),
		// TODO(hi-rustin): add gRPC metrics to Options.
		// See also: https://github.com/pingcap/tiflow/pull/2341#discussion_r673032407.
		pd.WithGRPCDialOptions(
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
		))
	if err != nil {
		return nil, errors.Annotatef(err,
			"fail to open PD client, please check pd address \"%s\"", pdAddr)
	}

	err = version.CheckClusterVersion(ctx, pdClient, pdEndpoints, credential, true)
	if err != nil {
		return nil, err
	}

	return pdClient, nil
}

// APIV1Client returns cdc api v1 client.
func (f *factoryImpl) APIV1Client() (*apiv1client.APIV1Client, error) {
	serverAddr, err := f.findServerAddr()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info(serverAddr)
	return apiv1client.NewAPIClient(serverAddr, f.clientGetter.GetCredential())
}

// APIV2Client returns cdc api v2 client.
func (f *factoryImpl) APIV2Client() (*apiv2client.APIV2Client, error) {
	serverAddr, err := f.findServerAddr()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info(serverAddr)
	return apiv2client.NewAPIClient(serverAddr, f.clientGetter.GetCredential())
}

func (f *factoryImpl) findServerAddr() (string, error) {
	if f.fetchedServerAddr != "" {
		return f.fetchedServerAddr, nil
	}

	pdAddr := f.clientGetter.GetPdAddr()
	serverAddr := f.clientGetter.GetServerAddr()
	if pdAddr == "" && serverAddr == "" {
		return "http://127.0.0.1:8300", nil
	}
	if pdAddr != "" && serverAddr != "" {
		return "", errors.New("Parameter --pd is deprecated, " +
			"please use parameter --server instead. " +
			"These two parameters cannot be specified at the same time.")
	}
	if f.clientGetter.GetServerAddr() != "" {
		return f.clientGetter.GetServerAddr(), nil
	}
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return "", errors.Trace(err)
	}

	ctx := cmdconetxt.GetDefaultContext()
	ownerID, err := etcdClient.GetOwnerID(ctx)
	if err != nil {
		return "", err
	}
	_, captures, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	for _, capture := range captures {
		if capture.ID == ownerID {
			f.fetchedServerAddr = capture.AdvertiseAddr
			return capture.AdvertiseAddr, nil
		}
	}
	return "", errors.New("no capture is found")
}
