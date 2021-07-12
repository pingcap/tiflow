package util

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	cmdconetxt "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	etcdlogutil "go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type factoryImpl struct {
	clientGetter ClientGetter
}

func NewFactory(clientGetter ClientGetter) Factory {
	if clientGetter == nil {
		panic("attempt to instantiate factory with nil clientGetter")
	}
	f := &factoryImpl{
		clientGetter: clientGetter,
	}

	return f
}

func (f *factoryImpl) ToTLSConfig() (*tls.Config, error) {
	return f.clientGetter.ToTLSConfig()
}

func (f *factoryImpl) ToGRPCDialOption() (grpc.DialOption, error) {
	return f.clientGetter.ToGRPCDialOption()
}

func (f *factoryImpl) GetPdAddr() string {
	return f.clientGetter.GetPdAddr()
}
func (f *factoryImpl) GetCredential() *security.Credential {
	return f.clientGetter.GetCredential()
}

func (f *factoryImpl) EtcdClient() (*kv.CDCEtcdClient, error) {
	tlsConfig, err := f.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	grpcTLSOption, err := f.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	logConfig := etcdlogutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	pdEndpoints := strings.Split(f.GetPdAddr(), ",")
	etcdClient, err := clientv3.New(clientv3.Config{
		Context:     cmdconetxt.GetDefaultContext(),
		Endpoints:   pdEndpoints,
		TLS:         tlsConfig,
		LogConfig:   &logConfig,
		DialTimeout: 30 * time.Second,
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
	client := kv.NewCDCEtcdClient(cmdconetxt.GetDefaultContext(), etcdClient)
	return &client, nil
}

func (f factoryImpl) PdClient() (pd.Client, error) {
	credential := f.GetCredential()
	grpcTLSOption, err := f.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	pdAddr := f.GetPdAddr()
	pdEndpoints := strings.Split(pdAddr, ",")
	ctx := cmdconetxt.GetDefaultContext()
	pdClient, err := pd.NewClientWithContext(
		ctx, pdEndpoints, credential.PDSecurityOption(),
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
		return nil, errors.Annotatef(err, "fail to open PD client, pd=\"%s\"", pdAddr)
	}

	err = version.CheckClusterVersion(ctx, pdClient, pdEndpoints[0], credential, true)
	if err != nil {
		return nil, err
	}

	return pdClient, nil
}
