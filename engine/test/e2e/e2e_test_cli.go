package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

type utCli struct {
	// used to operate with server master, such as submit job
	masterCli client.MasterClient
	// used to query metadata which is stored from business logic
	metaCli metaclient.KVClient
	// used to write to etcd to simulate the business of fake job, this etcd client
	// reuses the endpoints of user meta KV.
	fakeJobCli *clientv3.Client
	fakeJobCfg *FakeJobConfig
}

type FakeJobConfig struct {
	EtcdEndpoints []string
	WorkerCount   int
	KeyPrefix     string
}

func NewUTCli(
	ctx context.Context, masterAddrs, userMetaAddrs []string, cfg *FakeJobConfig,
) (*utCli, error) {
	masterCli, err := client.NewMasterClient(ctx, masterAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := metaclient.StoreConfigParams{Endpoints: userMetaAddrs}
	userRawKVClient, err := kvclient.NewKVClient(&conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaCli := kvclient.NewPrefixKVClient(userRawKVClient, tenant.DefaultUserTenantID)

	fakeJobCli, err := clientv3.New(clientv3.Config{
		Endpoints:   userMetaAddrs,
		Context:     ctx,
		DialTimeout: 3 * time.Second,
		DialOptions: []grpc.DialOption{},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &utCli{
		masterCli:  masterCli,
		metaCli:    metaCli,
		fakeJobCli: fakeJobCli,
		fakeJobCfg: cfg,
	}, nil
}

func (cli *utCli) CreateJob(ctx context.Context, tp pb.JobType, config []byte) (string, error) {
	req := &pb.SubmitJobRequest{Tp: tp, Config: config}
	resp, err := cli.masterCli.SubmitJob(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Err != nil {
		return "", errors.New(resp.Err.String())
	}
	return resp.JobIdStr, nil
}

func (cli *utCli) PauseJob(ctx context.Context, jobID string) error {
	req := &pb.PauseJobRequest{JobIdStr: jobID}
	resp, err := cli.masterCli.PauseJob(ctx, req)
	if err != nil {
		return err
	}
	if resp.Err != nil {
		return errors.New(resp.Err.String())
	}
	return nil
}

func (cli *utCli) CheckJobStatus(
	ctx context.Context, jobID string, expectedStatus pb.QueryJobResponse_JobStatus,
) (bool, error) {
	req := &pb.QueryJobRequest{JobId: jobID}
	resp, err := cli.masterCli.QueryJob(ctx, req)
	if err != nil {
		return false, err
	}
	if resp.Err != nil {
		return false, errors.New(resp.Err.String())
	}
	return resp.Status == expectedStatus, nil
}

func (cli *utCli) UpdateFakeJobKey(ctx context.Context, id int, value string) error {
	key := fmt.Sprintf("%s%d", cli.fakeJobCfg.KeyPrefix, id)
	_, err := cli.fakeJobCli.Put(ctx, key, value)
	return errors.Trace(err)
}

func (cli *utCli) getFakeJobCheckpoint(
	ctx context.Context, masterID string, jobIndex int,
) (*fake.Checkpoint, error) {
	ckptKey := fake.CheckpointKey(masterID)
	resp, metaErr := cli.metaCli.Get(ctx, ckptKey)
	if metaErr != nil {
		return nil, errors.New(metaErr.Error())
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("no checkpoint found")
	}
	checkpoint := &fake.Checkpoint{}
	err := json.Unmarshal(resp.Kvs[0].Value, checkpoint)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return checkpoint, nil
}

func (cli *utCli) CheckFakeJobTick(
	ctx context.Context, masterID string, jobIndex int, target int64,
) error {
	ckpt, err := cli.getFakeJobCheckpoint(ctx, masterID, jobIndex)
	if err != nil {
		return err
	}
	tick, ok := ckpt.Ticks[jobIndex]
	if !ok {
		return errors.Errorf("job %d not found in checkpoint %v", jobIndex, ckpt)
	}
	if tick < target {
		return errors.Errorf("tick %d not reaches target %d, checkpoint %v", tick, target, ckpt)
	}
	return nil
}

func (cli *utCli) CheckFakeJobKey(
	ctx context.Context, masterID string, jobIndex int, expectedMvcc int, expectedValue string,
) error {
	checkpoint, err := cli.getFakeJobCheckpoint(ctx, masterID, jobIndex)
	if err != nil {
		return err
	}
	ckpt, ok := checkpoint.EtcdCheckpoints[jobIndex]
	if !ok {
		return errors.Errorf("job %d not found in checkpoint %v", jobIndex, checkpoint)
	}
	if ckpt.Value != expectedValue {
		return errors.Errorf(
			"value not equals, expected: '%s', actual: '%s', checkpoint %v",
			expectedValue, ckpt.Value, checkpoint)
	}
	if ckpt.Mvcc != expectedMvcc {
		return errors.Errorf(
			"mvcc not equals, expected: '%d', actual: '%d', checkpoint %v",
			expectedMvcc, ckpt.Mvcc, checkpoint)
	}

	return nil
}
