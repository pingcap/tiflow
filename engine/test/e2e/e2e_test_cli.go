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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/lib/fake"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

// ChaosCli is used to interact with server master, fake job and provides ways
// to adding chaos in e2e test.
type ChaosCli struct {
	// used to operate with server master, such as submit job
	masterCli client.MasterClient
	// used to query metadata which is stored from business logic
	metaCli metaclient.KVClient
	// used to write to etcd to simulate the business of fake job, this etcd client
	// reuses the endpoints of user meta KV.
	fakeJobCli *clientv3.Client
	fakeJobCfg *FakeJobConfig
	// used to save project info
	project tenant.ProjectInfo
}

// FakeJobConfig is used to construct a fake job configuration
type FakeJobConfig struct {
	EtcdEndpoints []string
	WorkerCount   int
	KeyPrefix     string
}

// NewUTCli creates a new ChaosCli instance
func NewUTCli(
	ctx context.Context, masterAddrs, userMetaAddrs []string, project tenant.ProjectInfo, cfg *FakeJobConfig,
) (*ChaosCli, error) {
	masterCli, err := client.NewMasterClient(ctx, masterAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := metaclient.StoreConfigParams{Endpoints: userMetaAddrs}
	userRawKVClient, err := kvclient.NewKVClient(&conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaCli := kvclient.NewPrefixKVClient(userRawKVClient, project.UniqueID())

	fakeJobCli, err := clientv3.New(clientv3.Config{
		Endpoints:   userMetaAddrs,
		Context:     ctx,
		DialTimeout: 3 * time.Second,
		DialOptions: []grpc.DialOption{},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ChaosCli{
		masterCli:  masterCli,
		metaCli:    metaCli,
		fakeJobCli: fakeJobCli,
		fakeJobCfg: cfg,
		project:    project,
	}, nil
}

// CreateJob sends SubmitJob command to servermaster
func (cli *ChaosCli) CreateJob(ctx context.Context, tp pb.JobType, config []byte) (string, error) {
	req := &pb.SubmitJobRequest{
		Tp:     tp,
		Config: config,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  cli.project.TenantID(),
			ProjectId: cli.project.ProjectID(),
		},
	}
	resp, err := cli.masterCli.SubmitJob(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Err != nil {
		return "", errors.New(resp.Err.String())
	}
	return resp.JobIdStr, nil
}

// PauseJob sends PauseJob command to servermaster
func (cli *ChaosCli) PauseJob(ctx context.Context, jobID string) error {
	req := &pb.PauseJobRequest{
		JobIdStr: jobID,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  cli.project.TenantID(),
			ProjectId: cli.project.ProjectID(),
		},
	}
	resp, err := cli.masterCli.PauseJob(ctx, req)
	if err != nil {
		return err
	}
	if resp.Err != nil {
		return errors.New(resp.Err.String())
	}
	return nil
}

// CheckJobStatus checks job status is as expected.
func (cli *ChaosCli) CheckJobStatus(
	ctx context.Context, jobID string, expectedStatus pb.QueryJobResponse_JobStatus,
) (bool, error) {
	req := &pb.QueryJobRequest{
		JobId: jobID,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  cli.project.TenantID(),
			ProjectId: cli.project.ProjectID(),
		},
	}
	resp, err := cli.masterCli.QueryJob(ctx, req)
	if err != nil {
		return false, err
	}
	if resp.Err != nil {
		return false, errors.New(resp.Err.String())
	}
	return resp.Status == expectedStatus, nil
}

// UpdateFakeJobKey updates the etcd value of a worker beloinging to a fake job
func (cli *ChaosCli) UpdateFakeJobKey(ctx context.Context, id int, value string) error {
	key := fmt.Sprintf("%s%d", cli.fakeJobCfg.KeyPrefix, id)
	_, err := cli.fakeJobCli.Put(ctx, key, value)
	return errors.Trace(err)
}

func (cli *ChaosCli) getFakeJobCheckpoint(
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

// CheckFakeJobTick queries the checkpoint of a fake job and checks the tick count
// is as expected.
func (cli *ChaosCli) CheckFakeJobTick(
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

// CheckFakeJobKey queries the checkpoint of a fake job, checks the value and mvcc
// count are as expected. If error happens or check is not passed, return error.
func (cli *ChaosCli) CheckFakeJobKey(
	ctx context.Context, masterID string, jobIndex int, expectedMvcc int, expectedValue string,
) error {
	checkpoint, err := cli.getFakeJobCheckpoint(ctx, masterID, jobIndex)
	if err != nil {
		return err
	}
	ckpt, ok := checkpoint.Checkpoints[jobIndex]
	if !ok {
		return errors.Errorf("job %d not found in checkpoint %v", jobIndex, checkpoint)
	}
	if ckpt.Value != expectedValue {
		return errors.Errorf(
			"value not equals, expected: '%s', actual: '%s', checkpoint %v",
			expectedValue, ckpt.Value, checkpoint)
	}
	if ckpt.MvccCount != expectedMvcc {
		return errors.Errorf(
			"mvcc not equals, expected: '%d', actual: '%d', checkpoint %v",
			expectedMvcc, ckpt.MvccCount, checkpoint)
	}

	return nil
}

// GetRevision puts a key gets the latest revision of etcd cluster
func (cli *ChaosCli) GetRevision(ctx context.Context) (int64, error) {
	resp, err := cli.fakeJobCli.Put(ctx, "/chaos/gen_epoch/key", "/chaos/gen_epoch/value")
	if err != nil {
		return 0, errors.Trace(err)
	}
	return resp.Header.Revision, nil
}

func runCmdHandleError(cmd *exec.Cmd) []byte {
	log.L().Info("Start executing command", zap.String("cmd", cmd.String()))
	bytes, err := cmd.Output()
	if err, ok := err.(*exec.ExitError); ok {
		log.L().Info("Running command failed", zap.ByteString("stderr", err.Stderr))
	}

	if err != nil {
		log.L().Fatal("Running command failed",
			zap.Error(err),
			zap.String("command", cmd.String()),
			zap.ByteString("output", bytes))
	}

	log.L().Info("Finished executing command", zap.String("cmd", cmd.String()), zap.ByteString("output", bytes))
	return bytes
}

// ContainerRestart restarts a docker container
func (cli *ChaosCli) ContainerRestart(name string) {
	cmd := exec.Command("docker", "restart", name)
	runCmdHandleError(cmd)
	log.L().Info("Finished restarting container", zap.String("name", name))
}

// ContainerStop stops a docker container
func (cli *ChaosCli) ContainerStop(name string) {
	cmd := exec.Command("docker", "stop", name)
	runCmdHandleError(cmd)
	log.L().Info("Finished stopping container", zap.String("name", name))
}

// ContainerStart starts a docker container
func (cli *ChaosCli) ContainerStart(name string) {
	cmd := exec.Command("docker", "start", name)
	runCmdHandleError(cmd)
	log.L().Info("Finished starting container", zap.String("name", name))
}
