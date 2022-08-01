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
	"io"
	"net/url"
	"os/exec"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/fake"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	server "github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/pkg/httputil"
)

var (
	defaultTimeout = 3 * time.Second
)

func init() {
	// set the debug level log for easy test
	log.SetLevel(zapcore.DebugLevel)
}

// ChaosCli is used to interact with server master, fake job and provides ways
// to adding chaos in e2e test.
type ChaosCli struct {
	masterAddrs []string
	// masterCli is used to operate with server master, such as submit job
	masterCli client.MasterClient
	// clientConn is the client connection for business metastore
	clientConn metaModel.ClientConn
	// metaCli is used to query metadata which is stored from business logic(job-level isolation)
	// NEED to reinitialize the metaCli if we access to a different job
	metaCli metaModel.KVClient
	// fakeJobCli is used to write to etcd to simulate the business of fake job
	fakeJobCli *clientv3.Client
	fakeJobCfg *FakeJobConfig
	// project is used to save project info
	project tenant.ProjectInfo
}

// FakeJobConfig is used to construct a fake job configuration
type FakeJobConfig struct {
	EtcdEndpoints []string
	WorkerCount   int
	KeyPrefix     string
}

// NewUTCli creates a new ChaosCli instance
func NewUTCli(ctx context.Context, masterAddrs, businessMetaAddrs []string, project tenant.ProjectInfo,
	cfg *FakeJobConfig,
) (*ChaosCli, error) {
	// TODO: NEED to move metastore config to a toml, and parse the toml
	defaultSchema := "test"
	masterCli, err := client.NewMasterClient(ctx, masterAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := server.NewDefaultBusinessMetaConfig()
	conf.Endpoints = businessMetaAddrs
	conf.Schema = defaultSchema
	cc, err := meta.NewClientConn(conf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fakeJobCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		Context:     ctx,
		DialTimeout: 3 * time.Second,
		DialOptions: []grpc.DialOption{},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ChaosCli{
		masterAddrs: masterAddrs,
		masterCli:   masterCli,
		clientConn:  cc,
		fakeJobCli:  fakeJobCli,
		fakeJobCfg:  cfg,
		project:     project,
	}, nil
}

// CreateJob sends SubmitJob command to servermaster
func (cli *ChaosCli) CreateJob(ctx context.Context, tp engineModel.JobType, config []byte) (string, error) {
	return CreateJobViaOpenAPI(ctx, cli.masterAddrs[0], cli.project.TenantID(),
		cli.project.ProjectID(), tp, string(config))
}

// PauseJob sends PauseJob command to servermaster
func (cli *ChaosCli) PauseJob(ctx context.Context, jobID string) error {
	req := &pb.PauseJobRequest{
		JobId: jobID,
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

// UpdateFakeJobKey updates the etcd value of a worker belonging to a fake job
func (cli *ChaosCli) UpdateFakeJobKey(ctx context.Context, id int, value string) error {
	key := fmt.Sprintf("%s%d", cli.fakeJobCfg.KeyPrefix, id)
	_, err := cli.fakeJobCli.Put(ctx, key, value)
	return errors.Trace(err)
}

func (cli *ChaosCli) getFakeJobCheckpoint(
	ctx context.Context, masterID string,
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
	log.Debug("get fake job checkpoint", zap.String("ckptKey", ckptKey),
		zap.Any("checkpoint", checkpoint))
	return checkpoint, nil
}

// CheckFakeJobTick queries the checkpoint of a fake job and checks the tick count
// is as expected.
func (cli *ChaosCli) CheckFakeJobTick(
	ctx context.Context, masterID string, jobIndex int, target int64,
) error {
	ckpt, err := cli.getFakeJobCheckpoint(ctx, masterID)
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
	checkpoint, err := cli.getFakeJobCheckpoint(ctx, masterID)
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
	log.Info("Start executing command", zap.String("cmd", cmd.String()))
	bytes, err := cmd.Output()
	if err, ok := err.(*exec.ExitError); ok {
		log.Info("Running command failed", zap.ByteString("stderr", err.Stderr))
	}

	if err != nil {
		log.Fatal("Running command failed",
			zap.Error(err),
			zap.String("command", cmd.String()),
			zap.ByteString("output", bytes))
	}

	log.Info("Finished executing command", zap.String("cmd", cmd.String()), zap.ByteString("output", bytes))
	return bytes
}

// ContainerRestart restarts a docker container
func (cli *ChaosCli) ContainerRestart(name string) {
	cmd := exec.Command("docker", "restart", name)
	runCmdHandleError(cmd)
	log.Info("Finished restarting container", zap.String("name", name))
}

// ContainerStop stops a docker container
func (cli *ChaosCli) ContainerStop(name string) {
	cmd := exec.Command("docker", "stop", name)
	runCmdHandleError(cmd)
	log.Info("Finished stopping container", zap.String("name", name))
}

// ContainerStart starts a docker container
func (cli *ChaosCli) ContainerStart(name string) {
	cmd := exec.Command("docker", "start", name)
	runCmdHandleError(cmd)
	log.Info("Finished starting container", zap.String("name", name))
}

// InitializeMetaClient initializes the business kvclient
func (cli *ChaosCli) InitializeMetaClient(jobID string) error {
	if cli.metaCli != nil {
		cli.metaCli.Close()
	}
	metaCli, err := meta.NewKVClientWithNamespace(cli.clientConn, cli.project.UniqueID(), jobID)
	if err != nil {
		return errors.Trace(err)
	}

	cli.metaCli = metaCli
	return nil
}

// CreateJobViaOpenAPI wraps OpenAPI to create a job
func CreateJobViaOpenAPI(
	ctx context.Context, apiEndpoint string, tenantID string, projectID string,
	tp engineModel.JobType, cfg string,
) (string, error) {
	cli, err := httputil.NewClient(nil)
	if err != nil {
		return "", err
	}
	data := url.Values{
		"job_type":   {strconv.Itoa(int(tp))},
		"job_config": {cfg},
		"tenant_id":  {tenantID},
		"project_id": {projectID},
	}
	apiURL := "http://" + apiEndpoint + "/api/v1/jobs"
	resp, err := cli.PostForm(ctx, apiURL, data)
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var jobID string
	err = json.Unmarshal(body, &jobID)
	return jobID, err
}
