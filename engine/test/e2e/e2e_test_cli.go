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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/fake"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster"
	server "github.com/pingcap/tiflow/engine/servermaster"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	// set the debug level log for easy test
	log.SetLevel(zapcore.DebugLevel)
}

var ErrLeaderNotFound = errors.New("leader not found")

// ChaosCli is used to interact with server master, fake job and provides ways
// to adding chaos in e2e test.
type ChaosCli struct {
	jobManagerCli pb.JobManagerClient
	masterAddrs   []string
	// masterCli is used to operate with server master, such as submit job
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

// NewJobManagerClient returns a pb.JobManagerClient that can be used for tests.
// TODO remove this function. It is only for temporary use before an OpenAPI client is ready.
func NewJobManagerClient(endpoint string) (pb.JobManagerClient, error) {
	endpoint = strings.TrimLeft(endpoint, "http://")
	conn, err := grpc.Dial(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return pb.NewJobManagerClient(conn), nil
}

// NewUTCli creates a new ChaosCli instance
func NewUTCli(ctx context.Context, masterAddrs, businessMetaAddrs []string, project tenant.ProjectInfo,
	cfg *FakeJobConfig,
) (*ChaosCli, error) {
	if len(masterAddrs) == 0 {
		panic("length of masterAddrs is 0")
	}

	jobManagerCli, err := NewJobManagerClient(masterAddrs[0])
	if err != nil {
		return nil, err
	}

	// TODO: NEED to move metastore config to a toml, and parse the toml
	defaultSchema := "test"

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
		jobManagerCli: jobManagerCli,
		masterAddrs:   masterAddrs,
		clientConn:    cc,
		fakeJobCli:    fakeJobCli,
		fakeJobCfg:    cfg,
		project:       project,
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
	resp, err := cli.jobManagerCli.PauseJob(ctx, req)
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
	resp, err := QueryJobViaOpenAPI(ctx, cli.masterAddrs[0],
		cli.project.TenantID(), cli.project.ProjectID(), jobID)
	if err != nil {
		return false, err
	}
	return resp.Status == int32(expectedStatus), nil
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

// GetLeaderAddr gets the address of the leader of the server master.
func (cli *ChaosCli) GetLeaderAddr(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/v1/leader", cli.masterAddrs[0]), nil)
	if err != nil {
		return "", errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrLeaderNotFound
	}
	if resp.StatusCode/100 != 2 {
		return "", errors.Errorf("unexpected status code %d", resp.StatusCode)
	}
	var leaderInfo struct {
		AdvertiseAddr string `json:"advertise_addr"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&leaderInfo); err != nil {
		return "", errors.Trace(err)
	}
	return leaderInfo.AdvertiseAddr, nil
}

// ResignLeader resigns the leader at the given addr.
func (cli *ChaosCli) ResignLeader(ctx context.Context, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/v1/leader/resign", addr), nil)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return errors.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}

// CreateJobViaOpenAPI wraps OpenAPI to create a job
func CreateJobViaOpenAPI(
	ctx context.Context, apiEndpoint string, tenantID string, projectID string,
	tp engineModel.JobType, cfg string,
) (string, error) {
	data := &servermaster.APICreateJobRequest{
		JobType:   int32(tp),
		JobConfig: cfg,
	}
	postData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/jobs?tenant_id=%s&project_id=%s",
			apiEndpoint, tenantID, projectID,
		),
		"application/json",
		bytes.NewReader(postData),
	)
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

// QueryJobViaOpenAPI wraps OpenAPI to query a job
func QueryJobViaOpenAPI(
	ctx context.Context, apiEndpoint string, tenantID, projectID, jobID string,
) (result *servermaster.APIQueryJobResponse, err error) {
	url := fmt.Sprintf(
		"http://%s/api/v1/jobs/%s?tenant_id=%s&project_id=%s",
		apiEndpoint, jobID, tenantID, projectID,
	)
	resp, err := http.Get(url) // #nosec G107
	if err != nil {
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	result = &servermaster.APIQueryJobResponse{}
	err = json.Unmarshal(body, result)
	return
}
