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

package broker

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/bucket"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	fileExists    = true
	fileNotExists = false

	persisted    = true
	notPersisted = false
)

var (
	fakeProjectInfo = tenant.NewProjectInfo("fakeTenant", "fakeProject")
	fakeJobID       = "fakeJob"
)

func newBrokerForS3(
	t *testing.T, executorID resModel.ExecutorID,
) (*DefaultBroker, *manager.MockClient, string, brStorage.ExternalStorage, string) {
	s3Prefix := fmt.Sprintf("%s-%s-%d", t.Name(),
		time.Now().Format("20060102-150405"), rand.Int())
	return newBrokerForS3WithPrefix(t, executorID, s3Prefix)
}

func newBrokerForS3WithPrefix(
	t *testing.T, executorID resModel.ExecutorID, s3Prefix string,
) (*DefaultBroker, *manager.MockClient, string, brStorage.ExternalStorage, string) {
	// Remove the following comments if running tests locally.
	// os.Setenv("ENGINE_S3_ENDPOINT", "http://127.0.0.1:9000/")
	// os.Setenv("ENGINE_S3_ACCESS_KEY", "engine")
	// os.Setenv("ENGINE_S3_SECRET_KEY", "engineSecret")

	log.Warn("s3 prefix", zap.String("s3prefix", s3Prefix))
	tmpDir := t.TempDir()
	cli := manager.NewMockClient()
	s3Cfg, err := bucket.GetS3OptionsForUT()
	if err != nil {
		// skip integration tests in ut
		t.Skipf("server not configured for s3 integration: %s", err.Error())
	}

	// request during persisting dummy resource
	cli.On("CreateResource", mock.Anything,
		&pb.CreateResourceRequest{
			ProjectInfo:     &pb.ProjectInfo{},
			ResourceId:      bucket.DummyResourceID,
			CreatorExecutor: string(executorID),
			JobId:           bucket.GetDummyJobID(executorID),
			CreatorWorkerId: bucket.DummyWorkerID,
		}, mock.Anything).Return(nil)

	broker, err := NewBrokerWithConfig(&resModel.Config{
		Local: resModel.LocalFileConfig{BaseDir: tmpDir},
		S3: resModel.S3Config{
			S3BackendOptions: *s3Cfg,
			Bucket:           bucket.UtBucketName,
			Prefix:           s3Prefix,
		},
	}, executorID, cli)
	require.NoError(t, err)
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	rootURI := fmt.Sprintf("s3://%s/%s/%s", url.QueryEscape(bucket.UtBucketName),
		s3Prefix, url.QueryEscape(string(executorID)))
	rootStrorage, err := bucket.GetExternalStorageFromURI(context.Background(), rootURI, &brStorage.BackendOptions{S3: *s3Cfg})
	require.NoError(t, err)

	// check dummy resource exists
	checkFile(t, rootStrorage, bucket.GetDummyResPath(".keep"), fileExists)
	return broker, cli, tmpDir, rootStrorage, s3Prefix
}

func closeBrokerForS3(
	t *testing.T, broker *DefaultBroker,
	cli *manager.MockClient, rootStrorage brStorage.ExternalStorage,
) {
	// request during remove dummy resource
	cli.On("RemoveResource", mock.Anything,
		&pb.RemoveResourceRequest{
			ResourceKey: &pb.ResourceKey{
				JobId:      bucket.GetDummyJobID(broker.executorID),
				ResourceId: bucket.DummyResourceID,
			},
		}, mock.Anything).Return(nil)
	broker.Close()
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	// check dummy resource removed
	checkFile(t, rootStrorage, "keep-alive-worker/dummy/.keep", fileNotExists)
}

func checkFile(t *testing.T, storage brStorage.ExternalStorage, path string, expected bool) {
	exist, err := storage.FileExists(context.Background(), path)
	require.NoError(t, err, "check file %s error", path)
	require.Equal(t, expected, exist, "file %s should %s", path, func() string {
		if expected {
			return "exist"
		}
		return "not exist"
	}())
}

func checkS3ResourceForWorker(
	t *testing.T, storage brStorage.ExternalStorage,
	creator resModel.WorkerID, resID resModel.ResourceID,
	expected bool, testFiles ...string,
) {
	tp, resName, err := resModel.ParseResourceID(resID)
	require.NoError(t, err)
	require.Equal(t, resModel.ResourceTypeS3, tp)

	resPath := fmt.Sprintf("%s/%s", creator, resName)
	// check .keep file
	checkFile(t, storage, fmt.Sprintf("%s/.keep", resPath), expected)
	for _, fileName := range testFiles {
		checkFile(t, storage, fmt.Sprintf("%s/%s", resPath, fileName), expected)
	}
}

func createS3ResourceForWorker(
	t *testing.T, brk *DefaultBroker, cli *manager.MockClient,
	rootStrorage brStorage.ExternalStorage, creator resModel.WorkerID, resID resModel.ResourceID,
	isPersisted bool, newTestFiles ...string,
) {
	tp, resName, err := resModel.ParseResourceID(resID)
	require.NoError(t, err)
	require.Equal(t, resModel.ResourceTypeS3, tp)

	// create resource `resID` for worker `creator`
	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: fakeJobID, ResourceId: resID}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), errors.ErrResourceDoesNotExist.GenWithStackByArgs(resID))
	hdl, err := brk.OpenStorage(context.Background(), fakeProjectInfo, creator, fakeJobID, resID)
	require.NoError(t, err)
	require.Equal(t, resID, hdl.ID())
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	keepFilePath := fmt.Sprintf("%s/%s/.keep", creator, resName)
	checkFile(t, rootStrorage, keepFilePath, fileExists)

	if newTestFiles != nil {
		resStorage := hdl.BrExternalStorage()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for index, fileName := range newTestFiles {
			filePath := fmt.Sprintf("%s/%s/%s", creator, resName, fileName)
			f, err := resStorage.Create(ctx, fileName)
			require.NoError(t, err)
			content := filePath + fmt.Sprintf("_index-%d", index)
			// FIXME(CharlesCheung): If nothing is written, f.Close will report an error.
			// Figure out if this is a bug. (Minio or BR or Engine)
			_, err = f.Write(ctx, []byte(content))
			require.NoError(t, err)
			err = f.Close(ctx)
			require.NoError(t, err)
			checkFile(t, rootStrorage, filePath, fileExists)
		}
	}

	if isPersisted {
		cli.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
			ProjectInfo: &pb.ProjectInfo{
				TenantId:  fakeProjectInfo.TenantID(),
				ProjectId: fakeProjectInfo.ProjectID(),
			},
			ResourceId:      resID,
			CreatorExecutor: bucket.MockExecutorID,
			JobId:           fakeJobID,
			CreatorWorkerId: creator,
		}, mock.Anything).Return(nil)
		err = hdl.Persist(context.Background())
		require.NoError(t, err)
		cli.AssertExpectations(t)
	}
}

//nolint:unparam
func getExistingS3ResourceForWorker(
	t *testing.T, brk *DefaultBroker, cli *manager.MockClient,
	creator, user resModel.WorkerID, persistedRes resModel.ResourceID,
) Handle {
	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: fakeJobID, ResourceId: persistedRes}}, mock.Anything).
		Return(&pb.QueryResourceResponse{
			CreatorExecutor: bucket.MockExecutorID,
			JobId:           fakeJobID,
			CreatorWorkerId: creator,
		}, nil)
	hdl, err := brk.OpenStorage(context.Background(), fakeProjectInfo, user, fakeJobID, persistedRes)
	require.NoError(t, err)
	require.Equal(t, persistedRes, hdl.ID())
	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil
	return hdl
}

func TestIntegrationBrokerOpenNewS3Storage(t *testing.T) {
	t.Parallel()
	brk, cli, dir, rootStrorage, _ := newBrokerForS3(t, bucket.MockExecutorID)

	// test local file works well under this condition
	resID := "/local/test-1"
	_, resName, err := resModel.ParseResourceID(resID)
	require.NoError(t, err)

	cli.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: fakeJobID, ResourceId: resID}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), errors.ErrResourceDoesNotExist.GenWithStackByArgs(resID))
	hdl, err := brk.OpenStorage(context.Background(), fakeProjectInfo, "worker-1", fakeJobID, resID)
	require.NoError(t, err)
	require.Equal(t, resID, hdl.ID())

	cli.AssertExpectations(t)
	cli.ExpectedCalls = nil

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	cli.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  fakeProjectInfo.TenantID(),
			ProjectId: fakeProjectInfo.ProjectID(),
		},
		ResourceId:      resID,
		CreatorExecutor: bucket.MockExecutorID,
		JobId:           fakeJobID,
		CreatorWorkerId: "worker-1",
	}, mock.Anything).Return(nil)

	err = hdl.Persist(context.Background())
	require.NoError(t, err)

	cli.AssertExpectations(t)

	local.AssertLocalFileExists(t, dir, "worker-1", resName, "1.txt")

	// test s3
	testFiles := []string{"1.txt", "inner1/2.txt", "inner1/inner2/3.txt"}

	// Worker-1 creates a persistent resource test-1
	createS3ResourceForWorker(t, brk, cli, rootStrorage,
		"worker-1", "/s3/test-1", persisted, testFiles...)
	// Worker-1 creates a temporary resource test-2
	createS3ResourceForWorker(t, brk, cli, rootStrorage,
		"worker-1", "/s3/test-2", notPersisted, testFiles...)

	closeBrokerForS3(t, brk, cli, rootStrorage)
	// persistent resource test-1 should not removed after broker closed.
	checkS3ResourceForWorker(t, rootStrorage, "worker-1", "/s3/test-1", fileExists, testFiles...)
	// temporary resource test-2 should be removed after broker closed.
	checkS3ResourceForWorker(t, rootStrorage, "worker-1", "/s3/test-2", fileNotExists, testFiles...)
}

func TestIntegrationBrokerOpenExistingS3Storage(t *testing.T) {
	t.Parallel()
	brk, cli, _, rootStrorage, s3Prefix := newBrokerForS3(t, bucket.MockExecutorID)

	testFiles := []string{"1.txt", "inner1/2.txt", "inner1/inner2/3.txt"}

	persistedRes := "/s3/test-1"
	// Worker-1 creates a persistent resource test-1
	createS3ResourceForWorker(t, brk, cli, rootStrorage,
		"worker-1", persistedRes, persisted, testFiles...)
	// Worker-1 creates a temporary resource test-2
	createS3ResourceForWorker(t, brk, cli, rootStrorage,
		"worker-1", "/s3/test-2", notPersisted, testFiles...)

	// Scenario 1: creator `worker-1` opens the persistent resource `test-1` again
	getExistingS3ResourceForWorker(t, brk, cli, "worker-1", "worker-1", persistedRes)

	// Scenario 2: other worker in current executor opens the persistent resource `test-1`
	getExistingS3ResourceForWorker(t, brk, cli, "worker-1", "worker-2", persistedRes)

	// Scenario 3: worker in other executor opens the persistent resource `test-1`
	brk1, cli1, _, rootStrorage1, _ := newBrokerForS3WithPrefix(t, "executor-test-persisted", s3Prefix)
	defer closeBrokerForS3(t, brk1, cli1, rootStrorage1)
	getExistingS3ResourceForWorker(t, brk1, cli1, "worker-1", "worker-1", persistedRes)
	getExistingS3ResourceForWorker(t, brk1, cli1, "worker-1", "worker-3", persistedRes)

	closeBrokerForS3(t, brk, cli, rootStrorage)
	// persistent resource test-1 should not removed after broker closed.
	checkS3ResourceForWorker(t, rootStrorage, "worker-1", persistedRes, fileExists, testFiles...)
	// temporary resource test-2 should be removed after broker closed.
	checkS3ResourceForWorker(t, rootStrorage, "worker-1", "/s3/test-2", fileNotExists, testFiles...)

	// Scenario 4: worker in other executor opens the persistent resource `test-1` after executor exits
	getExistingS3ResourceForWorker(t, brk1, cli1, "worker-1", "worker-1", persistedRes)
	hdl := getExistingS3ResourceForWorker(t, brk1, cli1, "worker-1", "worker-3", persistedRes)

	err := hdl.Persist(context.Background())
	require.NoError(t, err)

	cli1.On("RemoveResource", mock.Anything,
		&pb.RemoveResourceRequest{
			ResourceKey: &pb.ResourceKey{
				JobId:      fakeJobID,
				ResourceId: persistedRes,
			},
		}, mock.Anything).Return(nil)
	err = hdl.Discard(context.Background())
	require.NoError(t, err)

	checkS3ResourceForWorker(t, rootStrorage, "worker-1", persistedRes, fileNotExists, testFiles...)
}

func TestIntegrationBrokerGCClosedWorker(t *testing.T) {
	t.Parallel()
	brk, cli, _, rootStrorage, _ := newBrokerForS3(t, bucket.MockExecutorID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testFiles := []string{"1.txt", "inner1/2.txt", "inner1/inner2/3.txt"}

	rand.Seed(time.Now().UnixNano())
	workerCount := 10
	maxResNumPerWorker := 10
	maxWorkerRunDuration := 10 * time.Second

	type Res struct {
		workerID string
		gc       bool
		resID    string
		exists   bool
	}
	var toCheckClosedRes []Res
	var toCheckGCRes []Res
	var gcWorkerCnt atomic.Int32
	expectedGCWorkerCnt := int32(0)
	wg := sync.WaitGroup{}

	for worker := 0; worker < workerCount; worker++ {
		gc := rand.Intn(2) == 0
		workerID := fmt.Sprintf("worker-%d", worker)

		resCount := rand.Intn(maxResNumPerWorker) + 1
		persistedResCount := 0
		for res := 0; res < resCount; res++ {
			resID := fmt.Sprintf("/s3/test-%d", res)
			isPersisted := rand.Intn(2) == 0
			if isPersisted {
				persistedResCount++
			}
			createS3ResourceForWorker(t, brk, cli, rootStrorage,
				workerID, resID, isPersisted, testFiles...)
			toCheckRes := Res{
				workerID: workerID,
				gc:       gc,
				resID:    resID,
				exists:   isPersisted,
			}
			if gc {
				toCheckGCRes = append(toCheckGCRes, toCheckRes)
			} else {
				toCheckClosedRes = append(toCheckClosedRes, toCheckRes)
			}
		}

		if gc {
			if persistedResCount == 0 {
				log.Warn("worker has no persisted resource", zap.String("worker", workerID))
			}
			wg.Add(1)
			expectedGCWorkerCnt++
			go func(curWorkerID resModel.WorkerID) {
				defer wg.Done()
				runningDuration := rand.Intn(int(maxWorkerRunDuration))
				timer := time.NewTimer(time.Duration(runningDuration))
				select {
				case <-ctx.Done():
					t.Error("context canceled")
				case <-timer.C:
					brk.OnWorkerClosed(ctx, curWorkerID, fakeJobID)
					gcWorkerCnt.Add(1)
					log.Info("worker closed", zap.String("workerID", curWorkerID))
				}
			}(workerID)
		}
	}
	wg.Wait()
	require.Equal(t, expectedGCWorkerCnt, gcWorkerCnt.Load())

	// wait gc done
	require.Eventually(t, func() bool {
		return len(brk.closedWorkerCh) == 0
	}, 60*time.Second, 300*time.Millisecond)

	// check closed worker resources
	for _, res := range toCheckGCRes {
		checkS3ResourceForWorker(t, rootStrorage, res.workerID, res.resID, res.exists, testFiles...)
	}
	for _, res := range toCheckClosedRes {
		checkS3ResourceForWorker(t, rootStrorage, res.workerID, res.resID, fileExists, testFiles...)
	}

	closeBrokerForS3(t, brk, cli, rootStrorage)
	for _, res := range toCheckClosedRes {
		checkS3ResourceForWorker(t, rootStrorage, res.workerID, res.resID, res.exists, testFiles...)
	}
}
