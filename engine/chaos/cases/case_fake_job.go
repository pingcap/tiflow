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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/fake"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/test/e2e"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util"
)

func runFakeJobCase(ctx context.Context, cfg *config) error {
	serverMasterEndpoints := []string{cfg.MasterAddr}
	etcdEndpoints := []string{cfg.EtcdAddr}

	jobCfg := &fake.Config{
		JobName:     "fake-job-case",
		WorkerCount: 8,
		// use a large enough target tick to ensure the fake job long running
		TargetTick:      10000000,
		EtcdWatchEnable: true,
		EtcdEndpoints:   etcdEndpoints,
		EtcdWatchPrefix: "/fake-job/test/",
	}
	e2eCfg := &e2e.FakeJobConfig{
		EtcdEndpoints: etcdEndpoints, // reuse user meta KV endpoints
		WorkerCount:   jobCfg.WorkerCount,
		KeyPrefix:     jobCfg.EtcdWatchPrefix,
	}
	cli, err := e2e.NewUTCli(ctx, serverMasterEndpoints, etcdEndpoints, tenant.DefaultUserProjectInfo, e2eCfg)
	if err != nil {
		return err
	}
	revision, err := cli.GetRevision(ctx)
	if err != nil {
		return err
	}
	jobCfg.EtcdStartRevision = revision
	cfgBytes, err := json.Marshal(jobCfg)
	if err != nil {
		return err
	}

	// retry to create a fake job, since chaos exists, the server master may be
	// unavailable for sometime.
	var jobID string
	err = retry.Do(ctx, func() error {
		var inErr error
		jobID, err = cli.CreateJob(ctx, engineModel.JobTypeFakeJob, cfgBytes)
		return inErr
	},
		retry.WithBackoffBaseDelay(1000 /* 1 second */),
		retry.WithBackoffMaxDelay(8000 /* 8 seconds */),
		retry.WithMaxTries(15 /* fail after 103 seconds */),
	)
	if err != nil {
		return err
	}

	// update upstream etcd, and check fake job works normally every 60 seconds
	// run 10 times, about 10 minutes totally.
	mvcc := 0
	interval := 60 * time.Second
	runTime := 10
	for i := 0; i < runTime; i++ {
		value := fmt.Sprintf("update-value-index-%d", i)
		mvcc++
		start := time.Now()
		err := updateKeyAndCheck(ctx, cli, jobID, jobCfg.WorkerCount, value, mvcc)
		if err != nil {
			return err
		}
		duration := time.Since(start)
		log.L().Info("update key and check test", zap.Int("round", i), zap.Duration("duration", duration))
		if duration < interval {
			time.Sleep(start.Add(interval).Sub(time.Now()))
		}
	}

	log.L().Info("run fake job case successfully")

	return nil
}

func updateKeyAndCheck(
	ctx context.Context, cli *e2e.ChaosCli, jobID string, workerCount int,
	updateValue string, expectedMvcc int,
) error {
	for i := 0; i < workerCount; i++ {
		err := cli.UpdateFakeJobKey(ctx, i, updateValue)
		if err != nil {
			return err
		}
	}
	finished := util.WaitSomething(60, time.Second*5, func() bool {
		for jobIdx := 0; jobIdx < workerCount; jobIdx++ {
			err := cli.CheckFakeJobKey(ctx, jobID, jobIdx, expectedMvcc, updateValue)
			if err != nil {
				log.L().Warn("check fail job failed", zap.Error(err))
				return false
			}
		}
		return true
	})
	if !finished {
		return errors.New("wait fake job normally timeout")
	}
	return nil
}
