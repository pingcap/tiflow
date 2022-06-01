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

package ctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

func newQueryJob() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-job",
		Short: "query job info",
		RunE:  runQueryJob,
	}
	cmd.Flags().String("job-id", "", "the targeted job id")
	cmd.Flags().String("tenant-id", "", "the tenant id")
	cmd.Flags().String("project-id", "", "the project id")
	return cmd
}

func getProjectInfo(cmd *cobra.Command) tenant.ProjectInfo {
	tenantID, err := cmd.Flags().GetString("tenant-id")
	if err != nil {
		log.L().Error("error in parse `--tenant-id`, use default tenant id", zap.Error(err))
		tenantID = tenant.DefaultUserProjectInfo.TenantID()
	} else if tenantID == "" {
		log.L().Warn("tenant-id is empty, use default tenant id")
		tenantID = tenant.DefaultUserProjectInfo.TenantID()
	}

	projectID, err := cmd.Flags().GetString("project-id")
	if err != nil {
		log.L().Error("error in parse `--project-id`, use default project id", zap.Error(err))
		projectID = tenant.DefaultUserProjectInfo.ProjectID()
	} else if projectID == "" {
		log.L().Warn("project-id is empty, use default project id")
		projectID = tenant.DefaultUserProjectInfo.ProjectID()
	}

	return tenant.NewProjectInfo(tenantID, projectID)
}

func runQueryJob(cmd *cobra.Command, _ []string) error {
	id, err := cmd.Flags().GetString("job-id")
	if err != nil {
		log.L().Error("error in parse `--job-id`")
		return err
	}
	if id == "" {
		log.L().Error("job-id should not be empty")
		return err
	}

	project := getProjectInfo(cmd)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := cltManager.MasterClient().QueryJob(ctx, &pb.QueryJobRequest{
		JobId: id,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  project.TenantID(),
			ProjectId: project.ProjectID(),
		},
	})
	if err != nil {
		log.L().Error("failed to query job", zap.Error(err))
		os.Exit(1)
	}
	switch resp.Tp {
	case int64(lib.CvsJobMaster):
		if resp.Status == pb.QueryJobResponse_online && resp.JobMasterInfo != nil {
			statusBytes := resp.JobMasterInfo.Status
			status := &libModel.WorkerStatus{}
			err = json.Unmarshal(statusBytes, status)
			if err != nil {
				log.L().Error("failed to query job", zap.Error(err))
				os.Exit(1)
			}
			ext, err := strconv.ParseInt(string(status.ExtBytes), 10, 64)
			if err != nil {
				log.L().Error("failed to query job", zap.Error(err))
				os.Exit(1)
			}
			log.L().Info("status ext info", zap.Int64("ext", ext))
		}
	default:
	}
	log.L().Info("query result", zap.String("resp", resp.String()))
	return nil
}

func newSubmitJob() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit-job",
		Short: "submit job to master",
		RunE:  runSubmitJob,
	}
	cmd.Flags().String("executor-addr", "", "the targeted executor address")
	cmd.Flags().String("executor-id", "", "the targeted executor id")
	cmd.Flags().String("job-type", "", "job type")
	cmd.Flags().String("job-config", "", "config file for the demo job")
	cmd.Flags().String("tenant-id", "", "the tenant id")
	cmd.Flags().String("project-id", "", "the project id")
	return cmd
}

func openFileAndReadString(path string) (content []byte, err error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return ioutil.ReadAll(fp)
}

func validJobType(job string) (pb.JobType, error) {
	tp, ok := pb.JobType_value[job]
	if !ok {
		// TODO: print valid job types
		return 0, errors.ErrInvalidJobType.GenWithStackByArgs(job)
	}
	return pb.JobType(tp), nil
}

func runSubmitJob(cmd *cobra.Command, _ []string) error {
	tp, err := cmd.Flags().GetString("job-type")
	if err != nil {
		fmt.Print("error in parse `--job-type`")
		return err
	}
	jobType, err := validJobType(tp)
	if err != nil {
		return err
	}
	path, err := cmd.Flags().GetString("job-config")
	if err != nil {
		fmt.Print("error in parse `--job-config`")
		return err
	}
	jobConfig, err := openFileAndReadString(path)
	if err != nil {
		fmt.Print("error in parse job-config")
		return err
	}

	project := getProjectInfo(cmd)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := cltManager.MasterClient().SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     jobType,
		Config: jobConfig,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  project.TenantID(),
			ProjectId: project.ProjectID(),
		},
	})
	if err != nil {
		log.L().Error("failed to submit job", zap.Error(err))
		os.Exit(1)
	}
	log.L().Info("resp", zap.Any("resp", resp))
	return nil
}

func newPauseJob() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-job",
		Short: "pause job",
		RunE:  runPauseJob,
	}
	cmd.Flags().String("job-id", "", "the targeted job id")
	cmd.Flags().String("tenant-id", "", "the tenant id")
	cmd.Flags().String("project-id", "", "the project id")
	return cmd
}

func runPauseJob(cmd *cobra.Command, _ []string) error {
	id, err := cmd.Flags().GetString("job-id")
	if err != nil {
		log.L().Error("error in parse `--job-id`")
		return err
	}
	if id == "" {
		log.L().Error("job-id should not be empty")
		return err
	}
	project := getProjectInfo(cmd)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := cltManager.MasterClient().PauseJob(ctx, &pb.PauseJobRequest{
		JobIdStr: id,
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  project.TenantID(),
			ProjectId: project.ProjectID(),
		},
	})
	if err != nil {
		log.L().Error("failed to query job", zap.Error(err))
		os.Exit(1)
	}
	log.L().Info("pause result", zap.String("err", resp.Err.String()))
	return nil
}
