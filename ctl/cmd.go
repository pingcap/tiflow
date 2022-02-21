package ctl

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewQueryJob() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-job",
		Short: "query job info",
		RunE:  runQueryJob,
	}
	cmd.Flags().String("job-id", "", "the targeted job id")
	return cmd
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
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := cltManager.MasterClient().QueryJob(ctx, &pb.QueryJobRequest{
		JobId: id,
	})
	if err != nil {
		log.L().Error("failed to submit job", zap.Error(err))
		os.Exit(1)
	}
	log.L().Info("query result", zap.String("resp", resp.String()))
	return nil
}

func NewSubmitJob() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit-job",
		Short: "submit job to master",
		RunE:  runSubmitJob,
	}
	cmd.Flags().String("executor-addr", "", "the targeted executor address")
	cmd.Flags().String("executor-id", "", "the targeted executor id")
	cmd.Flags().String("job-type", "", "job type")
	cmd.Flags().String("job-config", "", "config file for the demo job")
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
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := cltManager.MasterClient().SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     jobType,
		Config: jobConfig,
		User:   "hanfei",
	})
	if err != nil {
		log.L().Error("failed to submit job", zap.Error(err))
		os.Exit(1)
	}
	log.L().Info("resp", zap.Any("resp", resp))
	return nil
}
