package ctl

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRunFake() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit-job",
		Short: "Run a fake workload to a specific executor",
		RunE:  runFakeFunc,
	}
	cmd.Flags().StringP("executor-addr", "", "", "the targeted executor address")
	cmd.Flags().StringP("executor-id", "", "", "the targeted executor id")
	cmd.Flags().StringP("job-config", "", "", "config file for the demo job")
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

func runFakeFunc(cmd *cobra.Command, _ []string) error {
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
		Tp:     pb.JobType_CVSDemo, // TODO: Support different job types.
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
