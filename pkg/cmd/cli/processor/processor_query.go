package processor

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

type QueryProcessorOptions struct {
	changefeedID string
	captureID    string
}

func NewQueryProcessorOptions() *QueryProcessorOptions {
	return &QueryProcessorOptions{
		changefeedID: "",
		captureID:    "",
	}
}

func NewCmdQueryProcessor(f util.Factory) *cobra.Command {
	o := NewQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			_, status, err := etcdClient.GetTaskStatus(ctx, o.changefeedID, o.captureID)
			if err != nil && cerror.ErrTaskStatusNotExists.Equal(err) {
				return err
			}
			_, position, err := etcdClient.GetTaskPosition(ctx, o.changefeedID, o.captureID)
			if err != nil && cerror.ErrTaskPositionNotExists.Equal(err) {
				return err
			}
			meta := &processorMeta{Status: status, Position: position}
			return util.JsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().StringVarP(&o.captureID, "capture-id", "p", "", "Capture ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	_ = command.MarkPersistentFlagRequired("capture-id")

	return command
}
