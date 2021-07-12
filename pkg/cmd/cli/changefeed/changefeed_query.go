package changefeed

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type captureTaskStatus struct {
	CaptureID  string            `json:"capture-id"`
	TaskStatus *model.TaskStatus `json:"status"`
}

// cfMeta holds changefeed info and changefeed status
type cfMeta struct {
	Info       *model.ChangeFeedInfo   `json:"info"`
	Status     *model.ChangeFeedStatus `json:"status"`
	Count      uint64                  `json:"count"`
	TaskStatus []captureTaskStatus     `json:"task-status"`
}

type QueryChangefeedOptions struct {
	commonOptions *commonOptions

	simplified bool
}

func NewQueryChangefeedOptions(commonOptions *commonOptions) *QueryChangefeedOptions {
	return &QueryChangefeedOptions{
		simplified:    false,
		commonOptions: commonOptions,
	}
}

func NewCmdQueryChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := NewQueryChangefeedOptions(commonOptions)

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			if o.simplified {
				resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, o.commonOptions.changefeedID, f.GetCredential())
				if err != nil {
					return err
				}
				cmd.Println(resp)
				return nil
			}

			info, err := etcdClient.GetChangeFeedInfo(ctx, o.commonOptions.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			status, _, err := etcdClient.GetChangeFeedStatus(ctx, o.commonOptions.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			if err != nil && cerror.ErrChangeFeedNotExists.Equal(err) {
				log.Error("This changefeed does not exist", zap.String("changefeed", o.commonOptions.changefeedID))
				return err
			}
			taskPositions, err := etcdClient.GetAllTaskPositions(ctx, o.commonOptions.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			var count uint64
			for _, pinfo := range taskPositions {
				count += pinfo.Count
			}
			processorInfos, err := etcdClient.GetAllTaskStatus(ctx, o.commonOptions.changefeedID)
			if err != nil {
				return err
			}
			taskStatus := make([]captureTaskStatus, 0, len(processorInfos))
			for captureID, status := range processorInfos {
				taskStatus = append(taskStatus, captureTaskStatus{CaptureID: captureID, TaskStatus: status})
			}
			meta := &cfMeta{Info: info, Status: status, Count: count, TaskStatus: taskStatus}
			if info == nil {
				log.Warn("This changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.", zap.String("changgefeed", o.commonOptions.changefeedID))
			}
			return util.JsonPrint(cmd, meta)
		},
	}

	command.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
