package changefeed

import (
	"context"
	"encoding/json"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/model"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdResumeChangefeed(f util.Factory, commonOptions *CommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "resume",
		Short: "Resume a paused replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
				Type: model.AdminResume,
			}
			if err := resumeChangefeedCheck(f, ctx, cmd, commonOptions); err != nil {
				return err
			}
			return ApplyAdminChangefeed(f, ctx, job, f.GetCredential())
		},
	}

	return command
}

func resumeChangefeedCheck(f util.Factory, ctx context.Context, cmd *cobra.Command, commonOptions *CommonOptions) error {
	resp, err := ApplyOwnerChangefeedQuery(f, ctx, commonOptions.changefeedID, f.GetCredential())
	if err != nil {
		return err
	}
	info := &cdc.ChangefeedResp{}
	err = json.Unmarshal([]byte(resp), info)
	if err != nil {
		return err
	}
	return util.ConfirmLargeDataGap(f, ctx, cmd, commonOptions, info.TSO)
}
