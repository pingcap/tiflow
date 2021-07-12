package unsafe

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdDeleteServiceGcSafepoint(f util.Factory, commonOptions *CommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "delete-service-gc-safepoint",
		Short: "Delete CDC service GC safepoint in PD, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmMetaDelete(cmd, commonOptions.noConfirm); err != nil {
				return err
			}
			ctx := context.GetDefaultContext()
			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}
			_, err = pdClient.UpdateServiceGCSafePoint(ctx, cdc.CDCServiceSafePointID, 0, 0)
			if err == nil {
				cmd.Println("CDC service GC safepoint truncated in PD!")
			}
			return errors.Trace(err)
		},
	}

	return command
}
