package processor

import (
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdListProcessor(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			info, err := etcdClient.GetProcessors(ctx)
			if err != nil {
				return err
			}
			return util.JsonPrint(cmd, info)
		},
	}

	return command
}
