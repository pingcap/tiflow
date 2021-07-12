package tso

import (
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	"os"
)

func NewCmdQueryTso(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Get tso from PD",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}
			ts, logic, err := pdClient.GetTS(ctx)
			if err != nil {
				return err
			}
			cmd.Println(oracle.ComposeTS(ts, logic))
			return nil
		},
	}
	command.SetOut(os.Stdout)
	command.SetErr(os.Stdout)

	return command
}
