package unsafe

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"strings"
)

func NewCmdReset(f util.Factory, commonOptions *CommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "reset",
		Short: "Reset the status of the TiCDC cluster, delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmMetaDelete(cmd, commonOptions.noConfirm); err != nil {
				return err
			}
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			leases, err := etcdClient.GetCaptureLeases(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = etcdClient.ClearAllCDCInfo(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = etcdClient.RevokeAllLeases(ctx, leases)
			if err != nil {
				return errors.Trace(err)
			}

			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}

			_, err = pdClient.UpdateServiceGCSafePoint(ctx, cdc.CDCServiceSafePointID, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}

			cmd.Println("reset and all metadata truncated in PD!")

			return nil
		},
	}

	return command
}

func confirmMetaDelete(cmd *cobra.Command, noConfirm bool) error {
	if noConfirm {
		return nil
	}
	cmd.Printf("Confirm that you know what this command will do and use it at your own risk [Y/N]\n")
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		return errors.NewNoStackError("abort meta command")
	}
	return nil
}
