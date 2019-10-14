package cmd

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc"
	"github.com/spf13/cobra"
)

var (
	pdEndpoints string

	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "runs capture server",
		Long:  "runs capture server",
		RunE:  runEServer,
	}
)

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&pdEndpoints, "pd-endpoints", "127.0.0.1:2379", "endpoints of PD, separated by comma")
}

func runEServer(cmd *cobra.Command, args []string) error {
	var opts []cdc.ServerOption
	opts = append(opts, cdc.PDEndpoints(pdEndpoints))

	server, err := cdc.NewServer(opts...)
	if err != nil {
		return errors.Annotate(err, "new server")
	}

	err = server.Run()
	if err != nil {
		return errors.Annotate(err, "run server")
	}

	return nil
}
