package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

func newMetadataCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "meta",
		Short: "Manage metadata stored in PD",
	}
	command.AddCommand(
		newDeleteMetaCommand(),
	)
	return command
}

func newDeleteMetaCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "delete",
		Short: "Delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cdcEtcdCli.ClearAllCDCInfo(context.Background())
			if err == nil {
				cmd.Println("already truncate all meta in etcd!")
			}
			return err
		},
	}
	return command
}
