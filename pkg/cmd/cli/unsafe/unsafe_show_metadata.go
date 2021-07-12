package unsafe

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdShowMetadata(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "show-metadata",
		Short: "Show metadata stored in PD",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			kvs, err := etcdClient.GetAllCDCInfo(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			for _, kv := range kvs {
				cmd.Printf("Key: %s, Value: %s\n", string(kv.Key), string(kv.Value))
			}
			cmd.Printf("Show %d KVs\n", len(kvs))

			return nil
		},
	}

	return command
}
