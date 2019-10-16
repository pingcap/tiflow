package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb-cdc/cdc"
	"github.com/pingcap/tidb-cdc/cdc/util"
)

func init() {
	rootCmd.AddCommand(pullCmd)

	pullCmd.Flags().StringVar(&pdAddr, "pd-addr", "localhost:2379", "address of PD")
}

var pdAddr string

var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "pull kv change and print out",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cli, err := pd.NewClient(strings.Split(pdAddr, ","), pd.SecurityOption{})
		if err != nil {
			fmt.Println(err)
			return
		}

		ts := oracle.ComposeTS(time.Now().Unix()*1000, 0)
		detail := cdc.ChangeFeedDetail{}

		p := cdc.NewPuller(cli, ts, []util.Span{{nil, nil}}, detail)

		g, ctx := errgroup.WithContext(context.Background())

		g.Go(func() error {
			return p.Run(ctx)
		})

		g.Go(func() error {
			return p.CollectRawTxns(ctx, func(ctx context.Context, txn cdc.RawTxn) error {
				fmt.Printf("%+v\n", txn)
				return nil
			})
		})

		err = g.Wait()

		if err != nil {
			fmt.Println(err)
		}
	},
}
