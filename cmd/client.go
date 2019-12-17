package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/google/uuid"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cliCmd)

	cliCmd.Flags().StringVar(&pdAddress, "pd-addr", "localhost:2379", "address of PD")
	cliCmd.Flags().Uint64Var(&startTs, "start-ts", 0, "start ts of changefeed")
	cliCmd.Flags().Uint64Var(&startTs, "target-ts", 0, "target ts of changefeed")
	cliCmd.Flags().StringVar(&sinkURI, "sink-uri", "root@tcp(127.0.0.1:3306)/test", "sink uri")
}

var (
	pdAddress string
	startTs   uint64
	targetTs  uint64
	sinkURI   string
)

var cliCmd = &cobra.Command{
	Use:   "cli",
	Short: "simulate client to create changefeed",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{pdAddress},
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBackoffMaxDelay(time.Second * 3),
			},
		})
		if err != nil {
			return err
		}
		pdCli, err := pd.NewClient([]string{pdAddress}, pd.SecurityOption{})
		if err != nil {
			return err
		}
		id := uuid.New().String()
		if startTs == 0 {
			ts, logical, err := pdCli.GetTS(context.Background())
			if err != nil {
				return err
			}
			startTs = oracle.ComposeTS(ts, logical)
		}
		detail := &model.ChangeFeedDetail{
			SinkURI:    sinkURI,
			Opts:       make(map[string]string),
			CreateTime: time.Now(),
			StartTs:    startTs,
			TargetTs:   targetTs,
		}
		fmt.Printf("create changefeed ID: %s detail %+v\n", id, detail)
		return kv.SaveChangeFeedDetail(context.Background(), cli, detail, id)
	},
}
