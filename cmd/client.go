package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/clientv3"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/google/uuid"
	"github.com/pingcap/errors"
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
	cliCmd.Flags().StringVar(&configFile, "config", "", "path of the configuration file")
}

var (
	pdAddress  string
	startTs    uint64
	targetTs   uint64
	sinkURI    string
	configFile string
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

		cfg := new(model.ReplicaConfig)
		if len(configFile) > 0 {
			if err := strictDecodeFile(configFile, "cdc", cfg); err != nil {
				return err
			}
		}

		detail := &model.ChangeFeedDetail{
			SinkURI:    sinkURI,
			Opts:       make(map[string]string),
			CreateTime: time.Now(),
			StartTs:    startTs,
			TargetTs:   targetTs,
			Config:     cfg,
		}
		d, err := detail.Marshal()
		if err != nil {
			return err
		}
		fmt.Printf("create changefeed ID: %s detail %s\n", id, d)
		return kv.SaveChangeFeedDetail(context.Background(), cli, detail, id)
	},
}

// strictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func strictDecodeFile(path, component string, cfg interface{}) error {
	metaData, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if undecoded := metaData.Undecoded(); len(undecoded) > 0 {
		var b strings.Builder
		for i, item := range undecoded {
			if i != 0 {
				b.WriteString(", ")
			}
			b.WriteString(item.String())
		}
		err = errors.Errorf("component %s's config file %s contained unknown configuration options: %s",
			component, path, b.String())
	}

	return errors.Trace(err)
}
