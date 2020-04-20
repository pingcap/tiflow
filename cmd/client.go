package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"

	"github.com/pingcap/ticdc/cdc/entry"

	"github.com/BurntSushi/toml"
	"github.com/chzyer/readline"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/google/uuid"
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

func init() {
	cliCmd := newCliCommand()
	cliCmd.PersistentFlags().StringVar(&cliPdAddr, "pd", "http://127.0.0.1:2379", "PD address")
	cliCmd.PersistentFlags().BoolVarP(&interact, "interact", "i", false, "Run cdc cli with readline")
	rootCmd.AddCommand(cliCmd)
}

var (
	opts       []string
	startTs    uint64
	targetTs   uint64
	sinkURI    string
	configFile string
	cliPdAddr  string
	noConfirm  bool

	cdcEtcdCli kv.CDCEtcdClient
	pdCli      pd.Client

	interact bool
)

func newCliCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			etcdCli, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{cliPdAddr},
				DialTimeout: 5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithConnectParams(grpc.ConnectParams{
						Backoff: backoff.Config{
							BaseDelay:  time.Second,
							Multiplier: 1.1,
							Jitter:     0.1,
							MaxDelay:   3 * time.Second,
						},
						MinConnectTimeout: 3 * time.Second,
					}),
				},
			})
			if err != nil {
				return err
			}
			cdcEtcdCli = kv.NewCDCEtcdClient(etcdCli)
			pdCli, err = pd.NewClient([]string{cliPdAddr}, pd.SecurityOption{})
			if err != nil {
				return err
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if interact {
				loop()
			}
		},
	}
	command.AddCommand(
		newCaptureCommand(),
		newChangefeedCommand(),
		newProcessorCommand(),
		newMetadataCommand(),
		newTsoCommand(),
	)

	return command
}

func newCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "capture",
		Short: "Manage capture (capture is a CDC server instance)",
	}
	command.AddCommand(
		newListCaptureCommand(),
		// TODO: add resign owner command
	)
	return command
}

func newChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}
	command.AddCommand(
		newListChangefeedCommand(),
		newQueryChangefeedCommand(),
		newCreateChangefeedCommand(),
		newStatisticsChangefeedCommand(),
		// TODO: add stop, resume, delete changefeed
	)
	return command
}

func newProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "processor",
		Short: "Manage processor (processor is a sub replication task running on a specified capture)",
	}
	command.AddCommand(
		newListProcessorCommand(),
		newQueryProcessorCommand(),
	)
	return command
}

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

func newTsoCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "tso",
		Short: "Manage tso",
	}
	command.AddCommand(
		newQueryTsoCommand(),
	)
	return command
}

func newCreateChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			id := uuid.New().String()
			if startTs == 0 {
				ts, logical, err := pdCli.GetTS(ctx)
				if err != nil {
					return err
				}
				startTs = oracle.ComposeTS(ts, logical)
			}
			if err := verifyStartTs(ctx, startTs, cdcEtcdCli); err != nil {
				return err
			}

			cfg := new(util.ReplicaConfig)
			if len(configFile) > 0 {
				if err := strictDecodeFile(configFile, "cdc", cfg); err != nil {
					return err
				}
			}

			info := &model.ChangeFeedInfo{
				SinkURI:    sinkURI,
				Opts:       make(map[string]string),
				CreateTime: time.Now(),
				StartTs:    startTs,
				TargetTs:   targetTs,
				Config:     cfg,
			}

			ineligibleTables, err := verifyTables(ctx, cfg)
			if err != nil {
				return err
			}
			if len(ineligibleTables) != 0 {
				cmd.Printf("[WARN] some tables are not eligible to replicate, %#v\n", ineligibleTables)
				if !noConfirm {
					cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n")
					var yOrN string
					_, err := fmt.Scan(&yOrN)
					if err != nil {
						return err
					}
					if strings.TrimSpace(yOrN) != "Y" {
						log.S().Fatal("Failed to create changefeed\n")
					}
				}
			}

			for _, opt := range opts {
				s := strings.SplitN(opt, "=", 2)
				if len(s) <= 0 {
					cmd.Printf("omit opt: %s", opt)
					continue
				}

				var key string
				var value string

				key = s[0]
				if len(s) > 1 {
					value = s[1]
				}
				info.Opts[key] = value
			}

			d, err := info.Marshal()
			if err != nil {
				return err
			}
			cmd.Printf("create changefeed ID: %s info %s\n", id, d)
			return cdcEtcdCli.SaveChangeFeedInfo(ctx, info, id)
		},
	}
	command.PersistentFlags().Uint64Var(&startTs, "start-ts", 0, "Start ts of changefeed")
	command.PersistentFlags().Uint64Var(&targetTs, "target-ts", 0, "Target ts of changefeed")
	command.PersistentFlags().StringVar(&sinkURI, "sink-uri", "mysql://root:123456@127.0.0.1:3306/", "sink uri")
	command.PersistentFlags().StringVar(&configFile, "config", "", "Path of the configuration file")
	command.PersistentFlags().StringSliceVar(&opts, "opts", nil, "Extra options, in the `key=value` format")
	command.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")

	return command
}

func verifyStartTs(ctx context.Context, startTs uint64, cli kv.CDCEtcdClient) error {
	resp, err := cli.Client.Get(ctx, tikv.GcSavedSafePoint)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Count == 0 {
		return nil
	}
	safePoint, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	if startTs < safePoint {
		return errors.Errorf("startTs %d less than gcSafePoint %d", startTs, safePoint)
	}
	return nil
}

func verifyTables(ctx context.Context, cfg *util.ReplicaConfig) (ineligibleTables []entry.TableName, err error) {
	kvStore, err := kv.CreateTiStore(cliPdAddr)
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, errors.Trace(err)
	}

	filter, err := util.NewFilter(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemaStorage, err := entry.NewSchemaStorage(jobs, filter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage.FlushIneligibleTables()

	snap := schemaStorage.GetLastSnapshot()
	for tID, tableName := range snap.CloneTables() {
		tableInfo, exist := snap.TableByID(int64(tID))
		if !exist {
			return nil, errors.NotFoundf("table %d", int64(tID))
		}
		if filter.ShouldIgnoreTable(tableName.Schema, tableName.Table) {
			continue
		}
		if !tableInfo.ExistTableUniqueColumn() {
			ineligibleTables = append(ineligibleTables, tableName)
		}
	}
	return
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

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args, err := shellwords.Parse(line)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}

		command := newCliCommand()
		command.SetArgs(args)
		_ = command.ParseFlags(args)
		command.SetOutput(os.Stdout)
		if err = command.Execute(); err != nil {
			command.Println(err)
		}
	}
}
