// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chzyer/readline"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/mattn/go-shellwords"
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
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
	sortEngine string
	sortDir    string

	cyclicReplicaID        uint64
	cyclicFilterReplicaIDs []uint
	cyclicSyncDDL          bool
	cyclicUpstreamDSN      string

	cdcEtcdCli kv.CDCEtcdClient
	pdCli      pd.Client

	interact bool

	changefeedID string
	captureID    string
	interval     uint

	defaultContext context.Context
)

// cf holds changefeed id, which is used for output only
type cf struct {
	ID string `json:"id"`
}

// capture holds capture information
type capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is-owner"`
	AdvertiseAddr string `json:"address"`
}

// cfMeta holds changefeed info and changefeed status
type cfMeta struct {
	Info       *model.ChangeFeedInfo   `json:"info"`
	Status     *model.ChangeFeedStatus `json:"status"`
	Count      uint64                  `json:"count"`
	TaskStatus []captureTaskStatus     `json:"task-status"`
}

type captureTaskStatus struct {
	CaptureID  string            `json:"capture-id"`
	TaskStatus *model.TaskStatus `json:"status"`
}

type profileStatus struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

func newCliCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			err := util.InitLogger(&util.Config{Level: "warn"})
			if err != nil {
				fmt.Printf("init logger error %v", errors.ErrorStack(err))
				os.Exit(1)
			}

			etcdCli, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{cliPdAddr},
				DialTimeout: 30 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBlock(),
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
				// PD embeds an etcd server.
				return errors.Annotate(err, "fail to open PD client")
			}
			cdcEtcdCli = kv.NewCDCEtcdClient(etcdCli)
			pdCli, err = pd.NewClient([]string{cliPdAddr}, pd.SecurityOption{},
				pd.WithGRPCDialOptions(
					grpc.WithBlock(),
					grpc.WithConnectParams(grpc.ConnectParams{
						Backoff: backoff.Config{
							BaseDelay:  time.Second,
							Multiplier: 1.1,
							Jitter:     0.1,
							MaxDelay:   3 * time.Second,
						},
						MinConnectTimeout: 3 * time.Second,
					}),
				))
			if err != nil {
				return errors.Annotate(err, "fail to open PD client")
			}
			ctx := defaultContext
			errorTiKVIncompatible := true // Error if TiKV is incompatible.
			err = util.CheckClusterVersion(ctx, pdCli, cliPdAddr, errorTiKVIncompatible)
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
