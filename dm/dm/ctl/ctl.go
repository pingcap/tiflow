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

package ctl

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/ctl/master"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"

	"github.com/chzyer/readline"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var commandMasterFlags = CommandMasterFlags{}

// CommandMasterFlags are flags that used in all commands for dm-master.
type CommandMasterFlags struct {
	workers []string // specify workers to control on these dm-workers
}

// Reset clears cache of CommandMasterFlags.
func (c CommandMasterFlags) Reset() {
	//nolint:staticcheck
	c.workers = c.workers[:0]
}

// NewRootCmd generates a new rootCmd.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "dmctl",
		Short:         "DM control",
		SilenceUsage:  true,
		SilenceErrors: true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	// --worker worker1 -w worker2 --worker=worker3,worker4 -w=worker5,worker6
	cmd.PersistentFlags().StringSliceVarP(&commandMasterFlags.workers, "source", "s", []string{}, "MySQL Source ID.")
	cmd.AddCommand(
		master.NewStartTaskCmd(),
		master.NewStopTaskCmd(),
		master.NewPauseTaskCmd(),
		master.NewResumeTaskCmd(),
		master.NewCheckTaskCmd(),
		//	master.NewUpdateTaskCmd(),
		master.NewQueryStatusCmd(),
		master.NewShowDDLLocksCmd(),
		master.NewUnlockDDLLockCmd(),
		master.NewPauseRelayCmd(),
		master.NewResumeRelayCmd(),
		master.NewPurgeRelayCmd(),
		master.NewOperateSourceCmd(),
		master.NewOfflineMemberCmd(),
		master.NewOperateLeaderCmd(),
		master.NewListMemberCmd(),
		master.NewOperateSchemaCmd(),
		master.NewGetCfgCmd(),
		master.NewHandleErrorCmd(),
		master.NewTransferSourceCmd(),
		master.NewStartRelayCmd(),
		master.NewStopRelayCmd(),
		master.NewBinlogCmd(),
		master.NewShardDDLLockCmd(),
		master.NewSourceTableSchemaCmd(),
		master.NewConfigCmd(),
		newDecryptCmd(),
		newEncryptCmd(),
	)
	// copied from (*cobra.Command).InitDefaultHelpCmd
	helpCmd := &cobra.Command{
		Use:   "help [command]",
		Short: "Gets help about any command",
		Long: `Help provides help for any command in the application.
Simply type ` + cmd.Name() + ` help [path to command] for full details.`,

		Run: func(c *cobra.Command, args []string) {
			cmd2, _, e := c.Root().Find(args)
			if cmd2 == nil || e != nil {
				c.Printf("Unknown help topic %#q\n", args)
				_ = c.Root().Usage()
			} else {
				cmd2.InitDefaultHelpFlag() // make possible 'help' flag to be shown
				_ = cmd2.Help()
			}
		},
	}
	cmd.SetHelpCommand(helpCmd)
	return cmd
}

// Init initializes dm-control.
func Init(cfg *common.Config) error {
	// set the log level temporarily
	log.SetLevel(zapcore.InfoLevel)

	return errors.Trace(common.InitUtils(cfg))
}

// Start starts running a command.
func Start(args []string) (cmd *cobra.Command, err error) {
	commandMasterFlags.Reset()
	rootCmd := NewRootCmd()
	rootCmd.SetArgs(args)
	return rootCmd.ExecuteC()
}

func loop() error {
	l, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[31m»\033[0m ",
		HistoryFile:     "/tmp/dmctlreadline.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "^D",
	})
	if err != nil {
		return err
	}

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

		line = strings.TrimSpace(line)
		if line == "exit" {
			l.Close()
			os.Exit(0)
		} else if line == "" {
			continue
		}

		args := common.SplitArgsRespectQuote(line)
		c, err := Start(args)
		if err != nil {
			fmt.Println("fail to run:", args)
			fmt.Println("Error:", err)
			if c.CalledAs() == "" {
				fmt.Printf("Run '%v --help' for usage.\n", c.CommandPath())
			}
		}

		syncErr := log.L().Sync()
		if syncErr != nil {
			fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		}
	}
	return l.Close()
}

// MainStart starts running a command.
func MainStart(args []string) {
	rootCmd := NewRootCmd()
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return loop()
		}
		return cmd.Help()
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if printVersion, err := cmd.Flags().GetBool("version"); err != nil {
			return errors.Trace(err)
		} else if printVersion {
			cmd.Println(utils.GetRawInfo())
			os.Exit(0)
		}

		// Make it compatible to flags encrypt/decrypt
		if encrypt, err := cmd.Flags().GetString(common.EncryptCmdName); err != nil {
			return errors.Trace(err)
		} else if encrypt != "" {
			ciphertext, err := utils.Encrypt(encrypt)
			if err != nil {
				return errors.Trace(err)
			}
			fmt.Println(ciphertext)
			os.Exit(0)
		}
		if decrypt, err := cmd.Flags().GetString(common.DecryptCmdName); err != nil {
			return errors.Trace(err)
		} else if decrypt != "" {
			plaintext, err := utils.Decrypt(decrypt)
			if err != nil {
				return errors.Trace(err)
			}
			fmt.Println(plaintext)
			os.Exit(0)
		}

		if cmd.Name() == common.DecryptCmdName || cmd.Name() == common.EncryptCmdName {
			return nil
		}

		cfg := common.NewConfig(cmd.Flags())
		err := cfg.Adjust()
		if err != nil {
			return err
		}

		err = cfg.Validate()
		if err != nil {
			return err
		}

		return Init(cfg)
	}
	common.DefineConfigFlagSet(rootCmd.PersistentFlags())
	rootCmd.SetArgs(args)
	if c, err := rootCmd.ExecuteC(); err != nil {
		rootCmd.Println("Error:", err)
		if c.CalledAs() == "" {
			rootCmd.Printf("Run '%v --help' for usage.\n", c.CommandPath())
		}
		os.Exit(1)
	}
}

func newEncryptCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "encrypt <plain-text>",
		Short: "Encrypts plain text to cipher text",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}
			ciphertext, err := utils.Encrypt(args[0])
			if err != nil {
				return errors.Trace(err)
			}
			fmt.Println(ciphertext)
			return nil
		},
	}
}

func newDecryptCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "decrypt <cipher-text>",
		Short: "Decrypts cipher text to plain text",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}
			plaintext, err := utils.Decrypt(args[0])
			if err != nil {
				return errors.Trace(err)
			}
			fmt.Println(plaintext)
			return nil
		},
	}
}
