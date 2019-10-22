package cmd

import (
	"fmt"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	logFile  string
	logLevel string
)

var rootCmd = &cobra.Command{
	Use:   "cdc",
	Short: "CDC",
	Long:  `Change Data Capture`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
		feed()
	},
}

func init() {
	cobra.OnInitialize(func() {
		err := initLog()
		if err != nil {
			fmt.Printf("fail to init log: %v", err)
			os.Exit(1)
		}
	})
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "cdc.log", "log file path")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "debug", "log level (etc: debug|info|warn|error)")
}

func initLog() error {
	// Init log.
	err := util.InitLogger(&util.Config{
		File:  logFile,
		Level: logLevel,
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", logFile), zap.String("level", logLevel))

	return nil
}

func Execute() {
	// Run root cmd.
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
