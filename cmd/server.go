package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	pdEndpoints string
	statusHost  string
	statusPort  int

	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "runs capture server",
		Long:  "runs capture server",
		RunE:  runEServer,
	}
)

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&pdEndpoints, "pd-endpoints", "http://127.0.0.1:2379", "endpoints of PD, separated by comma")
	serverCmd.Flags().StringVar(&statusHost, "status-host", "127.0.0.1", "host of status http server")
	serverCmd.Flags().IntVar(&statusPort, "status-port", 8300, "portof status http server")
}

func runEServer(cmd *cobra.Command, args []string) error {
	var opts []cdc.ServerOption
	opts = append(opts, cdc.PDEndpoints(pdEndpoints), cdc.StatusHost(statusHost), cdc.StatusPort(statusPort))

	server, err := cdc.NewServer(opts...)
	if err != nil {
		return errors.Annotate(err, "new server")
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-sc
		log.Info("got signal to exit", zap.Stringer("signal", sig))
		server.Close(ctx, cancel)
	}()

	err = server.Run(ctx)
	if err != nil {
		if errors.Cause(err) == context.Canceled {
			return nil
		}
		log.Error("run server", zap.String("error", errors.ErrorStack(err)))
		return errors.Annotate(err, "run server")
	}

	return nil
}
