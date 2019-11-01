package cmd

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	pdEndpoints string
	statusAddr  string

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
	serverCmd.Flags().StringVar(&statusAddr, "status-addr", "127.0.0.1:8300", "bind address for http status server")
}

func runEServer(cmd *cobra.Command, args []string) error {
	addrs := strings.Split(statusAddr, ":")
	if len(addrs) != 2 {
		return errors.Errorf("invalid status address: %s", statusAddr)
	}
	statusPort, err := strconv.ParseInt(addrs[1], 10, 64)
	if err != nil {
		return errors.Annotatef(err, "invalid status address: %s", statusAddr)
	}

	var opts []cdc.ServerOption
	opts = append(opts, cdc.PDEndpoints(pdEndpoints), cdc.StatusHost(addrs[0]), cdc.StatusPort(int(statusPort)))

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
