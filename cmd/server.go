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
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	serverPdAddr string
	statusAddr   string
	caPath       string
	certPath     string
	keyPath      string

	serverCmd = &cobra.Command{
		Use:              "server",
		Short:            "Start a TiCDC capture server",
		PersistentPreRun: preRunLogInfo,
		RunE:             runEServer,
	}
)

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVar(&serverPdAddr, "pd", "http://127.0.0.1:2379", "PD address, separated by comma")
	serverCmd.Flags().StringVar(&statusAddr, "status-addr", "127.0.0.1:8300", "Bind address for http status server")
	serverCmd.Flags().StringVar(&caPath, "ca", "", "CA certificate path for TLS connection")
	serverCmd.Flags().StringVar(&certPath, "cert", "", "Certificate path for TLS connection")
	serverCmd.Flags().StringVar(&keyPath, "key", "", "Private key path for TLS connection")
}

func preRunLogInfo(cmd *cobra.Command, args []string) {
	util.LogVersionInfo()
}

func runEServer(cmd *cobra.Command, args []string) error {
	addrs := strings.Split(statusAddr, ":")
	if len(addrs) != 2 {
		return errors.Errorf("invalid status address: %s", statusAddr)
	}
	_, err := strconv.ParseInt(addrs[1], 10, 64)
	if err != nil {
		return errors.Annotatef(err, "invalid status address: %s", statusAddr)
	}

	config := &cdc.Config{
		PD:         serverPdAddr,
		StatusAddr: statusAddr,
		Security: &cdc.Security{
			CAPath:   caPath,
			CertPath: certPath,
			KeyPath:  keyPath,
		},
	}

	server, err := cdc.NewServer(config)
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
		cancel()
	}()

	err = server.Run(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("run server", zap.String("error", errors.ErrorStack(err)))
		return errors.Annotate(err, "run server")
	}
	server.Close()
	log.Info("cdc server exits successfully")

	return nil
}
