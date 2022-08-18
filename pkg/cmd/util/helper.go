// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http/httpproxy"
)

// Endpoint schemes.
const (
	HTTP  = "http"
	HTTPS = "https"
)

// InitCmd initializes the logger, the default context and returns its cancel function.
func InitCmd(cmd *cobra.Command, logCfg *logutil.Config) context.CancelFunc {
	// Init log.
	err := logutil.InitLogger(logCfg, logutil.WithInitGRPCLogger(), logutil.WithInitSaramaLogger())
	if err != nil {
		cmd.Printf("init logger error %v\n", errors.ErrorStack(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", logCfg.File), zap.String("level", logCfg.Level))

	ctx, cancel := context.WithCancel(context.Background())
	cmdconetxt.SetDefaultContext(ctx)

	return cancel
}

// shutdownNotify is a callback to notify caller that TiCDC is about to shutdown.
// It returns a done channel which receive an empty struct when shutdown is complete.
// It must be non-blocking.
type shutdownNotify func() <-chan struct{}

// InitSignalHandling initializes signal handling.
// It must be called after InitCmd.
func InitSignalHandling(shutdown shutdownNotify, cancel context.CancelFunc) {
	// systemd and k8s send signals twice. The first is for graceful shutdown,
	// and the second is for force shutdown.
	// We use 2 for channel length to ease testing.
	signalChanLen := 2
	sc := make(chan os.Signal, signalChanLen)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Info("got signal, prepare to shutdown", zap.Stringer("signal", sig))
		done := shutdown()
		select {
		case <-done:
			log.Info("shutdown complete")
		case sig = <-sc:
			log.Info("got signal, force shutdown", zap.Stringer("signal", sig))
		}
		cancel()
	}()
}

// LogHTTPProxies logs HTTP proxy relative environment variables.
func LogHTTPProxies() {
	fields := findProxyFields()
	if len(fields) > 0 {
		log.Info("using proxy config", fields...)
	}
}

func findProxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}

// StrictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func StrictDecodeFile(path, component string, cfg interface{}, ignoreCheckItems ...string) error {
	metaData, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// check if item is a ignoreCheckItem
	hasIgnoreItem := func(item []string) bool {
		for _, ignoreCheckItem := range ignoreCheckItems {
			if item[0] == ignoreCheckItem {
				return true
			}
		}
		return false
	}

	if undecoded := metaData.Undecoded(); len(undecoded) > 0 {
		var b strings.Builder
		hasUnknownConfigSize := 0
		for _, item := range undecoded {
			if hasIgnoreItem(item) {
				continue
			}

			if hasUnknownConfigSize > 0 {
				b.WriteString(", ")
			}
			b.WriteString(item.String())
			hasUnknownConfigSize++
		}
		if hasUnknownConfigSize > 0 {
			err = errors.Errorf("component %s's config file %s contained unknown configuration options: %s",
				component, path, b.String())
		}
	}
	return errors.Trace(err)
}

// VerifyPdEndpoint verifies whether the pd endpoint is a valid http or https URL.
// The certificate is required when using https.
func VerifyPdEndpoint(pdEndpoint string, useTLS bool) error {
	u, err := url.Parse(pdEndpoint)
	if err != nil {
		return errors.Annotate(err, "parse PD endpoint")
	}
	if (u.Scheme != HTTP && u.Scheme != HTTPS) || u.Host == "" {
		return errors.New("PD endpoint should be a valid http or https URL")
	}

	if useTLS {
		if u.Scheme == HTTP {
			return errors.New("PD endpoint scheme should be https")
		}
	} else {
		if u.Scheme == HTTPS {
			return errors.New("PD endpoint scheme is https, please provide certificate")
		}
	}
	return nil
}

// JSONPrint will output the data in JSON format.
func JSONPrint(cmd *cobra.Command, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	cmd.Printf("%s\n", data)
	return nil
}

// CheckErr is used to cmd err.
func CheckErr(err error) {
	if cerror.IsCliUnprintableError(err) {
		err = nil
	}
	cobra.CheckErr(err)
}
