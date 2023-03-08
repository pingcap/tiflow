// Copyright 2023 PingCAP, Inc.
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

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/httputil"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		log.Warn("received signal to exit", zap.Stringer("signal", sig))
		switch sig {
		case syscall.SIGTERM:
			cancel()
			os.Exit(0)
		default:
			cancel()
			os.Exit(1)
		}
	}()

	if err := run(ctx); err != nil {
		log.Panic("run api v2 integration test failed", zap.Error(err))
		os.Exit(1)
	}
}

func newAPIClient() (*CDCRESTClient, error) {
	cli, err := httputil.NewClient(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostURL, err := url.Parse("http://127.0.0.1:8300")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CDCRESTClient{
		base:             hostURL,
		versionedAPIPath: "/api/v2",
		Client:           cli,
	}, nil
}

var cases = []func(ctx context.Context, client *CDCRESTClient) error{
	testStatus,
	testClusterHealth,
	testChangefeed,
	testCreateChangefeed,
	testRemoveChangefeed,
	testCapture,
	testProcessor,
	testResignOwner,
	testSetLogLevel,
}

func run(ctx context.Context) error {
	client, err := newAPIClient()
	if err != nil {
		return errors.Trace(err)
	}
	for _, cse := range cases {
		if err := cse(ctx, client); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
