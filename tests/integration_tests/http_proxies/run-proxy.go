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

package main

import (
	"flag"
	"fmt"

	grpc_proxy "github.com/bradleyjkemp/grpc-tools/grpc-proxy"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func main() {
	defer func() {
		fmt.Println("proxy stopped")
	}()

	grpc_proxy.RegisterDefaultFlags()
	flag.Parse()

	log.Info("starting proxy", zap.Any("flags", flag.Args()))

	proxy, err := grpc_proxy.New(
		grpc_proxy.WithInterceptor(intercept),
		grpc_proxy.DefaultFlags(),
	)
	if err != nil {
		log.Fatal("failed to create proxy", zap.Error(err))
	}
	err = proxy.Start()
	if err != nil {
		log.Fatal("failed to start proxy", zap.Error(err))
	}
	fmt.Println("proxy started")
}

func intercept(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fmt.Println(info.FullMethod)
	err := handler(srv, ss)
	if err != nil {
		md, ok := metadata.FromIncomingContext(ss.Context())
		log.Error("failed to handle stream",
			zap.String("method", info.FullMethod),
			zap.Bool("ok", ok),
			zap.Any("metadata", md),
			zap.Error(err))
	}
	return err
}
