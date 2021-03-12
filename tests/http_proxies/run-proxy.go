package main

import (
	"flag"
	"fmt"

	grpc_proxy "github.com/bradleyjkemp/grpc-tools/grpc-proxy"
	"google.golang.org/grpc"
)

func main() {
	grpc_proxy.RegisterDefaultFlags()
	flag.Parse()
	proxy, _ := grpc_proxy.New(
		grpc_proxy.WithInterceptor(intercept),
		grpc_proxy.DefaultFlags(),
	)
	proxy.Start()
}

func intercept(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fmt.Println(info.FullMethod)
	return handler(srv, ss)
}
