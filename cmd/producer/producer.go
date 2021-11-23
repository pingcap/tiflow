package main

import (
	"log"
	"net"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

type server struct {
	pb.TmpServiceServer
}

func (s *server) EventFeed(in *pb.Request, server pb.TmpService_EventFeedServer) error {
	tid := in.MaxTid
	log.Printf("here comes new req %d", tid)
	i := int32(0)
	for {
		tm := time.Now().Format(time.RFC3339Nano)
		err := server.Send(&pb.Record{
			StartTs: []byte(tm),
			Tid:     i,
			Payload: []byte(tm),
		})
		if err != nil {
			log.Printf("meet error %v", err)
			return err
		}
		i = (i + 1) % tid
	}
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("fail to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTmpServiceServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
