// Copyright 2022 PingCAP, Inc.
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
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	demoAddress = "0.0.0.0:1234"
	demoDir     = "/data/demo/"
	dataNum     = 0
)

var (
	ready = make(chan struct{})
	mock  = false
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// Use config file save this chaos.
	if len(os.Args) > 1 {
		if len(os.Args) == 2 {
			if os.Args[1] == "mock" {
				mock = true
				goto START
			}
			fmt.Print("unknown flag " + os.Args[1])
			os.Exit(1)
		}
		hint := "demo args should be: -d dir -a port [-r record number]"
		fmt.Println(hint)
		demoAddress = os.Args[4]
		demoDir = os.Args[2]
		if len(os.Args) > 5 {
			var err error
			dataNum, err = strconv.Atoi(os.Args[6]) //nolint:gosec
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}
	}
START:
	fmt.Printf("starting demo, dir %s addr %s\n", demoDir, demoAddress)
	err := logutil.InitLogger(&logutil.Config{
		Level: "info",
		File:  demoDir + "/demo.log",
	})
	if err != nil {
		fmt.Printf("err: %v", err)
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		select {
		case <-ctx.Done():
		case sig := <-sc:
			log.Info("got signal to exit", zap.Stringer("signal", sig))
			cancel()
		}
	}()
	startDataService(ctx)
	log.Info("server exits normally")
}

// ErrorInfo wraps info
type ErrorInfo struct {
	info string
}

func (e *ErrorInfo) Error() string {
	return e.info
}

// GenerateData implements DataRWService.GenerateData
func (s *DataRWServer) GenerateData(ctx context.Context, req *pb.GenerateDataRequest) (*pb.GenerateDataResponse, error) {
	ready = make(chan struct{})
	s.mu.Lock()
	for _, bucket := range s.dbMap {
		for _, db := range bucket {
			err := db.Close()
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
	s.dbMap = make(map[string]dbBuckets)
	s.mu.Unlock()
	log.Info("Start to generate data ...")
	start := time.Now()

	fileNum := int(req.FileNum)
	batches := make([]db.Batch, 0, fileNum)
	bucket := make(dbBuckets)
	for i := 0; i < fileNum; i++ {
		fileDB, err := db.OpenPebble(s.ctx, i, demoDir,
			config.GetDefaultServerConfig().Debug.DB,
			db.WithCache(256<<10))
		if err != nil {
			return &pb.GenerateDataResponse{ErrMsg: err.Error()}, nil
		}
		bucket[i] = fileDB
		batches = append(batches, fileDB.Batch(1024))
	}

	for k := 0; k < int(req.RecordNum); k++ {
		index := k % fileNum
		key := strconv.Itoa(k)
		value := strconv.Itoa(rand.Intn(int(req.RecordNum)))
		batch := batches[index]
		batch.Put([]byte(key), []byte(value))
	}
	for _, batch := range batches {
		err := batch.Commit()
		if err != nil {
			return &pb.GenerateDataResponse{ErrMsg: err.Error()}, nil
		}
	}

	s.mu.Lock()
	s.dbMap[demoDir] = bucket
	s.mu.Unlock()

	log.Info("files have been created", zap.Any("filenumber", fileNum),
		zap.Duration("duration", time.Since(start)))
	close(ready)
	return &pb.GenerateDataResponse{}, nil
}

func startDataService(ctx context.Context) {
	grpcServer := grpc.NewServer()
	var s pb.DataRWServiceServer
	if mock {
		s = &dataRWServiceMock{
			dbMap: make(map[string]memDB),
		}
		pb.RegisterDataRWServiceServer(grpcServer, s)
	} else {
		s = NewDataRWServer(ctx)
		pb.RegisterDataRWServiceServer(grpcServer, s)
	}
	lis, err := net.Listen("tcp", demoAddress) //nolint:gosec
	if err != nil {
		log.Panic("listen the port failed",
			zap.String("error:", err.Error()))
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		log.Info("grpc serving ..")
		return grpcServer.Serve(lis)
	})

	wg.Go(func() error {
		<-ctx.Done()
		grpcServer.Stop()
		return nil
	})
	wg.Go(func() error {
		if dataNum == 0 {
			return nil
		}
		log.Info("preparing data...", zap.Any("num", dataNum))
		resp, _ := s.GenerateData(ctx, &pb.GenerateDataRequest{ // nolint: errcheck
			FileNum:   10,
			RecordNum: int32(dataNum), //nolint:gosec
		})
		if len(resp.ErrMsg) > 0 {
			log.Error("generate data failed", zap.String("err", resp.ErrMsg))
			os.Exit(1)
		}
		log.Info("generate data finish")
		return nil
	})

	if err := wg.Wait(); err != nil {
		log.Error("run grpc server with error", zap.Error(err))
	}
}

type dbBuckets map[int]db.DB

// DataRWServer defines a struct that implements DataRWService
type DataRWServer struct {
	ctx   context.Context
	mu    sync.Mutex
	dbMap map[string]dbBuckets
}

// NewDataRWServer creates a new DataRWServer instance
func NewDataRWServer(ctx context.Context) *DataRWServer {
	s := &DataRWServer{
		ctx:   ctx,
		dbMap: make(map[string]dbBuckets),
	}

	return s
}

// ListFiles implements DataRWService.ListFiles
func (s *DataRWServer) ListFiles(ctx context.Context, _ *pb.ListFilesReq) (*pb.ListFilesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.ListFilesResponse{FileNum: int32(len(s.dbMap[demoDir]))}, nil
}

// IsReady implements DataRWService.IsReady
func (s *DataRWServer) IsReady(ctx context.Context, req *pb.IsReadyRequest) (*pb.IsReadyResponse, error) {
	select {
	case <-ready:
		return &pb.IsReadyResponse{Ready: true}, nil
	default:
		return &pb.IsReadyResponse{Ready: false}, nil
	}
}

func (s *DataRWServer) compareDBs(db1, db2 db.DB) error {
	iter1 := db1.Iterator([]byte{}, []byte{0xff}, 0, math.MaxUint64)
	iter2 := db2.Iterator([]byte{}, []byte{0xff}, 0, math.MaxUint64)
	iter1.Seek([]byte{})
	iter2.Seek([]byte{})
	var lastBytes string
	for {
		if err := iter1.Error(); err != nil {
			return err
		}
		if err := iter2.Error(); err != nil {
			return err
		}
		if !iter1.Valid() || !iter2.Valid() {
			if iter1.Valid() {
				return errors.New("db2 is shorter than db1, lastChar is " + lastBytes)
			}
			if iter2.Valid() {
				return errors.New("db1 is shorter than db2, lastChar is" + lastBytes)
			}
			return nil
		}
		k1 := string(iter1.Key())
		k2 := string(iter2.Key())
		lastBytes = k1
		if k1 != k2 {
			return fmt.Errorf("keys are different, k1 %s, k2 %s", k1, k2)
		}
		v1 := string(iter1.Value())
		v2 := string(iter2.Value())
		if v1 != v2 {
			return fmt.Errorf("keys are different, key %s, v1 %s, v2 %s", k1, v1, v2)
		}
		iter1.Next()
		iter2.Next()
	}
}

// CheckDir implements DataRWService.CheckDir
func (s *DataRWServer) CheckDir(ctx context.Context, req *pb.CheckDirRequest) (*pb.CheckDirResponse, error) {
	s.mu.Lock()
	originBucket := s.dbMap[demoDir]
	targetBucket, ok := s.dbMap[req.Dir]
	if !ok {
		return &pb.CheckDirResponse{ErrMsg: fmt.Sprintf("cannot find %s db", req.Dir)}, nil
	}
	s.mu.Unlock()
	for originID, originDB := range originBucket {
		targetDB, ok := targetBucket[originID]
		if !ok {
			return &pb.CheckDirResponse{ErrMsg: fmt.Sprintf("%d id cannot find in %s db", originID, req.Dir), ErrFileIdx: int32(originID)}, nil
		}
		err := s.compareDBs(originDB, targetDB)
		if err != nil {
			log.Error("compare failed", zap.String("req dir", req.Dir), zap.Any("id", originID), zap.Error(err))
			return &pb.CheckDirResponse{ErrMsg: err.Error(), ErrFileIdx: int32(originID)}, nil
		}
	}
	return &pb.CheckDirResponse{}, nil
}

// ReadLines implements DataRWService.ReadLines
func (s *DataRWServer) ReadLines(req *pb.ReadLinesRequest, stream pb.DataRWService_ReadLinesServer) error {
	log.Info("receive the request for reading file ", zap.Any("idx", req.FileIdx), zap.String("lineNo", string(req.LineNo)))
	s.mu.Lock()
	db, ok := s.dbMap[demoDir][int(req.FileIdx)]
	s.mu.Unlock()
	if !ok {
		return stream.Send(&pb.ReadLinesResponse{ErrMsg: fmt.Sprintf("file idx %d is out of range %d", req.FileIdx, len(s.dbMap[demoAddress])), IsEof: true})
	}
	iter := db.Iterator([]byte{}, []byte{0xff}, 0, math.MaxUint64)
	if !iter.Seek(req.LineNo) {
		return stream.Send(&pb.ReadLinesResponse{ErrMsg: "Cannot find key " + string(req.LineNo)})
	}
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			err := iter.Error()
			if err != nil {
				return stream.Send(&pb.ReadLinesResponse{ErrMsg: err.Error()})
			}
			if !iter.Valid() {
				log.Info("reach the end of the file")
				return stream.Send(&pb.ReadLinesResponse{IsEof: true})
			}
			err = stream.Send(&pb.ReadLinesResponse{Key: iter.Key(), Val: iter.Value(), IsEof: false})
			if err != nil {
				return err
			}
			iter.Next()
		}
	}
}

// WriteLines implements DataRWService.WriteLines
func (s *DataRWServer) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	var dir string
	var idx int
	var peddleDB db.DB
	var batch db.Batch
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			res, err := stream.Recv()
			if err == nil {
				if dir == "" {
					dir = res.Dir
					idx = int(res.FileIdx)
					s.mu.Lock()
					log.Info("first writing", zap.String("dir", dir), zap.Any("idx", idx))
					bucket, ok := s.dbMap[dir]
					if !ok {
						bucket = make(dbBuckets)
					}
					peddleDB, ok = bucket[idx]
					if !ok {
						peddleDB, err = db.OpenPebble(s.ctx, idx, dir,
							config.GetDefaultServerConfig().Debug.DB,
							db.WithCache(256<<10))
						if err != nil {
							s.mu.Unlock()
							log.Error("write line meet error", zap.String("request", res.String()), zap.Error(err))
							return stream.SendAndClose(&pb.WriteLinesResponse{ErrMsg: err.Error()})
						}
						bucket[idx] = peddleDB
					}
					s.dbMap[dir] = bucket
					s.mu.Unlock()
				} else {
					if dir != res.Dir {
						log.Error("Different writing dir in the same thread", zap.String("dir1", dir), zap.String("dir2", res.Dir))
						return stream.SendAndClose(&pb.WriteLinesResponse{ErrMsg: "wrong dir names"})
					}
					if idx != int(res.FileIdx) {
						log.Error("Different file idx in the same thread", zap.Any("idx1", idx), zap.Any("idx2", res.FileIdx))
						return stream.SendAndClose(&pb.WriteLinesResponse{ErrMsg: "wrong idx"})
					}
				}
				batch = peddleDB.Batch(2048)
				batch.Put(res.Key, res.Value)
				err := batch.Commit()
				if err != nil {
					log.Error("write data failed  ",
						zap.String("error   ", err.Error()))
					return stream.SendAndClose(&pb.WriteLinesResponse{ErrMsg: err.Error()})
				}
			} else if err == io.EOF {
				log.Info("receive the eof")
				return stream.SendAndClose(&pb.WriteLinesResponse{})
			} else {
				log.Error("receive loop met error", zap.Error(err))
				return err
			}
		}
	}
}
