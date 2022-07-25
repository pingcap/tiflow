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
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"go.uber.org/zap"
)

type memFile struct {
	start int
	end   int
	step  int
	iter  int
}

func (m *memFile) setIter(iter int) (*memFile, bool) {
	newIter := 0
	if iter < m.start {
		newIter = m.start
	} else if iter <= m.end {
		newIter = iter
	} else {
		return nil, false
	}
	return &memFile{
		start: m.start,
		end:   m.end,
		step:  m.step,
		iter:  newIter,
	}, true
}

func (m *memFile) get() (int, bool) {
	defer func() { m.iter += m.step }()
	if m.iter > m.end {
		return 0, false
	}
	return m.iter, true
}

func (m *memFile) insert(v int) bool {
	if v < m.step {
		m.start = v
		m.end = v
	} else if m.end < v {
		if m.end+m.step != v {
			return false
		}
		m.end = v
	}
	return true
}

type memDB map[int]*memFile

type dataRWServiceMock struct {
	mu    sync.Mutex
	dbMap map[string]memDB
}

// ListFiles implements DataRWService.ListFiles
func (s *dataRWServiceMock) ListFiles(ctx context.Context, _ *pb.ListFilesReq) (*pb.ListFilesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.ListFilesResponse{FileNum: int32(len(s.dbMap[demoDir]))}, nil
}

// GenerateData implements DataRWService.GenerateData
func (s *dataRWServiceMock) GenerateData(ctx context.Context, req *pb.GenerateDataRequest) (*pb.GenerateDataResponse, error) {
	ready = make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Info("Start to generate data ...")
	fileNum := int(req.FileNum)
	origin := make(map[int]*memFile)
	for i := 0; i < fileNum; i++ {
		origin[i] = &memFile{step: fileNum}
	}

	for k := 0; k < int(req.RecordNum); k++ {
		index := k % fileNum
		origin[index].insert(k)
	}
	s.dbMap[demoDir] = origin

	log.Info("files have been created", zap.Any("filenumber", fileNum))
	close(ready)
	return &pb.GenerateDataResponse{}, nil
}

// ReadLines implements DataRWService.ReadLines
func (s *dataRWServiceMock) ReadLines(req *pb.ReadLinesRequest, stream pb.DataRWService_ReadLinesServer) error {
	log.Info("receive the request for reading file ", zap.Any("idx", req.FileIdx), zap.String("lineNo", string(req.LineNo)))
	s.mu.Lock()
	db, ok := s.dbMap[demoDir][int(req.FileIdx)]
	s.mu.Unlock()
	if !ok {
		return stream.Send(&pb.ReadLinesResponse{ErrMsg: fmt.Sprintf("file idx %d is out of range %d", req.FileIdx, len(s.dbMap[demoAddress])), IsEof: true})
	}
	log.Info("db reading", zap.Any("begin", db.start), zap.Any("end", db.end))
	seekByte := req.GetLineNo()
	seek := 0
	if len(seekByte) > 0 {
		var err error
		seek, err = strconv.Atoi(string(seekByte))
		if err != nil {
			return stream.Send(&pb.ReadLinesResponse{ErrMsg: "Cannot find key " + string(req.LineNo)})
		}
	}
	iter, ok := db.setIter(seek)
	if !ok {
		return stream.Send(&pb.ReadLinesResponse{ErrMsg: "Cannot find key " + string(req.LineNo)})
	}
	for {
		v, ok := iter.get()
		if !ok {
			return stream.Send(&pb.ReadLinesResponse{IsEof: true})
		}
		vByte := strconv.Itoa(v)
		err := stream.Send(&pb.ReadLinesResponse{Key: []byte(vByte), IsEof: false})
		if err != nil {
			return err
		}
	}
}

// WriteLines implements DataRWService.WriteLines
func (s *dataRWServiceMock) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	var dir string
	var idx int
	file := &memFile{}
	for {
		res, err := stream.Recv()
		if err == nil {
			if dir == "" {
				dir = res.Dir
				idx = int(res.FileIdx)
				s.mu.Lock()
				log.Info("first writing", zap.String("dir", dir), zap.Any("idx", idx))
				bucket, ok := s.dbMap[dir]
				if !ok {
					bucket = make(memDB)
				}
				file, ok = bucket[idx]
				if !ok {
					file = &memFile{step: len(s.dbMap[demoDir])}
					bucket[idx] = file
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
			v, err := strconv.Atoi(string(res.Key))
			if err != nil {
				log.Error("write wrong data v",
					zap.Error(err), zap.String("key", string(res.Key)))
				return stream.SendAndClose(&pb.WriteLinesResponse{ErrMsg: err.Error()})
			}
			if !file.insert(v) {
				log.Error("write incorrect value", zap.Int("end", file.end), zap.Int("insert", v))
			}
		} else if err == io.EOF {
			log.Info("receive the eof")
			return stream.SendAndClose(&pb.WriteLinesResponse{})
		} else {
			log.Error("write loop met error", zap.Error(err))
			return err
		}
	}
}

// CheckDir implements DataRWService.CheckDir
func (s *dataRWServiceMock) CheckDir(ctx context.Context, req *pb.CheckDirRequest) (*pb.CheckDirResponse, error) {
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
		if originDB.end == targetDB.end {
			continue
		}
		err := fmt.Errorf("origin end is %d but target end is %d", originDB.end, targetDB.end)
		log.Error("compare failed", zap.String("req dir", req.Dir), zap.Any("id", originID), zap.Error(err))
		return &pb.CheckDirResponse{ErrMsg: err.Error(), ErrFileIdx: int32(originID)}, nil
	}
	return &pb.CheckDirResponse{}, nil
}

// IsReady implements DataRWService.IsReady
func (s *dataRWServiceMock) IsReady(ctx context.Context, req *pb.IsReadyRequest) (*pb.IsReadyResponse, error) {
	select {
	case <-ready:
		return &pb.IsReadyResponse{Ready: true}, nil
	default:
		return &pb.IsReadyResponse{Ready: false}, nil
	}
}
