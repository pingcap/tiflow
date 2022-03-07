package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/sergi/go-diff/diffmatchpatch"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	FILENUM     = 50
	RECORDERNUM = 1000000
	FLUSHLEN    = 10
	PORT        = "0.0.0.0:1234"
)

var ready = make(chan struct{})

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	err := log.InitLogger(&log.Config{
		Level: "info",
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
			log.L().Info("got signal to exit", zap.Stringer("signal", sig))
			cancel()
		}
	}()
	Run(ctx, os.Args)
	log.L().Info("server exits normally")
}

func Run(ctx context.Context, args []string) {
	if len(args) >= 2 {
		folder := args[1]
		recorderNum := RECORDERNUM
		if len(args) > 2 {
			var err error
			recorderNum, err = strconv.Atoi(args[2])
			if err != nil {
				log.L().Fatal("the third parameter should be an interger")
			}
		}
		go generateData(folder, recorderNum)
	} else {
		log.L().Fatal("the args should be : dir [100000]")
	}

	StartDataService(ctx)
}

type ErrorInfo struct {
	info string
}

func (e *ErrorInfo) Error() string {
	return e.info
}

func generateData(folderName string, recorders int) int {
	fmt.Println("Start to generate the data ...")
	_dir, err := ioutil.ReadDir(folderName)
	if err != nil {
		log.L().Warn("the folder doesn't exist ", zap.String("folder", folderName))
		err = os.Mkdir(folderName, os.ModePerm)
		if err != nil {
			log.L().Error("create the folder failed", zap.String("folder", folderName))
			return 0
		}

	} else {
		for _, _file := range _dir {
			fileName := folderName + "/" + _file.Name()
			if _file.IsDir() {
				os.RemoveAll(fileName)
			} else {
				os.Remove(fileName)
			}
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fileNum := r.Intn(FILENUM) + 1
	fileWriterMap := make(map[int]*bufio.Writer)
	for i := 0; i < fileNum; i++ {
		fileName := folderName + "/" + strconv.Itoa(i) + ".txt"
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			// We don't allow failure of creating files for simplicity.
			log.L().Logger.Panic("fail to generate file", zap.String("name", fileName), zap.Error(err))
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		fileWriterMap[i] = writer
	}
	shouldFlush := false
	for k := 0; k < recorders; k++ {
		if (k % FLUSHLEN) == 0 {
			shouldFlush = true
		}
		index := k % fileNum
		_, err2 := fileWriterMap[index].WriteString(strconv.Itoa(k) + "," + "value\n")
		if err2 != nil {
			continue
		}
		if shouldFlush {
			err1 := fileWriterMap[index].Flush()
			if err1 != nil {
				panic(err1)
			}
		}
		if index == 0 {
			shouldFlush = false
		}
	}
	for _, writer := range fileWriterMap {
		writer.Flush()
	}
	log.L().Info("files have been created", zap.Any("filnumber", fileNum))
	close(ready)
	return fileNum
}

func StartDataService(ctx context.Context) {
	grpcServer := grpc.NewServer()
	pb.RegisterDataRWServiceServer(grpcServer, NewDataRWServer(ctx))
	lis, err := net.Listen("tcp", PORT) //nolint:gosec
	if err != nil {
		log.L().Panic("listen the port failed",
			zap.String("error:", err.Error()))
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		log.L().Info("grpc serving ..")
		return grpcServer.Serve(lis)
	})

	wg.Go(func() error {
		<-ctx.Done()
		grpcServer.Stop()
		return nil
	})

	if err := wg.Wait(); err != nil {
		log.L().Error("run grpc server with error", zap.Error(err))
	}
}

type DataRWServer struct {
	ctx           context.Context
	mu            sync.Mutex
	fileWriterMap map[string]*bufio.Writer
}

func NewDataRWServer(ctx context.Context) *DataRWServer {
	s := &DataRWServer{
		ctx:           ctx,
		fileWriterMap: make(map[string]*bufio.Writer),
	}

	return s
}

func (*DataRWServer) ListFiles(ctx context.Context, folder *pb.ListFilesReq) (*pb.ListFilesResponse, error) {
	fd := folder.FolderName
	_dir, err := ioutil.ReadDir(fd)
	log.L().Info("receive the list file call ",
		zap.String("folder name ", fd))
	if err != nil {
		return &pb.ListFilesResponse{}, err
	}

	files := []string{}
	for _, _file := range _dir {
		if !_file.IsDir() {
			files = append(files, _file.Name())
		}
	}
	return &pb.ListFilesResponse{FileNames: files}, nil
}

func (s *DataRWServer) IsReady(ctx context.Context, req *pb.IsReadyRequest) (*pb.IsReadyResponse, error) {
	select {
	case <-ready:
		return &pb.IsReadyResponse{Ready: true}, nil
	default:
		return &pb.IsReadyResponse{Ready: false}, nil
	}
}

func openFileAndReadString(path string) (content []byte, err error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return ioutil.ReadAll(fp)
}

func (s *DataRWServer) compareTwoFiles(path1, path2 string) error {
	log.L().Info("compare two files", zap.String("p1", path1), zap.String("p2", path2))
	str1, err := openFileAndReadString(path1)
	if err != nil {
		return err
	}
	str2, err := openFileAndReadString(path2)
	if err != nil {
		return err
	}
	dmp := diffmatchpatch.New()

	diffs := dmp.DiffMain(string(str1), string(str2), false)
	for i, diff := range diffs {
		if diff.Type != diffmatchpatch.DiffEqual {
			return errors.New(dmp.DiffPrettyText(diffs[i:]))
		}
	}
	return nil
}

func (s *DataRWServer) CheckDir(ctx context.Context, req *pb.CheckDirRequest) (*pb.CheckDirResponse, error) {
	dir := req.Dir
	s.mu.Lock()
	defer s.mu.Unlock()
	for path := range s.fileWriterMap {
		name := filepath.Base(path)
		destPath := filepath.Join(dir, name)
		err := s.compareTwoFiles(path, destPath)
		if err != nil {
			return &pb.CheckDirResponse{ErrMsg: err.Error(), ErrFileName: destPath}, nil
		}
	}
	return &pb.CheckDirResponse{}, nil
}

func (s *DataRWServer) ReadLines(req *pb.ReadLinesRequest, stream pb.DataRWService_ReadLinesServer) error {
	fileName := req.FileName
	log.L().Info("receive the request for reading file ")
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0o666)
	if err != nil {
		log.L().Error("make sure the file exist ",
			zap.String("fileName ", fileName))
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	i := 0
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			time.Sleep(time.Millisecond * 5)
			reply, err := reader.ReadString('\n')
			if err == io.EOF {
				log.L().Info("reach the end of the file")
				err = stream.Send(&pb.ReadLinesResponse{Linestr: "", IsEof: true})
				return err
			}
			if i < int(req.LineNo) {
				continue
			}
			reply = strings.TrimSpace(reply)
			if reply == "" {
				continue
			}
			err = stream.Send(&pb.ReadLinesResponse{Linestr: reply, IsEof: false})
			i++
			if err != nil {
				return err
			}
		}
	}
}

func (s *DataRWServer) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	count := 0
	fileNames := make([]string, 0)
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			res, err := stream.Recv()
			if err == nil {
				fileName := res.FileName
				if strings.TrimSpace(fileName) == "" {
					continue
				}
				fileNames = append(fileNames, fileName)
				s.mu.Lock()
				writer, ok := s.fileWriterMap[fileName]
				s.mu.Unlock()
				if !ok {
					var file *os.File
					_, err = os.Stat(fileName)
					if err == nil {
						file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0o666)
					} else {
						index := strings.LastIndex(fileName, "/")
						if index <= 1 {
							log.L().Error("bad file name ",
								zap.Int("index ", index))
							return &ErrorInfo{info: " bad file name :" + fileName}
						}
						folder := fileName[0:index]
						_, err = ioutil.ReadDir(folder)
						// create the dir if not exist
						if err != nil {
							if strings.Index(folder, "/") > 0 {
								err = os.MkdirAll(folder, os.ModePerm)
							} else {
								err = os.Mkdir(folder, os.ModePerm)
							}
							if err != nil {
								log.L().Warn("create folder failed", zap.String("folder", folder), zap.Error(err))
								// may be folder has been created
								time.Sleep(10 * time.Millisecond)
							} else {
								log.L().Info("create the folder ",
									zap.String("folder  ", folder))
							}
						}
						// create the file
						file, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
					}
					defer file.Close()
					if err != nil {
						log.L().Error("create file failed", zap.String("file", fileName), zap.Error(err))
						return err
					}
					writer = bufio.NewWriter(file)
					defer writer.Flush()
					s.mu.Lock()
					s.fileWriterMap[fileName] = writer
					s.mu.Unlock()
				}
				_, err = writer.WriteString(res.Key + "," + strings.TrimSpace(res.Value) + "\n")
				count++
				if (count % FLUSHLEN) == 0 {
					err = writer.Flush()
				}
				if err != nil {
					log.L().Error("write data failed  ",
						zap.String("error   ", err.Error()))
				}

			} else if err == io.EOF {
				log.L().Info("receive the eof")
				s.mu.Lock()
				for _, fileName := range fileNames {
					w := s.fileWriterMap[fileName]
					w.Flush()
				}
				s.mu.Unlock()
				return stream.SendAndClose(&pb.WriteLinesResponse{})
			} else {
				log.L().Error("receive loop met error", zap.Error(err))
				s.mu.Lock()
				for _, fileName := range fileNames {
					delete(s.fileWriterMap, fileName)
				}
				s.mu.Unlock()
				return err
			}
		}
	}
}
