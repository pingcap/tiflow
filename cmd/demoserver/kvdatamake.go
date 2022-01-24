package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	FILENUM     = 50
	RECORDERNUM = 10000
	FLUSHLEN    = 10
	PORT        = "127.0.0.1:1234"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var err error

	fmt.Println("Listening the client call ..")

	go DataService(ctx)
	fmt.Printf("type q if expected to quit .\n")
	fmt.Printf("create the files in the format : d dir [100000]\n")
	for {
		var cmd string
		reader := bufio.NewReader(os.Stdin)
		cmd, err = reader.ReadString('\n')
		if err != nil {
			return
		}
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		strs := strings.Fields(cmd)
		switch strs[0] {
		case "q":
			cancel()
			return
		case "d":

			if len(strs) >= 2 {
				folder := strs[1]
				recorderNum := RECORDERNUM
				if len(strs) > 2 {
					recorderNum, err = strconv.Atoi(os.Args[2])
					if err != nil {
						fmt.Printf("the third parameter should be an interger,the format is : d dir [100000]\n")
					}
				}
				go generateData(folder, recorderNum)
			} else {
				fmt.Printf("the format is : d dir [100000]\n")
			}
		default:

		}

	}
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
		fmt.Printf("the folder %s doesn't exist ", folderName)
		err = os.Mkdir(folderName, os.ModePerm)
		if err != nil {
			fmt.Printf("create the folder failed %v", folderName)
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
	var fileNum int
	for fileNum = r.Intn(FILENUM); fileNum == 0; {
		fileNum = r.Intn(FILENUM)
	}
	fileWriterMap := make(map[int]*bufio.Writer)
	for i := 0; i < fileNum; i++ {
		fileName := folderName + "/" + strconv.Itoa(i) + ".txt"
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			// if create file failed ,just ingore this file
			fileNum = fileNum - 1
			continue
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		fileWriterMap[i] = writer
	}
	shouldFlush := false
	for k := 0; (k < recorders) && fileNum > 0; k++ {
		if (k % FLUSHLEN) == 0 {
			shouldFlush = true
		}
		index := k % fileNum
		_, err2 := fileWriterMap[index].WriteString(strconv.Itoa(k) + "," + "value \r\n")
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
	fmt.Printf("%v files have been created \n", fileNum)
	return fileNum
}

func DataService(ctx context.Context) {
	grpcServer := grpc.NewServer()
	pb.RegisterDataRWServiceServer(grpcServer, NewDataRWServer(ctx))
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.L().Panic("listen the port failed",
			zap.String("error:", err.Error()))
	}
	err = grpcServer.Serve(lis)
	if err != nil {
		log.L().Panic("init the server failed",
			zap.String("error:", err.Error()))
	}
}

type DataRWServer struct {
	ctx           context.Context
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
	// fmt.Printf("receive the list file call %s", folder.String())
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

func (s *DataRWServer) ReadLines(req *pb.ReadLinesRequest, stream pb.DataRWService_ReadLinesServer) error {
	fileName := req.FileName
	log.L().Info("receive the request for reading file ")
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0o666)
	if err != nil {
		log.L().Info("make sure the file exist ",
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
			reply, err := reader.ReadString('\n')
			if err == io.EOF {
				log.L().Info("reach the end of the file ")
				break
			}
			if i < int(req.LineNo) {
				continue
			}
			reply = strings.TrimSpace(reply)
			if reply == "" {
				continue
			}
			err = stream.Send(&pb.ReadLinesResponse{Linestr: reply})
			i++
			if err != nil {
				return err
			}
		}
	}
}

func (s *DataRWServer) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	count := 0
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
				writer, ok := s.fileWriterMap[fileName]
				if !ok {
					var file *os.File
					_, err = os.Stat(fileName)
					if err == nil {
						file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0o666)
					} else {
						index := strings.LastIndex(fileName, "/")
						if index <= 1 {
							log.L().Info("bad file name ",
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
								return &ErrorInfo{info: "create the folder " + folder + " failed"}
							}
							fmt.Println(" append .")
							log.L().Info("create the folder ",
								zap.String("folder  ", folder))
						}
						// create the file
						file, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
					}
					defer file.Close()
					if err != nil {
						return &ErrorInfo{info: "create file " + fileName + " failed"}
					}
					writer = bufio.NewWriter(file)
					defer writer.Flush()
					s.fileWriterMap[fileName] = writer
				}
				_, err = writer.WriteString(res.Key + "," + strings.TrimSpace(res.Value) + "\n")
				count++

				if (count % FLUSHLEN) == 0 {
					err = writer.Flush()
				}
				return err
			} else if err == io.EOF {
				for _, w := range s.fileWriterMap {
					w.Flush()
				}
				return stream.SendAndClose(&pb.WriteLinesResponse{})
			}

		}
	}
}
