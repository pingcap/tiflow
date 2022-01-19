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
	"google.golang.org/grpc"
)

const (
	FILENUM     = 5
	RECORDERNUM = 100
	FLUSHLEN    = 10
	PORT        = "127.0.0.1:1234"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Please run the command in format : demoserver folder [recordernum]")
	} else {
		dataFolder := os.Args[1]
		recorderNum := RECORDERNUM
		var err error
		if len(args) > 2 {
			recorderNum, err = strconv.Atoi(os.Args[2])
		}
		if err != nil {
			fmt.Println("the third parameter should be a interger")
			return
		}
		fmt.Println("Start to generate the data ...")
		generateData(dataFolder, recorderNum)
		fmt.Println("Listening the client call ..")
		DataService()

	}
}

type ErrorInfo struct {
	info string
}

func (e *ErrorInfo) Error() string {
	return e.info
}

func generateData(folderName string, recorders int) int {
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
	return fileNum
}

type DataRWServer struct {
	fileWriterMap map[string]*bufio.Writer
	fileReaderMap map[string]*bufio.Reader
}

func NewDataRWServer() *DataRWServer {
	s := &DataRWServer{
		fileReaderMap: make(map[string]*bufio.Reader),
		fileWriterMap: make(map[string]*bufio.Writer),
	}
	return s
}

func (*DataRWServer) ListFiles(ctx context.Context, folder *pb.ListFilesReq) (*pb.ListFilesResponse, error) {
	// fmt.Printf("receive the list file call %s", folder.String())
	fd := folder.FolderName
	_dir, err := ioutil.ReadDir(fd)
	fmt.Printf("receive the list file call %v \n", fd)
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
	reader, exist := s.fileReaderMap[fileName]
	fmt.Printf("receive the readline request %v \n", fileName)
	if !exist {
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0o666)
		if err != nil {
			fmt.Printf("make sure the file % s exist, the error is %v \n", fileName, err)
			return err
		}
		defer file.Close()
		reader = bufio.NewReader(file)

		s.fileReaderMap[fileName] = reader
		fmt.Printf("create the reader for %v \n", fileName)
	} else {
		fmt.Println("the read already exist ")
	}
	fmt.Printf("enter the funct read line %v", fileName)
	i := 0
	for {
		reply, err := reader.ReadString('\n')
		if err == io.EOF {
			delete(s.fileReaderMap, fileName)
			fmt.Printf("the end of the file %v\n", fileName)
			break
		}
		reply = strings.TrimSpace(reply)
		fmt.Printf("the line no is %v , string is %v\n", i, reply)
		i++
		if reply == "" {
			continue
		}
		err = stream.Send(&pb.ReadLinesResponse{Linestr: reply})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DataRWServer) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	for {
		defer func() {
			for _, writer := range s.fileWriterMap {
				writer.Flush()
			}
		}()
		count := 0
		res, err := stream.Recv()
		if err == nil {
			fileName := res.FileName
			writer, exist := s.fileWriterMap[fileName]
			if !exist {
				fmt.Printf("create the writer %v\n", fileName)
				index := strings.LastIndex(fileName, "/")
				if index <= 1 {
					fmt.Printf("bad file name ,index  %v\n", index)
					return &ErrorInfo{info: " bad file name :" + fileName}
				}
				folder := fileName[0 : index-1]
				_, err := ioutil.ReadDir(folder)
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
					fmt.Printf("create the folder  %v\n", folder)
				}
				// create the file
				file, err1 := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
				if err1 != nil {
					return &ErrorInfo{info: "create file " + fileName + " failed"}
				}
				defer file.Close()
				writer = bufio.NewWriter(file)
				s.fileWriterMap[fileName] = writer
			}
			_, err = writer.WriteString(res.Key + "," + strings.TrimSpace(res.Value) + "\n")
			count++
			if (count % FLUSHLEN) == 0 {
				err = writer.Flush()
			}
		} else if err == io.EOF {
			return stream.SendAndClose(&pb.WriteLinesResponse{})
		}
		if err != nil {
			return err
		}
	}
}

func DataService() {
	grpcServer := grpc.NewServer()
	pb.RegisterDataRWServiceServer(grpcServer, NewDataRWServer())
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println("error happened ")
	}
	err = grpcServer.Serve(lis)
	if err != nil {
		fmt.Printf("error is %f", err)
	}
}
