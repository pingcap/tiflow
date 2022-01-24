package demo

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	SrcHost string `json:"srcHost"`
	SrcDir  string `json:"srcDir"`
	DstHost string `json:"dstHost"`
	DstDir  string `json:"dstDir"`
}

type errorInfo struct {
	info string
}

func (e *errorInfo) Error() string {
	return e.info
}

type CVSJobMaster struct {
	*lib.BaseMaster
	syncInfo      Config
	syncFilesInfo map[string]lib.WorkerHandle
	counter       int64
}

func NewCVSJobMaster(conf Config) lib.MasterImpl {
	jm := &CVSJobMaster{}
	jm.Impl = jm
	jm.syncInfo = conf
	jm.syncFilesInfo = make(map[string]lib.WorkerHandle)
	return jm
}

func (jm *CVSJobMaster) InitImpl(ctx context.Context) error {
	if jm.syncInfo.DstHost == jm.syncInfo.SrcHost && jm.syncInfo.SrcDir == jm.syncInfo.DstDir {
		return &errorInfo{info: "bad configure file ,make sure the source address is not the same as the destination"}
	}
	fileNames, err := jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	filesNum := len(fileNames)
	if filesNum == 0 {
		log.L().Info("no file found under the folder ", zap.Any("message", jm.syncInfo.DstDir))
	}
	for _, file := range fileNames {
		dstDir := jm.syncInfo.DstDir + "/" + file
		srcDir := jm.syncInfo.SrcDir + "/" + file
		conf := Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir}
		bytes, err := json.Marshal(conf)
		if err != nil {
		}
		// todo:createworker should return worker id
		_, err = jm.CreateWorker(2, bytes, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case
		}
	}
	return nil
}

func (jm *CVSJobMaster) Tick(ctx context.Context) error {
	for file, worker := range jm.syncFilesInfo {
		status := worker.Status()
		if status.Code == lib.WorkerStatusNormal {
			num, ok := status.Ext.(int64)
			if ok {
				jm.counter += num
				// todo : store the sync progress into the meta store for each file
			}
		} else {
			// todo : handle error case here
			log.L().Info("sync file failed ", zap.Any("message", file))
		}
	}
	return nil
}

func (jm *CVSJobMaster) OnMasterRecovered(ctx context.Context) error {
	return nil
}

func (jm *CVSJobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	return nil
}

func (jm *CVSJobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	// todo : add the worker information to the sync files map
	return nil
}

func (jm *CVSJobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	// worker.ID()
	return nil
}

func (jm *CVSJobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl is called when the master is being closed
func (jm *CVSJobMaster) CloseImpl(ctx context.Context) error {
	return nil
}

func (jm *CVSJobMaster) listSrcFiles(ctx context.Context) ([]string, error) {
	conn, err := grpc.Dial(jm.syncInfo.SrcHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the host  ", zap.Any("message", jm.syncInfo.SrcHost))
		return []string{}, err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reply, err := client.ListFiles(ctx, &pb.ListFilesReq{FolderName: jm.syncInfo.SrcDir})
	if err != nil {
		log.L().Info(" list the directory failed ", zap.Any("message", jm.syncInfo.SrcDir))
		return []string{}, err
	}
	//	fmt.Printf("the files name are %v", reply.String())
	return reply.GetFileNames(), nil
}
