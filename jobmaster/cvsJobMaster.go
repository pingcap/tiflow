package demo

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
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
	Index   int64  `json:"index"`
}

type workerInfo struct {
	file   string
	curLoc int64
	handle lib.WorkerHandle
}

type errorInfo struct {
	info string
}

func (e *errorInfo) Error() string {
	return e.info
}

type CVSJobMaster struct {
	*lib.BaseMaster
	syncInfo      *Config
	syncFilesInfo map[lib.WorkerID]*workerInfo
	counter       int64
	workerID      lib.WorkerID
}

func init() {
	constructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) lib.Worker {
		return NewCVSJobMaster(ctx, id, masterID, config)
	}
	factory := registry.NewSimpleWorkerFactory(constructor, &Config{})
	registry.NewRegistry().MustRegisterWorkerType(lib.CvsJobMaster, factory)
}

func NewCVSJobMaster(ctx *dcontext.Context, _workerID lib.WorkerID, masterID lib.MasterID, conf lib.WorkerConfig) *CVSJobMaster {
	jm := &CVSJobMaster{}
	jm.workerID = _workerID
	jm.syncInfo = conf.(*Config)
	jm.syncFilesInfo = make(map[lib.WorkerID]*workerInfo)
	deps := ctx.Dependencies
	base := lib.NewBaseMaster(
		jm,
		masterID,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		deps.ExecutorClientManager,
		deps.ServerMasterClient)
	jm.BaseMaster = base
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
	// todo: store the jobmaster information into the metastore
	for _, file := range fileNames {
		dstDir := jm.syncInfo.DstDir + "/" + file
		srcDir := jm.syncInfo.SrcDir + "/" + file
		conf := Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir, Index: 0}
		bytes, err := json.Marshal(conf)
		if err != nil {
		}
		// todo:createworker should return worker id
		workerID, err := jm.CreateWorker(2, bytes, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case
		}
		jm.syncFilesInfo[workerID] = &workerInfo{file: file, curLoc: 0, handle: nil}
	}
	return nil
}

func (jm *CVSJobMaster) Tick(ctx context.Context) error {
	for _, worker := range jm.syncFilesInfo {
		if worker.handle == nil {
			continue
		}
		status := worker.handle.Status()
		if status.Code == lib.WorkerStatusNormal {
			num, ok := status.Ext.(int64)
			if ok {
				worker.curLoc = num
				jm.counter += num
				// todo : store the sync progress into the meta store for each file
			}
		} else {
			// todo : handle error case here
			log.L().Info("sync file failed ", zap.Any("message", worker.file))
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
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	if !exist {
		log.L().Info("bad worker found", zap.Any("message", worker.ID()))
		panic(errorInfo{info: "bad worker "})
	}
	syncInfo.handle = worker
	return nil
}

func (jm *CVSJobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	if !exist {
		log.L().Info("bad worker found", zap.Any("message", worker.ID()))
	}
	var err error
	dstDir := jm.syncInfo.DstDir + "/" + syncInfo.file
	srcDir := jm.syncInfo.SrcDir + "/" + syncInfo.file
	conf := Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir, Index: syncInfo.curLoc}
	bytes, err := json.Marshal(conf)
	if err != nil {
		log.L().Info("error happened when getting json from the configure", zap.Any("configure:", conf))
	}
	workerID, err := jm.CreateWorker(2, bytes, 10)
	if err != nil {
		log.L().Info("create worker failed ", zap.String(" information :", err.Error()))
	}
	delete(jm.syncFilesInfo, worker.ID())
	// todo : if the worker id is empty ,the sync file will be lost.
	jm.syncFilesInfo[workerID] = &workerInfo{file: syncInfo.file, curLoc: syncInfo.curLoc, handle: nil}
	return err
}

func (jm *CVSJobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl is called when the master is being closed
func (jm *CVSJobMaster) CloseImpl(ctx context.Context) error {
	return nil
}

func (jm *CVSJobMaster) WorkerID() lib.WorkerID {
	return jm.workerID
}

func (jm *CVSJobMaster) Workload() model.RescUnit {
	return 2
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
