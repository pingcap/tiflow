package cvs

import (
	"context"
	"fmt"
	"strconv"

	cvsTask "github.com/hanfei1991/microcosm/executor/cvsTask"
	"github.com/hanfei1991/microcosm/executor/worker"
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
	SrcHost string `toml:"srcHost" json:"srcHost"`
	SrcDir  string `toml:"srcDir" json:"srcDir"`
	DstHost string `toml:"dstHost" json:"dstHost"`
	DstDir  string `toml:"dstDir" json:"dstDir"`
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

type JobMaster struct {
	lib.BaseJobMaster
	syncInfo      *Config
	syncFilesInfo map[lib.WorkerID]*workerInfo
	counter       int64
	workerID      lib.WorkerID
}

func RegisterWorker() {
	constructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) lib.Worker {
		return NewCVSJobMaster(ctx, id, masterID, config)
	}
	factory := registry.NewSimpleWorkerFactory(constructor, &Config{})
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.CvsJobMaster, factory)
}

func NewCVSJobMaster(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID, conf lib.WorkerConfig) *JobMaster {
	jm := &JobMaster{}
	jm.workerID = workerID
	jm.syncInfo = conf.(*Config)
	jm.syncFilesInfo = make(map[lib.WorkerID]*workerInfo)
	deps := ctx.Dependencies

	base := lib.NewBaseJobMaster(
		ctx,
		jm,
		masterID,
		workerID,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		deps.ExecutorClientManager,
		deps.ServerMasterClient,
	)
	jm.BaseJobMaster = base
	log.L().Info("new cvs jobmaster ", zap.Any("id :", jm.workerID))
	return jm
}

func (jm *JobMaster) InitImpl(ctx context.Context) error {
	if jm.syncInfo.DstHost == jm.syncInfo.SrcHost && jm.syncInfo.SrcDir == jm.syncInfo.DstDir {
		return &errorInfo{info: "bad configure file ,make sure the source address is not the same as the destination"}
	}
	log.L().Info("initializing the cvs jobmaster  ", zap.Any("id :", jm.workerID))
	fileNames, err := jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	filesNum := len(fileNames)
	if filesNum == 0 {
		log.L().Info("no file found under the folder ", zap.Any("message", jm.syncInfo.DstDir))
	}
	log.L().Info(" cvs jobmaster list file success", zap.Any("id :", jm.workerID), zap.Any(" file number :", filesNum))
	// todo: store the jobmaster information into the metastore
	for _, file := range fileNames {
		dstDir := jm.syncInfo.DstDir + "/" + file
		srcDir := jm.syncInfo.SrcDir + "/" + file
		conf := cvsTask.Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir, StartLoc: 0}
		workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case
			return err
		}
		jm.syncFilesInfo[workerID] = &workerInfo{file: file, curLoc: 0, handle: nil}
	}
	return nil
}

func (jm *JobMaster) Tick(ctx context.Context) error {
	jm.counter = 0
	for _, worker := range jm.syncFilesInfo {
		if worker.handle == nil {
			continue
		}
		status := worker.handle.Status()
		if status.Code == lib.WorkerStatusNormal {
			num, err := strconv.ParseInt(string(status.ExtBytes), 10, 64)
			if err != nil {
				return err
			}
			worker.curLoc = num
			jm.counter += num
			log.L().Debug("cvs job tmp num ", zap.Any("id :", worker.handle.ID()), zap.Int64("counter: ", num))
			// todo : store the sync progress into the meta store for each file
		} else if status.Code == lib.WorkerStatusFinished {
			// todo : handle error case here
			log.L().Info("sync file finished ", zap.Any("message", worker.file))
		} else if status.Code == lib.WorkerStatusError {
			log.L().Error("sync file failed ", zap.Any("message", worker.file))
		} else {
			log.L().Info("worker status abnormal", zap.Any("status", status))
		}
	}
	log.L().Info("cvs job master status  ", zap.Any("id :", jm.workerID), zap.Int64("counter: ", jm.counter))
	return nil
}

func (jm *JobMaster) OnMasterRecovered(ctx context.Context) error {
	return nil
}

func (jm *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	return nil
}

func (jm *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	// todo : add the worker information to the sync files map
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	if !exist {
		log.L().Panic("bad worker found", zap.Any("message", worker.ID()))
	} else {
		log.L().Info("worker online ", zap.Any("fileName", syncInfo.file))
	}
	syncInfo.handle = worker
	return nil
}

func (jm *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	log.L().Info("on worker offline ", zap.Any(" worker :", worker.ID()))
	if !exist {
		log.L().Info("bad worker found", zap.Any("message", worker.ID()))
	}
	var err error
	dstDir := jm.syncInfo.DstDir + "/" + syncInfo.file
	srcDir := jm.syncInfo.SrcDir + "/" + syncInfo.file
	conf := cvsTask.Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir, StartLoc: syncInfo.curLoc}
	workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10)
	if err != nil {
		log.L().Info("create worker failed ", zap.String(" information :", err.Error()))
	}
	delete(jm.syncFilesInfo, worker.ID())
	// todo : if the worker id is empty ,the sync file will be lost.
	jm.syncFilesInfo[workerID] = &workerInfo{file: syncInfo.file, curLoc: syncInfo.curLoc, handle: nil}
	return err
}

func (jm *JobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl is called when the master is being closed
func (jm *JobMaster) CloseImpl(ctx context.Context) error {
	return nil
}

func (jm *JobMaster) ID() worker.RunnableID {
	return jm.workerID
}

func (jm *JobMaster) Workload() model.RescUnit {
	return 2
}

func (jm *JobMaster) OnMasterFailover(reason lib.MasterFailoverReason) error {
	return nil
}

func (jm *JobMaster) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("cvs jobmaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (jm *JobMaster) Status() lib.WorkerStatus {
	return lib.WorkerStatus{
		Code:     lib.WorkerStatusNormal,
		ExtBytes: []byte(fmt.Sprintf("%d", jm.counter)),
	}
}

func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) listSrcFiles(ctx context.Context) ([]string, error) {
	conn, err := grpc.Dial(jm.syncInfo.SrcHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the host  ", zap.Any("message", jm.syncInfo.SrcHost))
		return []string{}, err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reply, err := client.ListFiles(ctx, &pb.ListFilesReq{FolderName: jm.syncInfo.SrcDir})
	if err != nil {
		log.L().Info(" list the directory failed ", zap.String("dir", jm.syncInfo.SrcDir), zap.Error(err))
		return []string{}, err
	}
	return reply.GetFileNames(), nil
}
