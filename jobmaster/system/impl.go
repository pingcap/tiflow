package system

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"strconv"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/pingcap/tiflow/dm/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func arrangeTasksByExecID(tasks []*model.Task) map[model.ExecutorID]scheduleGroup {
	arrangement := make(map[model.ExecutorID]scheduleGroup)
	for _, task := range tasks {
		subjob, ok := arrangement[task.Exec]
		if !ok {
			arrangement[task.Exec] = []*model.Task{task}
		} else {
			subjob = append(subjob, task)
			arrangement[task.Exec] = subjob
		}
	}
	return arrangement
}

// Master implements the master of benchmark workload.
type Master struct {
	ctx    context.Context
	cancel func()

	id      model.ID
	clients *client.Manager
	MetaKV  metadata.MetaKV

	runningTasks sync.Map

	scheduleWaitingTasks chan scheduleGroup
	// rate limit for rescheduling when error happens
	scheduleRateLimit *rate.Limiter
}

type TaskStatus int

const (
	Serving TaskStatus = iota
	Scheduling
	Pauseed
	Stopped
)

type Task struct {
	sync.Mutex
	*model.Task
	currentStatus TaskStatus
	targetStatus  TaskStatus
}

func (t *Task) setCurStatus(status TaskStatus) {
	t.Lock()
	t.currentStatus = status
	t.Unlock()
}

func (t *Task) setTargetStatus(status TaskStatus) {
	t.Lock()
	t.targetStatus = status
	t.Unlock()
}

// New creates a master instance
func New(
	id model.ID,
	clients *client.Manager,
) *Master {
	return &Master{
		id:      id,
		clients: clients,

		scheduleWaitingTasks: make(chan scheduleGroup, 1024),
		scheduleRateLimit:    rate.NewLimiter(rate.Every(time.Second), 1),
	}
}

func (m *Master) Cancel() {
	if m.cancel != nil {
		m.cancel()
	}
}

// scheduleGroup is the min unit of scheduler, and the tasks in the same group have to be scheduled in the same node.
type scheduleGroup []*model.Task

func (s scheduleGroup) toIDListPB() []int64 {
	ids := make([]int64, 0, len(s))
	for _, t := range s {
		ids = append(ids, int64(t.ID))
	}
	return ids
}

// ID implements JobMaster interface.
func (m *Master) ID() model.ID {
	return m.id
}

func (m *Master) RestoreTask(ctx context.Context, task *model.Task) error {
	res, err := m.MetaKV.Get(ctx, adapter.TaskKeyAdapter.Encode(strconv.Itoa(int(task.ID))), clientv3.WithLimit(1))
	// TODO: If we cant communicate with etcd, shall we wait the task timeout and dispatch this task again?
	if err != nil {
		return err
	}
	result := res.(*clientv3.GetResponse)
	if len(result.Kvs) == 0 {
		m.DispatchTasks(task)
		return nil
	}
	taskInfo := new(model.Task)
	err = json.Unmarshal(result.Kvs[0].Value, taskInfo)
	if err != nil {
		return err
	}
	m.runningTasks.Store(taskInfo.ID, taskInfo)
	// TODO: wait task to report heartbeat.
	return nil
}

func (m *Master) updateEtcd(ctx context.Context, tasks []*model.Task) error {
	txn := m.MetaKV.Txn(ctx).(clientv3.Txn)
	actions := make([]clientv3.Op, 0, len(tasks))
	for _, task := range tasks {
		taskKey := adapter.TaskKeyAdapter.Encode(strconv.Itoa(int(task.ID)))
		taskValue, err := json.Marshal(task)
		if err != nil {
			return err
		}
		actions = append(actions, clientv3.OpPut(taskKey, string(taskValue)))
	}
	txn.Then(actions...)
	_, err := txn.Commit()
	return err
}

// master dispatches a set of task.
func (m *Master) dispatch(ctx context.Context, tasks []*model.Task) error {
	// update etcd
	if err := m.updateEtcd(ctx, tasks); err != nil {
		return err
	}
	arrangement := arrangeTasksByExecID(tasks)
	var errTasks scheduleGroup

	for execID, taskList := range arrangement {
		// construct sub job
		reqPb := &pb.SubmitBatchTasksRequest{}
		for _, t := range taskList {
			reqPb.Tasks = append(reqPb.Tasks, t.ToPB())
		}
		log.L().Logger.Info("submit sub job", zap.String("exec id", string(execID)), zap.String("req pb", reqPb.String()))
		request := &client.ExecutorRequest{
			Cmd: client.CmdSubmitBatchTasks,
			Req: reqPb,
		}
		resp, err := m.clients.ExecutorClient(execID).Send(ctx, request)
		if err != nil {
			log.L().Logger.Info("Dispatch task meet error", zap.Error(err))
			errTasks = append(errTasks, taskList...)
			continue
		}
		respPb := resp.Resp.(*pb.SubmitBatchTasksResponse)
		if respPb.Err != nil {
			log.L().Logger.Info("Dispatch task meet error", zap.String("err", respPb.Err.Message))
			errTasks = append(errTasks, taskList...)
			continue
		}
		for _, t := range taskList {
			if value, ok := m.runningTasks.Load(t.ID); ok {
				task := value.(*Task)
				task.setCurStatus(Serving)
			} else {
				task := &Task{
					Task:          t,
					currentStatus: Serving,
					targetStatus:  Serving,
				}
				m.runningTasks.Store(t.ID, task)
			}
		}
	}

	m.addScheduleTasks(errTasks)

	return nil
}

func (m *Master) scheduleTask(group scheduleGroup) error {
	reqTasks := make([]*pb.ScheduleTask, 0, len(group))
	for _, task := range group {
		reqTasks = append(reqTasks, task.ToScheduleTaskPB())
	}
	req := &pb.TaskSchedulerRequest{Tasks: reqTasks}
	resp, err := m.clients.MasterClient().ScheduleTask(m.ctx, req, time.Minute)
	if err != nil {
		// TODO: convert grpc error to rfc error
		return err
	}
	for _, task := range group {
		schedule, ok := resp.GetSchedule()[int64(task.ID)]
		if !ok {
			return errors.ErrMasterScheduleMissTask.GenWithStackByArgs(task.ID)
		}
		task.Exec = model.ExecutorID(schedule.GetExecutorId())
		err := m.clients.AddExecutor(task.Exec, schedule.Addr)
		if err != nil {
			return err
		}
	}
	err = m.dispatch(m.ctx, group)
	return err
}

// DispatchJob implements JobMaster interface.
func (m *Master) DispatchTasks(tasks ...*model.Task) {
	m.addScheduleTasks(tasks)
}

func (m *Master) AsyncPauseTasks(tasks ...*model.Task) error {
	for _, t := range tasks {
		_, ok := m.runningTasks.Load(t.ID)
		if !ok {
			return errors.ErrTaskNotFound.FastGenByArgs(t.ID)
		}
	}

	for _, t := range tasks {
		value, _ := m.runningTasks.Load(t.ID)
		value.(*Task).setTargetStatus(Pauseed)
	}
	return nil
}

func (m *Master) StopTasks(ctx context.Context, tasks []*model.Task) error {
	arrange := arrangeTasksByExecID(tasks)
	var retErr error
	for exec, taskList := range arrange {
		req := &pb.CancelBatchTasksRequest{
			TaskIdList: taskList.toIDListPB(),
		}
		log.L().Info("begin to cancel tasks", zap.String("exec", string(exec)), zap.Any("task", taskList))
		resp, err := m.clients.ExecutorClient(exec).Send(ctx, &client.ExecutorRequest{
			Cmd: client.CmdCancelBatchTasks,
			Req: req,
		})
		if err != nil {
			retErr = err
		} else {
			respErr := resp.Resp.(*pb.CancelBatchTasksResponse).Err
			if respErr != nil {
				retErr = stdErrors.New(respErr.Message)
			}
		}
	}
	return retErr
}

// Listen the events from every tasks
func (m *Master) StartInternal(parentCtx context.Context) {
	m.ctx, m.cancel = context.WithCancel(parentCtx)
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
	go m.monitorRunningTasks()
	// TODO: keep the goroutines alive.
	go m.monitorSchedulingTasks()
}

func (m *Master) addScheduleTasks(group scheduleGroup) {
	if len(group) == 0 {
		return
	}
	for _, t := range group {
		if value, ok := m.runningTasks.Load(t.ID); ok {
			task := value.(*Task)
			task.setCurStatus(Scheduling)
		} else {
			task := &Task{
				Task:          t,
				currentStatus: Scheduling,
				targetStatus:  Serving,
			}
			m.runningTasks.Store(t.ID, task)
		}
	}

	go func() {
		m.scheduleWaitingTasks <- group
	}()
}

func (m *Master) monitorRunningTasks() {
	timer := time.NewTicker(time.Second * 1)
	defer timer.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			m.checkRunningTasks()
		}
	}
}

func (m *Master) checkRunningTasks() {
	tasksToPause := make([]*model.Task, 0)
	m.runningTasks.Range(func(_, value interface{}) bool {
		t := value.(*Task)
		t.Lock()
		if t.targetStatus == Pauseed && t.currentStatus == Serving {
			log.L().Logger.Info("plan to pause", zap.Int32("id", int32(t.ID)))
			tasksToPause = append(tasksToPause, t.Task)
		}
		t.Unlock()
		return true
	})
	m.pauseTaskImpl(tasksToPause)
}

func (m *Master) pauseTaskImpl(tasks []*model.Task) {
	arrangement := arrangeTasksByExecID(tasks)
	for execID, tasks := range arrangement {
		req := &pb.PauseBatchTasksRequest{
			TaskIdList: tasks.toIDListPB(),
		}
		resp, err := m.clients.ExecutorClient(execID).Send(m.ctx, &client.ExecutorRequest{
			Cmd: client.CmdPauseBatchTasks,
			Req: req,
		})
		if err != nil {
			log.L().Logger.Info("pause task failed", zap.Error(err))
			continue
		}
		if err := resp.Resp.(*pb.PauseBatchTasksResponse).Err; err != nil {
			log.L().Logger.Info("pause task failed", zap.String("error", err.Message))
			continue
		}
		for _, t := range tasks {
			value, ok := m.runningTasks.Load(t.ID)
			if ok {
				value.(*Task).setCurStatus(Pauseed)
			}
		}
	}
}

func (m *Master) monitorSchedulingTasks() {
	for {
		select {
		case group := <-m.scheduleWaitingTasks:
			log.L().Logger.Info("begin to schedule task group", zap.Any("group", group))
			if err := m.scheduleTask(group); err != nil {
				log.L().Logger.Error("cant schedule task", zap.Error(err))

				// Use a global rate limit for task rescheduling
				delay := m.scheduleRateLimit.Reserve().Delay()
				if delay != 0 {
					log.L().Logger.Warn("schedule task rate limit", zap.Duration("delay", delay))
					timer := time.NewTimer(delay)
					select {
					case <-m.ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
						timer.Stop()
					}
				}
				m.addScheduleTasks(group)
			}
		case <-m.ctx.Done():
			return
		}
	}
}
