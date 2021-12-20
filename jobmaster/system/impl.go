package system

import (
	"context"
	stdErrors "errors"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Master implements the master of benchmark workload.
type Master struct {
	ctx    context.Context
	cancel func()

	id      model.ID
	clients *client.Manager

	offExecutors chan model.ExecutorID

	mu           sync.Mutex
	execTasks    map[model.ExecutorID][]*model.Task
	runningTasks map[model.ID]*Task

	scheduleWaitingTasks chan scheduleGroup
	// rate limit for rescheduling when error happens
	scheduleRateLimit *rate.Limiter
}

// New creates a master instance
func New(
	parentCtx context.Context,
	id model.ID,
	clients *client.Manager,
) *Master {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Master{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		clients: clients,

		offExecutors:         make(chan model.ExecutorID, 100),
		scheduleWaitingTasks: make(chan scheduleGroup, 1024),
		scheduleRateLimit:    rate.NewLimiter(rate.Every(time.Second), 1),

		execTasks:    make(map[model.ExecutorID][]*model.Task),
		runningTasks: make(map[model.ID]*Task),
	}
}

func (m *Master) Cancel() {
	m.cancel()
}

// scheduleGroup is the min unit of scheduler, and the tasks in the same group have to be scheduled in the same node.
type scheduleGroup []*Task

// TaskStatus represents the current status of the task.
type TaskStatus int32

const (
	// Running means the task is running.
	Running TaskStatus = iota
	// Stopped means the task has been stopped by any means.
	Stopped
	// Finished means the task has finished its job.
	Finished
)

// Task is the container of a dispatched tasks,  and records its status.
type Task struct {
	*model.Task

	exec   model.ExecutorID
	status TaskStatus
}

// ID implements JobMaster interface.
func (m *Master) ID() model.ID {
	return m.id
}

// master dispatches a set of task.
func (m *Master) dispatch(ctx context.Context, tasks []*Task) error {
	arrangement := make(map[model.ExecutorID][]*model.Task)
	for _, task := range tasks {
		subjob, ok := arrangement[task.exec]
		if !ok {
			arrangement[task.exec] = []*model.Task{task.Task}
		} else {
			subjob = append(subjob, task.Task)
			arrangement[task.exec] = subjob
		}
	}

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
			log.L().Logger.Info("Send meet error", zap.Error(err))
			return err
		}
		respPb := resp.Resp.(*pb.SubmitBatchTasksResponse)
		if respPb.Err != nil {
			stdErr := stdErrors.New(respPb.Err.GetMessage())
			return errors.Wrap(errors.ErrSubJobFailed, stdErr, execID, m.ID())
		}
	}

	// apply the new arrangement.
	m.mu.Lock()
	for eid, taskList := range arrangement {
		originTasks, ok := m.execTasks[eid]
		if ok {
			originTasks = append(originTasks, taskList...)
			m.execTasks[eid] = originTasks
		} else {
			m.execTasks[eid] = taskList
		}
	}
	for _, t := range tasks {
		m.runningTasks[t.ID] = t
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) reScheduleTask(group scheduleGroup) error {
	reqTasks := make([]*pb.ScheduleTask, 0, len(group))
	for _, task := range group {
		reqTasks = append(reqTasks, task.ToScheduleTaskPB())
	}
	req := &pb.TaskSchedulerRequest{Tasks: reqTasks}
	resp, err := m.clients.MasterClient().RequestForSchedule(m.ctx, req, time.Minute)
	if err != nil {
		// TODO: convert grpc error to rfc error
		return err
	}
	for _, task := range group {
		schedule, ok := resp.GetSchedule()[int64(task.ID)]
		if !ok {
			return errors.ErrMasterScheduleMissTask.GenWithStackByArgs(task.ID)
		}
		task.exec = model.ExecutorID(schedule.GetExecutorId())
		err := m.clients.AddExecutor(task.exec, schedule.Addr)
		if err != nil {
			return err
		}
	}
	err = m.dispatch(m.ctx, group)
	return err
}

func (m *Master) scheduleJobImpl(ctx context.Context, tasks []*model.Task) error {
	reqTasks := make([]*pb.ScheduleTask, 0, len(tasks))
	for _, task := range tasks {
		reqTasks = append(reqTasks, task.ToScheduleTaskPB())
	}
	req := &pb.TaskSchedulerRequest{Tasks: reqTasks}
	resp, err := m.clients.MasterClient().RequestForSchedule(m.ctx, req, time.Minute)
	if err != nil {
		// TODO: convert grpc error to rfc error
		return err
	}
	sysTasks := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		schedule, ok := resp.GetSchedule()[int64(task.ID)]
		if !ok {
			return errors.ErrMasterScheduleMissTask.GenWithStackByArgs(task.ID)
		}
		sysTasks = append(sysTasks, &Task{
			Task: task,
			exec: model.ExecutorID(schedule.GetExecutorId()),
		})
		err := m.clients.AddExecutor(model.ExecutorID(schedule.GetExecutorId()), schedule.Addr)
		if err != nil {
			return err
		}
	}

	err = m.dispatch(ctx, sysTasks)
	return err
}

// DispatchJob implements JobMaster interface.
func (m *Master) DispatchTasks(ctx context.Context, tasks []*model.Task) error {
	retry := 1
	for i := 1; i <= retry; i++ {
		if err := m.scheduleJobImpl(ctx, tasks); err == nil {
			return nil
		} else if i == retry {
			return err
		}
		// sleep for a while to backoff
	}
	return nil
}

func (m *Master) StopTasks(ctx context.Context, tasks []*model.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	arrange := make(map[model.ExecutorID][]int64)
	for _, task := range tasks {
		runningTask := m.runningTasks[task.ID]
		li, ok := arrange[runningTask.exec]
		if !ok {
			arrange[runningTask.exec] = []int64{int64(task.ID)}
		} else {
			li = append(li, int64(task.ID))
			arrange[runningTask.exec] = li
		}
	}
	var retErr error
	for exec, taskList := range arrange {
		req := &pb.CancelBatchTasksRequest{
			TaskIdList: taskList,
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
func (m *Master) StartInternal() {
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
	// TODO: keep the goroutines alive.
	go m.monitorExecutorOffline()
	go m.monitorSchedulingTasks()
}

func (m *Master) monitorSchedulingTasks() {
	for {
		select {
		case group := <-m.scheduleWaitingTasks:
			//for _, t := range group {
			//	curT := m.runningTasks[t.ID]
			//	if curT.exec != t.exec {
			//		// this task has been scheduled away.
			//		log.L().Logger.Info("cur task exec id is not same as reschedule one", zap.Int32("cur id", int32(curT.exec)), zap.Int32("id", int32(t.exec)))
			//		continue
			//	}
			//}

			//if t.status == Running {
			// cancel it
			//}

			log.L().Logger.Info("begin to reschedule task group", zap.Any("group", group))
			if err := m.reScheduleTask(group); err != nil {
				log.L().Logger.Error("cant reschedule task", zap.Error(err))

				// Use a global rate limit for task rescheduling
				delay := m.scheduleRateLimit.Reserve().Delay()
				if delay != 0 {
					log.L().Logger.Warn("reschedule task rate limit", zap.Duration("delay", delay))
					timer := time.NewTimer(delay)
					select {
					case <-m.ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
						timer.Stop()
					}
				}
				// FIXME: this could cause deadlock problem if scheduleWaitingTasks channel is full
				m.scheduleWaitingTasks <- group
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// OfflineExecutor implements JobMaster interface.
func (m *Master) OfflineExecutor(id model.ExecutorID) {
	m.offExecutors <- id
	log.L().Logger.Info("executor is offlined", zap.String("eid", string(id)))
}

func (m *Master) monitorExecutorOffline() {
	for {
		select {
		case execID := <-m.offExecutors:
			log.L().Logger.Info("executor is offlined", zap.String("eid", string(execID)))
			m.mu.Lock()
			taskList, ok := m.execTasks[execID]
			if !ok {
				m.mu.Unlock()
				log.L().Logger.Info("executor has been removed, nothing todo", zap.String("id", string(execID)))
				continue
			}
			delete(m.execTasks, execID)
			m.mu.Unlock()

			var group scheduleGroup
			for _, task := range taskList {
				t, ok := m.runningTasks[task.ID]
				if !ok || t.exec != execID {
					log.L().Logger.Error("running task is not consistant with executor-task map")
					continue
				}
				t.status = Finished
				group = append(group, t)
			}
			m.scheduleWaitingTasks <- group
		case <-m.ctx.Done():
			return
		}
	}
}
