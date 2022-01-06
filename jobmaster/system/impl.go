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
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Master implements the master of benchmark workload.
type Master struct {
	ctx    context.Context
	cancel func()

	id      model.ID
	clients *client.Manager
	MetaKV  metadata.MetaKV

	mu           sync.Mutex
	runningTasks map[model.ID]*model.Task

	scheduleWaitingTasks chan scheduleGroup
	// rate limit for rescheduling when error happens
	scheduleRateLimit *rate.Limiter
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

		runningTasks: make(map[model.ID]*model.Task),
	}
}

func (m *Master) Cancel() {
	if m.cancel != nil {
		m.cancel()
	}
}

// scheduleGroup is the min unit of scheduler, and the tasks in the same group have to be scheduled in the same node.
type scheduleGroup []*model.Task

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
	m.mu.Lock()
	m.runningTasks[taskInfo.ID] = taskInfo
	// And wait task to report heartbeat.
	m.mu.Unlock()
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
	arrangement := make(map[model.ExecutorID][]*model.Task)
	var errTasks scheduleGroup
	for _, task := range tasks {
		subjob, ok := arrangement[task.Exec]
		if !ok {
			arrangement[task.Exec] = []*model.Task{task}
		} else {
			subjob = append(subjob, task)
			arrangement[task.Exec] = subjob
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
			log.L().Logger.Info("Dispatch task meet error", zap.Error(err))
			errTasks = append(errTasks, taskList...)
		}
		respPb := resp.Resp.(*pb.SubmitBatchTasksResponse)
		if respPb.Err != nil {
			log.L().Logger.Info("Dispatch task meet error", zap.String("err", respPb.Err.Message))
			errTasks = append(errTasks, taskList...)
		}
	}

	m.addScheduleTasks(errTasks)

	// apply the new arrangement.
	m.mu.Lock()
	for _, t := range tasks {
		m.runningTasks[t.ID] = t
	}
	m.mu.Unlock()
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

func (m *Master) StopTasks(ctx context.Context, tasks []*model.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	arrange := make(map[model.ExecutorID][]int64)
	for _, task := range tasks {
		runningTask := m.runningTasks[task.ID]
		li, ok := arrange[runningTask.Exec]
		if !ok {
			arrange[runningTask.Exec] = []int64{int64(task.ID)}
		} else {
			li = append(li, int64(task.ID))
			arrange[runningTask.Exec] = li
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
func (m *Master) StartInternal(parentCtx context.Context) {
	m.ctx, m.cancel = context.WithCancel(parentCtx)
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
	// TODO: keep the goroutines alive.
	go m.monitorSchedulingTasks()
}

func (m *Master) addScheduleTasks(group scheduleGroup) {
	if len(group) == 0 {
		return
	}
	go func() {
		m.scheduleWaitingTasks <- group
	}()
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
