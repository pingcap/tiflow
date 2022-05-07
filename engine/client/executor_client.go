package client

import (
	"context"
)

type ExecutorClient interface {
	baseExecutorClient

	DispatchTask(
		ctx context.Context,
		args *DispatchTaskArgs,
		startWorkerTimer StartWorkerCallback,
		abortWorker AbortWorkerCallback,
	) error
}

func newExecutorClient(addr string) (ExecutorClient, error) {
	base, err := newBaseExecutorClient(addr)
	if err != nil {
		return nil, err
	}

	taskDispatcher := newTaskDispatcher(base)
	return &executorClientImpl{
		baseExecutorClientImpl: base,
		TaskDispatcher:         taskDispatcher,
	}, nil
}

type executorClientImpl struct {
	*baseExecutorClientImpl
	*TaskDispatcher
}
