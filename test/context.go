package test

import "time"

type ExecutorChangeType int

const (
	Delete ExecutorChangeType = iota
)

type ExecutorChangeEvent struct {
	Tp   ExecutorChangeType
	Time time.Time
}

type Context struct {
	executorChangeCh chan *ExecutorChangeEvent
}

func NewContext() *Context {
	return &Context{make(chan *ExecutorChangeEvent, 1024)}
}

func (c *Context) ExcutorChange() <-chan *ExecutorChangeEvent {
	return c.executorChangeCh
}

func (c *Context) NotifyExecutorChange(event *ExecutorChangeEvent) {
	c.executorChangeCh <- event
}
