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
	dataCh           chan interface{}
}

func NewContext() *Context {
	return &Context{
		executorChangeCh: make(chan *ExecutorChangeEvent, 1024),
		dataCh:           make(chan interface{}, 128),
	}
}

func (c *Context) ExecutorChange() <-chan *ExecutorChangeEvent {
	return c.executorChangeCh
}

func (c *Context) NotifyExecutorChange(event *ExecutorChangeEvent) {
	c.executorChangeCh <- event
}

func (c *Context) SendRecord(data interface{}) {
	c.dataCh <- data
}

func (c *Context) RecvRecord() interface{} {
	data := <-c.dataCh
	return data
}
