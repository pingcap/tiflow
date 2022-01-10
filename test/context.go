package test

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pkg/metadata"
)

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
	metaKV           metadata.MetaKV
}

func NewContext() *Context {
	return &Context{
		executorChangeCh: make(chan *ExecutorChangeEvent, 1024),
		dataCh:           make(chan interface{}, 128),
	}
}

func (c *Context) SetMetaKV(kv metadata.MetaKV) {
	c.metaKV = kv
}

func (c *Context) GetMetaKV() metadata.MetaKV {
	if c.metaKV == nil {
		c.metaKV = metadata.NewMetaMock()
	}
	return c.metaKV
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

func (c *Context) RecvRecord(ctx context.Context) interface{} {
	select {
	case data := <-c.dataCh:
		return data
	case <-ctx.Done():
		return nil
	}
}

func (c *Context) TryRecvRecord() interface{} {
	select {
	case data := <-c.dataCh:
		return data
	default:
		return nil
	}
}
