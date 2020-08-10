package framework

import (
	"context"
	"database/sql"
	"go.uber.org/zap"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
)

type Task interface {
	Name() string
	GetCDCProfile() *CDCProfile
	Prepare(taskContext *TaskContext) error
	Run(taskContext *TaskContext) error
}

type TaskContext struct {
	Upstream     *sql.DB
	Downstream   *sql.DB
	env          Environment
	waitForReady func() error
	Ctx          context.Context
}

type CDCProfile struct {
	PDUri   string
	SinkUri string
	Opts    map[string]string
}

func (c *TaskContext) CreateDB(name string) error {
	log.Debug("Creating database in upstream", zap.String("db", name))
	_, err := c.Upstream.ExecContext(c.Ctx, "create database "+name)
	if err != nil {
		log.Warn("Failed to create database in upstream", zap.String("db", name), zap.Error(err))
		return err
	}
	log.Debug("Successfully created database in upstream", zap.String("db", name))

	log.Debug("Creating database in downstream", zap.String("db", name))
	_, err = c.Downstream.ExecContext(c.Ctx, "create database "+name)
	if err != nil {
		log.Warn("Failed to create database in downstream", zap.String("db", name), zap.Error(err))
		return err
	}
	log.Debug("Successfully created database in downstream", zap.String("db", name))

	return nil
}

func (c *TaskContext) SqlHelper() *SqlHelper {
	return &SqlHelper{
		upstream:   c.Upstream,
		downstream: c.Downstream,
		ctx: c.Ctx,
	}
}

func (p *CDCProfile) String() string {
	builder := strings.Builder{}
	builder.WriteString("cli changefeed create ")
	if p.PDUri == "" {
		p.PDUri = "http://127.0.0.1:2379"
	}

	builder.WriteString("--pd=" + p.PDUri + " ")

	if p.SinkUri == "" {
		log.Fatal("SinkUri cannot be empty!")
	}

	builder.WriteString("--sink-uri=" + p.SinkUri + " ")

	if p.Opts == nil || len(p.Opts) == 0 {
		return builder.String()
	}

	for k, v := range p.Opts {
		builder.WriteString("--opts=\"" + k + "=" + v + "\" ")
	}
	return builder.String()
}
