package framework

import (
	"context"
	"database/sql"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Task interface {
	GetCDCProfile() *CDCProfile
	Run(taskContext *TaskContext) error
}

type TaskContext struct {
	Upstream sql.DB
	Downstream sql.DB
	ctx context.Context
}

type CDCProfile struct {
	PDUri string
	SinkUri string
	Opts map[string]string
}

func (p *CDCProfile) String() string {
	builder := strings.Builder{}
	builder.WriteString("cdc cli changefeed create ")
	if p.PDUri == "" {
		p.PDUri = "http://127.0.0.1:2379"
	}

	builder.WriteString("--pd=" + p.PDUri + " ")

	if p.SinkUri == "" {
		log.Fatal("SinkUri cannot be empty!")
	}

	builder.WriteString("--sink-uri=" + p.SinkUri)

	if p.Opts == nil || len(p.Opts) == 0 {
		return builder.String()
	}


}