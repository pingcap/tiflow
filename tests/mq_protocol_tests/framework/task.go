// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package framework

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql" // imported for side effects
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Task represents a single test case
type Task interface {
	Name() string
	GetCDCProfile() *CDCProfile
	Prepare(taskContext *TaskContext) error
	Run(taskContext *TaskContext) error
}

// TaskContext is passed to the test case to provide basic utilities for testing
type TaskContext struct {
	Upstream     *sql.DB
	Downstream   *sql.DB
	Env          Environment
	WaitForReady func() error
	Ctx          context.Context
}

// CDCProfile represents the command line arguments used to create the changefeed
type CDCProfile struct {
	PDUri          string
	SinkURI        string
	ConfigFile     string
	SchemaRegistry string
}

// CreateDB creates a database in both the upstream and the downstream
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

// SQLHelper returns an SQLHelper
func (c *TaskContext) SQLHelper() *SQLHelper {
	return &SQLHelper{
		upstream:   c.Upstream,
		downstream: c.Downstream,
		ctx:        c.Ctx,
	}
}

// String returns the string representation of the CDCProfile
func (p *CDCProfile) String() string {
	builder := strings.Builder{}
	builder.WriteString("cli changefeed create ")

	if p.PDUri == "" {
		p.PDUri = "http://127.0.0.1:2379"
	}
	builder.WriteString(fmt.Sprintf("--pd=%s ", strconv.Quote(p.PDUri)))

	if p.SinkURI == "" {
		log.Fatal("SinkURI cannot be empty!")
	}
	builder.WriteString(fmt.Sprintf("--sink-uri=%s ", strconv.Quote(p.SinkURI)))

	if p.ConfigFile != "" {
		builder.WriteString(fmt.Sprintf("--config=%s ", strconv.Quote(p.ConfigFile)))
	}

	if p.SchemaRegistry != "" {
		builder.WriteString(fmt.Sprintf("--schema-registry=%s ", strconv.Quote(p.SchemaRegistry)))
	}

	builder.WriteString(" --log-level debug")
	return builder.String()
}
