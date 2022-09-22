// Copyright 2022 PingCAP, Inc.
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

package dm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/dbutil"
	sqlconfig "github.com/pingcap/tiflow/dm/simulator/config"
	"github.com/pingcap/tiflow/dm/simulator/mcp"
	sqlgen "github.com/pingcap/tiflow/dm/simulator/sqlgen"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/test/e2e"
	"go.uber.org/zap"
)

const (
	tableNum     = 5
	rowNum       = 1000
	batch        = 100
	diffTimes    = 10
	diffInterval = 10 * time.Second
)

// Case is a data migration Case test case with one or more sources.
type Case struct {
	ctx context.Context

	addr     string
	cfgBytes []byte
	sources  []*dbConn
	target   *dbConn
	jobID    string
	name     string

	// source -> table -> mcp
	mcps []map[string]*mcp.ModificationCandidatePool
	// source -> table -> generator
	generators []map[string]sqlgen.SQLGenerator
	// table -> key -> struct{}
	keySet map[string]map[string]struct{}

	result []int
}

// NewCase creates a new test case.
func NewCase(ctx context.Context, addr string, name string, cfgPath string) (*Case, error) {
	cfgBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}

	var jobCfg config.JobCfg
	if err := jobCfg.Decode(cfgBytes); err != nil {
		return nil, err
	}

	c := &Case{
		ctx:        ctx,
		sources:    make([]*dbConn, 0, len(jobCfg.Upstreams)),
		cfgBytes:   cfgBytes,
		addr:       addr,
		name:       name,
		mcps:       make([]map[string]*mcp.ModificationCandidatePool, 0, 3),
		generators: make([]map[string]sqlgen.SQLGenerator, 0, 3),
		keySet:     make(map[string]map[string]struct{}, tableNum),
		result:     make([]int, 3),
	}
	for _, upstream := range jobCfg.Upstreams {
		source, err := newDBConn(ctx, upstream.DBCfg, name)
		if err != nil {
			return nil, err
		}
		c.sources = append(c.sources, source)
	}
	target, err := newDBConn(ctx, jobCfg.TargetDB, name)
	if err != nil {
		return nil, err
	}
	c.target = target

	for range c.sources {
		generators := make(map[string]sqlgen.SQLGenerator)
		mcps := make(map[string]*mcp.ModificationCandidatePool)
		for i := 1; i <= tableNum; i++ {
			tableName := fmt.Sprintf("tb%d", i)
			tableConfig := &sqlconfig.TableConfig{
				DatabaseName: c.name,
				TableName:    tableName,
				Columns: []*sqlconfig.ColumnDefinition{
					{
						ColumnName: "id",
						DataType:   "int",
						DataLen:    11,
					},
					{
						ColumnName: "name",
						DataType:   "varchar",
						DataLen:    255,
					},
					{
						ColumnName: "age",
						DataType:   "int",
						DataLen:    11,
					},
					{
						ColumnName: "team_id",
						DataType:   "int",
						DataLen:    11,
					},
				},
				UniqueKeyColumnNames: []string{"id"},
			}
			generators[tableName] = sqlgen.NewSQLGeneratorImpl(tableConfig)
			mcps[tableName] = mcp.NewModificationCandidatePool(1000000)
			c.keySet[tableName] = make(map[string]struct{})
		}
		c.generators = append(c.generators, generators)
		c.mcps = append(c.mcps, mcps)
	}

	return c, nil
}

// Run runs a test case.
func (c *Case) Run(ctx context.Context) error {
	defer func() {
		log.L().Info("finish run case", zap.Int("insert", c.result[0]), zap.Int("update", c.result[1]), zap.Int("delete", c.result[2]))
	}()
	if err := c.createJob(ctx); err != nil {
		return err
	}
	if err := c.genFullData(); err != nil {
		return err
	}
	if err := c.diffDataLoop(ctx); err != nil {
		return err
	}
	return c.incrLoop(ctx)
}

func (c *Case) createJob(ctx context.Context) error {
	jobID, err := e2e.CreateJobViaHTTP(ctx, c.addr, "chaos-dm-test", "project-dm", pb.Job_DM, c.cfgBytes)
	if err != nil {
		return err
	}
	c.jobID = jobID
	return nil
}

func (c *Case) genFullData() error {
	log.L().Info("start generate full data")
	for source, generators := range c.generators {
		for table, generator := range generators {
			if _, err := c.sources[source].ExecuteSQLs("CREATE DATABASE IF NOT EXISTS "+c.name+" CHARSET latin1", "USE "+c.name); err != nil {
				return err
			}
			if _, err := c.sources[source].ExecuteSQLs(generator.GenCreateTable()); err != nil {
				return err
			}
			sqls := make([]string, 0, rowNum)
			for j := 0; j < rowNum; j++ {
				sql, uk, err := generator.GenInsertRow()
				if err != nil {
					return err
				}
				// key already exists
				if _, ok := c.keySet[table][uk.GetValueHash()]; ok {
					continue
				}
				if err := c.mcps[source][table].AddUK(uk); err != nil {
					return err
				}
				c.keySet[table][uk.GetValueHash()] = struct{}{}
				sqls = append(sqls, sql)
			}
			if _, err := c.sources[source].ExecuteSQLs(sqls...); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: use sync-diff-inspector instead.
func (c *Case) diffData(ctx context.Context) (bool, error) {
	log.L().Info("start diff data")
	for i := 1; i <= tableNum; i++ {
		tableName := fmt.Sprintf("tb%d", i)
		row := c.target.db.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT count(1) FROM %s", dbutil.TableName(c.target.currDB, tableName)))
		if row.Err() != nil {
			return false, row.Err()
		}
		var count int
		if err := row.Scan(&count); err != nil {
			return false, err
		}
		var totalCount int
		for _, mcps := range c.mcps {
			totalCount += mcps[tableName].Len()
		}
		if count != totalCount {
			log.Error("data is not same", zap.Int("downstream", count), zap.Int("upstream", totalCount))
			return false, nil
		}
	}
}

func (c *Case) diffDataLoop(ctx context.Context) error {
	for i := 0; i < diffTimes; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(diffInterval):
			if same, err := c.diffData(ctx); err != nil {
				if strings.Contains(err.Error(), "not found") {
					continue
				}
				return err
			} else if same {
				return nil
			}
		}
	}
	return errors.New("data is not same")
}

// randDML generates DML (INSERT, UPDATE or DELETE).
func (c *Case) randDML(source int, table string) (string, error) {
	generator := c.generators[source][table]
	mcp := c.mcps[source][table]
	t := rand.Intn(3)
	c.result[t]++
	switch t {
	case 0:
		sql, uk, err := generator.GenInsertRow()
		if err != nil {
			return "", err
		}
		for _, ok := c.keySet[table][uk.GetValueHash()]; ok; {
			sql, uk, err = generator.GenInsertRow()
			if err != nil {
				return "", err
			}
		}
		if err := c.mcps[source][table].AddUK(uk); err != nil {
			return "", err
		}
		c.keySet[table][uk.GetValueHash()] = struct{}{}
		return sql, nil
	case 1:
		return generator.GenUpdateRow(mcp.NextUK())
	default:
		key := mcp.NextUK()
		sql, err := generator.GenDeleteRow(key)
		if err != nil {
			return "", err
		}
		delete(c.keySet[table], key.GetValueHash())
		err = mcp.DeleteUK(key)
		return sql, err
	}
}

func (c *Case) genIncrData(ctx context.Context) error {
	log.L().Info("start generate incremental data")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		source := rand.Intn(len(c.sources))
		tableName := fmt.Sprintf("tb%d", rand.Intn(tableNum)+1)

		sqls := make([]string, 0, batch)
		for i := 0; i < batch; i++ {
			sql, err := c.randDML(source, tableName)
			if err != nil {
				return err
			}
			sqls = append(sqls, sql)
		}
		if _, err := c.sources[source].ExecuteSQLs(sqls...); err != nil {
			return err
		}
	}
}

func (c *Case) incrLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		ctx2, cancel := context.WithTimeout(ctx, time.Second*10)
		err := c.genIncrData(ctx2)
		cancel()
		if err != nil {
			return err
		}
		if err := c.diffDataLoop(ctx); err != nil {
			return err
		}
	}
}
