// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"fmt"
	"testing"
	"time"

	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	timock "github.com/pingcap/tidb/pkg/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

var benchmarkCausalityKeyCount int

// Each benchmark operation processes one buffered batch, including job/row
// creation, compaction, flush and output causality-key retrieval. Database SQL,
// worker scheduling and channel concurrency are deliberately excluded.
func BenchmarkCompactorCausality(b *testing.B) {
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt("CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE, c INT)", "", "")
	if err != nil {
		b.Fatal(err)
	}
	ti, err := tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	if err != nil {
		b.Fatal(err)
	}
	table := &cdcmodel.TableName{Schema: "bench", Table: "tb"}
	whereHandle := sqlmodel.GetWhereHandle(ti, ti)
	type change struct{ pre, post []interface{} }
	for _, workload := range []string{"independent", "hot_stable", "hot_changing_uk", "interleaved_reuse"} {
		sizes := []int{1000}
		if workload == "hot_changing_uk" {
			sizes = []int{100, 1000, 4000}
		}
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/n%d", workload, size), func(b *testing.B) {
				changes := make([]change, size)
				for i := range size {
					switch workload {
					case "independent":
						changes[i] = change{[]interface{}{i, i, 0}, []interface{}{i, i, 1}}
					case "hot_stable":
						changes[i] = change{[]interface{}{1, 1, i}, []interface{}{1, 1, i + 1}}
					case "hot_changing_uk":
						changes[i] = change{[]interface{}{1, i, 0}, []interface{}{1, i + 1, 0}}
					case "interleaved_reuse":
						a := 3 * (i / 3)
						switch i % 3 {
						case 0:
							changes[i] = change{pre: []interface{}{a, a + 1, 0}}
						case 1:
							changes[i] = change{post: []interface{}{a + 1, a + 1, 0}}
						case 2:
							changes[i] = change{post: []interface{}{a, a, 0}}
						}
					}
				}
				c := &compactor{
					logger: log.L(), keyMap: make(map[string]map[string]int),
					buffer: make([]*job, 0, size), outCh: make(chan *job, size),
					updateJobMetricsFn: func(bool, string, *job) {},
				}
				emitted := 0
				b.ReportAllocs()
				b.ResetTimer()
				start := time.Now()
				for range b.N {
					for _, ch := range changes {
						row := sqlmodel.NewRowChange(table, table, ch.pre, ch.post, ti, nil, nil)
						row.SetWhereHandle(whereHandle)
						c.compactJob(newDMLJob(row, ec))
					}
					c.flushBuffer()
					for len(c.outCh) > 0 {
						j := <-c.outCh
						benchmarkCausalityKeyCount += len(j.causalityKeys())
						emitted++
					}
				}
				elapsed := time.Since(start)
				b.StopTimer()
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*size), "ns/input")
				b.ReportMetric(float64(emitted)/float64(b.N*size), "output/input")
			})
		}
	}

	b.Run("burst_then_single_flush/n1000", func(b *testing.B) {
		const (
			burstSize   = 4000
			singleCount = 1000
		)
		c := &compactor{
			logger: log.L(), keyMap: make(map[string]map[string]int),
			buffer: make([]*job, 0, burstSize), outCh: make(chan *job, burstSize),
			updateJobMetricsFn: func(bool, string, *job) {},
		}
		for i := range burstSize {
			row := sqlmodel.NewRowChange(table, table, []interface{}{i, i, 0}, []interface{}{i, i, 1}, ti, nil, nil)
			row.SetWhereHandle(whereHandle)
			c.compactJob(newDMLJob(row, ec))
		}
		c.flushBuffer()
		for len(c.outCh) > 0 {
			<-c.outCh
		}

		emitted := 0
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		for range b.N {
			for i := range singleCount {
				row := sqlmodel.NewRowChange(table, table, []interface{}{i, i, 0}, []interface{}{i, i, 1}, ti, nil, nil)
				row.SetWhereHandle(whereHandle)
				c.compactJob(newDMLJob(row, ec))
				c.flushBuffer()
				j := <-c.outCh
				benchmarkCausalityKeyCount += len(j.causalityKeys())
				emitted++
			}
		}
		elapsed := time.Since(start)
		b.StopTimer()
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*singleCount), "ns/input")
		b.ReportMetric(float64(emitted)/float64(b.N*singleCount), "output/input")
	})
}
