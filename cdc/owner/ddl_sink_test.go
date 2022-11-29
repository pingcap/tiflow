// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/stretchr/testify/require"
)

type mockSink struct {
	sink.Sink
	checkpointTs model.Ts
	ddl          *model.DDLEvent
	ddlMu        sync.Mutex
	ddlError     error
}

func (m *mockSink) EmitCheckpointTs(_ context.Context, ts uint64, _ []*model.TableInfo) error {
	atomic.StoreUint64(&m.checkpointTs, ts)
	return nil
}

func (m *mockSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	time.Sleep(1 * time.Second)
	m.ddl = ddl
	return m.ddlError
}

func (m *mockSink) Close(ctx context.Context) error {
	return nil
}

func (m *mockSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (m *mockSink) GetDDL() *model.DDLEvent {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	return m.ddl
}

func newDDLSink4Test(reportErr func(err error)) (DDLSink, *mockSink) {
	mockSink := &mockSink{}
	ddlSink := newDDLSink(model.DefaultChangeFeedID("changefeed-test"), &model.ChangeFeedInfo{}, reportErr)
	ddlSink.(*ddlSinkImpl).sinkInitHandler = func(ctx context.Context, s *ddlSinkImpl) error {
		s.sinkV1 = mockSink
		return nil
	}
	return ddlSink, mockSink
}

func TestCheckpoint(t *testing.T) {
	ddlSink, mSink := newDDLSink4Test(func(err error) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx)

	waitCheckpointGrowingUp := func(m *mockSink, targetTs model.Ts) error {
		return retry.Do(ctx, func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(30))
	}
	ddlSink.emitCheckpointTs(1, nil)
	require.Nil(t, waitCheckpointGrowingUp(mSink, 1))
	ddlSink.emitCheckpointTs(10, nil)
	require.Nil(t, waitCheckpointGrowingUp(mSink, 10))
}

func TestExecDDLEvents(t *testing.T) {
	ddlSink, mSink := newDDLSink4Test(func(err error) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx)

	ddlEvents := []*model.DDLEvent{
		{CommitTs: 1, Query: "create table t1(id int)"},
		{CommitTs: 2, Query: "create table t2(id int)"},
		{CommitTs: 3, Query: "create table t3(id int)"},
	}

	for _, event := range ddlEvents {
		for {
			done, err := ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			if done {
				require.Equal(t, mSink.GetDDL(), event)
				break
			}
		}
	}
}

func TestExecDDLError(t *testing.T) {
	var (
		resultErr   error
		resultErrMu sync.Mutex
	)
	readResultErr := func() error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		return resultErr
	}

	ddlSink, mSink := newDDLSink4Test(func(err error) {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		resultErr = err
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()

	ddlSink.run(ctx)

	mSink.ddlError = cerror.ErrExecDDLFailed.GenWithStackByArgs()
	ddl2 := &model.DDLEvent{CommitTs: 2, Query: "create table t2(id int)"}
	for {
		done, err := ddlSink.emitDDLEvent(ctx, ddl2)
		require.Nil(t, err)

		if done || readResultErr() != nil {
			require.Equal(t, mSink.GetDDL(), ddl2)
			break
		}
	}
	require.True(t, cerror.ErrExecDDLFailed.Equal(readResultErr()))
}

func TestAddSpecialComment(t *testing.T) {
	testCase := []struct {
		input  string
		result string
	}{
		{
			"create table t1 (id int ) shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) " +
				"/*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2     pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) " +
				"/*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 engine=innodb pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */" +
				" ENGINE = innodb /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) pre_split_regions=2 shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! PRE_SPLIT_REGIONS = 2 */" +
				" /*T! SHARD_ROW_ID_BITS = 2 */",
		},
		{
			"create table t6 (id int ) " +
				"shard_row_id_bits=2 shard_row_id_bits=3 pre_split_regions=2;",
			"CREATE TABLE `t6` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ " +
				"/*T! SHARD_ROW_ID_BITS = 3 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int primary key auto_random(2));",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(2) */)",
		},
		{
			"create table t1 (id int primary key auto_random);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM */)",
		},
		{
			"create table t1 (id int auto_random ( 4 ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY)",
		},
		{
			"create table t1 (id int  auto_random  (   4    ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY)",
		},
		{
			"create table t1 (id int auto_random ( 3 ) primary key) auto_random_base = 100;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(3) */" +
				" PRIMARY KEY) /*T![auto_rand_base] AUTO_RANDOM_BASE = 100 */",
		},
		{
			"create table t1 (id int auto_random primary key) auto_random_base = 50;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM */ PRIMARY KEY)" +
				" /*T![auto_rand_base] AUTO_RANDOM_BASE = 50 */",
		},
		{
			"create table t1 (id int auto_increment key) auto_id_cache 100;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT PRIMARY KEY) " +
				"/*T![auto_id_cache] AUTO_ID_CACHE = 100 */",
		},
		{
			"create table t1 (id int auto_increment unique) auto_id_cache 10;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT UNIQUE KEY) " +
				"/*T![auto_id_cache] AUTO_ID_CACHE = 10 */",
		},
		{
			"create table t1 (id int) auto_id_cache = 5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int) auto_id_cache=5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) clustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] CLUSTERED */)",
		},
		{
			"create table t1(id int, v int, primary key(a) clustered);",
			"CREATE TABLE `t1` (`id` INT,`v` INT,PRIMARY KEY(`a`) " +
				"/*T![clustered_index] CLUSTERED */)",
		},
		{
			"create table t1(id int primary key clustered, v int);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY " +
				"/*T![clustered_index] CLUSTERED */,`v` INT)",
		},
		{
			"alter table t add primary key(a) clustered;",
			"ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) nonclustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] NONCLUSTERED */)",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) " +
				"/*T![clustered_index] nonclustered */);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] NONCLUSTERED */)",
		},
		{
			"create table clustered_test(id int)",
			"CREATE TABLE `clustered_test` (`id` INT)",
		},
		{
			"create database clustered_test",
			"CREATE DATABASE `clustered_test`",
		},
		{
			"create database clustered",
			"CREATE DATABASE `clustered`",
		},
		{
			"create table clustered (id int)",
			"CREATE TABLE `clustered` (`id` INT)",
		},
		{
			"create table t1 (id int, a varchar(255) key clustered);",
			"CREATE TABLE `t1` (" +
				"`id` INT,`a` VARCHAR(255) PRIMARY KEY /*T![clustered_index] CLUSTERED */)",
		},
		{
			"alter table t force auto_increment = 12;",
			"ALTER TABLE `t` /*T![force_inc] FORCE */ AUTO_INCREMENT = 12",
		},
		{
			"alter table t force, auto_increment = 12;",
			"ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ , AUTO_INCREMENT = 12",
		},
		{
			"create table cdc_test (id varchar(10) primary key ,c1 varchar(10)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin" +
				"/*!90000  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
			"CREATE TABLE `cdc_test` (`id` VARCHAR(10) PRIMARY KEY,`c1` VARCHAR(10)) " +
				"ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN " +
				"/*T! SHARD_ROW_ID_BITS = 4 */ /*T! PRE_SPLIT_REGIONS = 3 */",
		},
		{
			"CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY auto_increment, " +
				"b varchar(255)) PLACEMENT POLICY=placement1;",
			"CREATE TABLE `t1` (`id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,`b` VARCHAR(255)) ",
		},
		{
			"CREATE TABLE `t1` (\n  `a` int(11) DEFAULT NULL\n) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin " +
				"/*T![placement] PLACEMENT POLICY=`p2` */",
			"CREATE TABLE `t1` (`a` INT(11) DEFAULT NULL) " +
				"ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN ",
		},
		{
			"CREATE TABLE t4 (" +
				"firstname VARCHAR(25) NOT NULL," +
				"lastname VARCHAR(25) NOT NULL," +
				"username VARCHAR(16) NOT NULL," +
				"email VARCHAR(35)," +
				"joined DATE NOT NULL) " +
				"PARTITION BY RANGE( YEAR(joined) )" +
				" (PARTITION p0 VALUES LESS THAN (1960) PLACEMENT POLICY=p1," +
				"PARTITION p1 VALUES LESS THAN (1970),PARTITION p2 VALUES LESS THAN (1980)," +
				"PARTITION p3 VALUES LESS THAN (1990),PARTITION p4 VALUES LESS THAN MAXVALUE);",
			"CREATE TABLE `t4` (" +
				"`firstname` VARCHAR(25) NOT NULL," +
				"`lastname` VARCHAR(25) NOT NULL," +
				"`username` VARCHAR(16) NOT NULL," +
				"`email` VARCHAR(35)," +
				"`joined` DATE NOT NULL) " +
				"PARTITION BY RANGE (YEAR(`joined`)) " +
				"(PARTITION `p0` VALUES LESS THAN (1960) ,PARTITION `p1` VALUES LESS THAN (1970)," +
				"PARTITION `p2` VALUES LESS THAN (1980),PARTITION `p3` VALUES LESS THAN (1990)," +
				"PARTITION `p4` VALUES LESS THAN (MAXVALUE))",
		},
		{
			"ALTER TABLE t3 PLACEMENT POLICY=DEFAULT;",
			"ALTER TABLE `t3`",
		},
		{
			"ALTER TABLE t1 PLACEMENT POLICY=p10",
			"ALTER TABLE `t1`",
		},
		{
			"ALTER TABLE t1 PLACEMENT POLICY=p10, add d text(50)",
			"ALTER TABLE `t1` ADD COLUMN `d` TEXT(50)",
		},
		{
			"alter table tp PARTITION p1 placement policy p2",
			"",
		},
		{
			"alter table t add d text(50) PARTITION p1 placement policy p2",
			"ALTER TABLE `t` ADD COLUMN `d` TEXT(50)",
		},
		{
			"alter table tp set tiflash replica 1 PARTITION p1 placement policy p2",
			"ALTER TABLE `tp` SET TIFLASH REPLICA 1",
		},
		{
			"ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT",
			"",
		},

		{
			"ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY p1 charset utf8mb4",
			"ALTER DATABASE `TestResetPlacementDB`  CHARACTER SET = utf8mb4",
		},
		{
			"/*T![placement] ALTER DATABASE `db1` PLACEMENT POLICY = `p1` */",
			"",
		},
		{
			"ALTER PLACEMENT POLICY p3 PRIMARY_REGION='us-east-1' " +
				"REGIONS='us-east-1,us-east-2,us-west-1';",
			"",
		},
	}
	for _, ca := range testCase {
		re, err := addSpecialComment(ca.input)
		require.Nil(t, err)
		require.Equal(t, re, ca.result)
	}
	require.Panics(t, func() {
		_, _ = addSpecialComment("alter table t force, auto_increment = 12;alter table t force, auto_increment = 12;")
	}, "invalid ddlQuery statement size")
}
