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

package checker

import (
	"testing"

	tc "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/filter"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	tc.TestingT(t)
}

var _ = tc.Suite(&testCheckSuite{})

type testCheckSuite struct{}

func (t *testCheckSuite) TestVerifyDumpPrivileges(c *tc.C) {
	cases := []struct {
		grants      []string
		checkTables []*filter.Table
		dumpState   State
		errMatch    string
	}{
		{
			grants:    nil, // non grants
			dumpState: StateFailure,
		},
		{
			grants:    []string{"invalid SQL statement"},
			dumpState: StateFailure,
		},
		{
			grants:    []string{"CREATE DATABASE db1"}, // non GRANT statement
			dumpState: StateFailure,
		},
		{
			grants:    []string{"GRANT RELOAD ON *.* TO 'user'@'%'"}, // lack SELECT privilege
			dumpState: StateFailure,
			checkTables: []*filter.Table{
				{Schema: "db1", Name: "tb1"},
			},
			errMatch: "lack of Select privilege: {`db1`.`tb1`}; ",
		},
		{
			grants: []string{ // lack optional privilege
				"GRANT RELOAD ON *.* TO 'user'@'%'",
				"GRANT EXECUTE ON FUNCTION db1.anomaly_score TO 'user1'@'domain-or-ip-address1'",
			},
			dumpState: StateFailure,
			checkTables: []*filter.Table{
				{Schema: "db1", Name: "anomaly_score"},
			},
			// `db1`.`anomaly_score`
			// can't guarantee the order
			errMatch: "lack of Select privilege: {.*}; ",
		},
		{
			grants: []string{ // have privileges
				"GRANT RELOAD, SELECT ON *.* TO 'user'@'%'",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // lower case
				"GRANT all privileges ON *.* TO 'user'@'%'",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret'",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password' WITH GRANT OPTION",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState: StateFailure,
			errMatch:  "lack of .* privilege.*; lack of .* privilege.*; ",
		},
		{
			grants: []string{ // test `LOAD FROM S3, SELECT INTO S3` not at end
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // ... and `LOAD FROM S3` at beginning, as well as not adjacent with `SELECT INTO S3`
				"GRANT LOAD FROM S3, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState: StateSuccess,
		},
		{
			grants: []string{ // lack db/table level privilege
				"GRANT ALL PRIVILEGES ON `medz`.* TO `zhangsan`@`10.8.1.9` WITH GRANT OPTION",
			},
			dumpState: StateFailure,
			checkTables: []*filter.Table{
				{Schema: "medz", Name: "medz"},
			},
			errMatch: "lack of RELOAD privilege; ",
		},
		{
			grants: []string{ // privilege on db/table level is not enough to execute SHOW MASTER STATUS
				"GRANT ALL PRIVILEGES ON `medz`.* TO `zhangsan`@`10.8.1.9` WITH GRANT OPTION",
			},
			dumpState: StateFailure,
			checkTables: []*filter.Table{
				{Schema: "medz", Name: "medz"},
			},
			errMatch: "lack of RELOAD privilege; ",
		},
		{
			grants: []string{ // privilege on column level is not enough to execute SHOW CREATE TABLE
				"GRANT RELOAD ON *.* TO 'user'@'%'",
				"GRANT SELECT (c) ON `lance`.`t` TO 'user'@'%'",
			},
			dumpState: StateFailure,
			checkTables: []*filter.Table{
				{Schema: "lance", Name: "t"},
			},
			errMatch: "lack of Select privilege: {`lance`.`t`}; ",
		},
		{
			grants: []string{
				"GRANT RELOAD ON *.* TO `u1`@`localhost`",
				"GRANT SELECT ON `db1`.* TO `u1`@`localhost`",
				"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
			},
			dumpState: StateSuccess,
			checkTables: []*filter.Table{
				{Schema: "db1", Name: "t"},
			},
		},
		{
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `root`@`localhost` WITH GRANT OPTION",
				"GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN,BACKUP_ADMIN,BINLOG_ADMIN,BINLOG_ENCRYPTION_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,ENCRYPTION_KEY_ADMIN,GROUP_REPLICATION_ADMIN,INNODB_REDO_LOG_ARCHIVE,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_APPLIER,REPLICATION_SLAVE_ADMIN,RESOURCE_GROUP_ADMIN,RESOURCE_GROUP_USER,ROLE_ADMIN,SERVICE_CONNECTION_ADMIN,SESSION_VARIABLES_ADMIN,SET_USER_ID,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN,TABLE_ENCRYPTION_ADMIN,XA_RECOVER_ADMIN ON *.* TO `root`@`localhost` WITH GRANT OPTION",
				"GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION",
			},
			dumpState: StateSuccess,
		},
	}
	dumpPrivileges := map[mysql.PrivilegeType]struct{}{
		mysql.SelectPriv: {},
		mysql.ReloadPriv: {},
	}
	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		dumpLackGrants := genDumpPriv(dumpPrivileges, cs.checkTables)
		err := verifyPrivileges(result, cs.grants, dumpLackGrants)
		c.Assert(err == nil, tc.Equals, cs.dumpState == StateSuccess)
		if err != nil && len(cs.errMatch) != 0 {
			c.Assert(err.ShortErr, tc.Matches, cs.errMatch)
		}
	}
}

func (t *testCheckSuite) TestVerifyReplicationPrivileges(c *tc.C) {
	cases := []struct {
		grants           []string
		checkTables      []*filter.Table
		replicationState State
		errMatch         string
	}{
		{
			grants:           nil, // non grants
			replicationState: StateFailure,
		},
		{
			grants:           []string{"invalid SQL statement"},
			replicationState: StateFailure,
		},
		{
			grants:           []string{"CREATE DATABASE db1"}, // non GRANT statement
			replicationState: StateFailure,
		},
		{
			grants:           []string{"GRANT SELECT ON *.* TO 'user'@'%'"}, // lack necessary privilege
			replicationState: StateFailure,
			errMatch:         "lack of .* privilege; lack of .* privilege; ",
		},
		{
			grants:           []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"}, // lack REPLICATION CLIENT privilege
			replicationState: StateFailure,
			errMatch:         "lack of REPLICATION CLIENT privilege; ",
		},
		{
			grants:           []string{"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'"}, // lack REPLICATION SLAVE privilege
			replicationState: StateFailure,
			errMatch:         "lack of REPLICATION SLAVE privilege; ",
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // lower case
				"GRANT all privileges ON *.* TO 'user'@'%'",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret'",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password' WITH GRANT OPTION",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			replicationState: StateFailure,
			errMatch:         "lack of .* privilege; lack of .* privilege; ",
		},
		{
			grants: []string{ // test `LOAD FROM S3, SELECT INTO S3` not at end
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // ... and `LOAD FROM S3` at beginning, as well as not adjacent with `SELECT INTO S3`
				"GRANT LOAD FROM S3, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			replicationState: StateSuccess,
		},
	}

	replicationPrivileges := map[mysql.PrivilegeType]struct{}{
		mysql.ReplicationClientPriv: {},
		mysql.ReplicationSlavePriv:  {},
	}
	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		replicationLackGrants := genReplicPriv(replicationPrivileges)
		err := verifyPrivileges(result, cs.grants, replicationLackGrants)
		c.Assert(err == nil, tc.Equals, cs.replicationState == StateSuccess)
		if err != nil && len(cs.errMatch) != 0 {
			c.Assert(err.ShortErr, tc.Matches, cs.errMatch)
		}
	}
}

func TestVerifyPrivilegesWildcard(t *testing.T) {
	cases := []struct {
		grants           []string
		checkTables      []*filter.Table
		replicationState State
		errStr           string
	}{
		{
			grants: []string{
				"GRANT SELECT ON `demo\\_foobar`.* TO `dmuser`@`%`",
			},
			checkTables: []*filter.Table{
				{Schema: "demo_foobar", Name: "t1"},
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{
				"GRANT SELECT ON `demo\\_foobar`.* TO `dmuser`@`%`",
			},
			checkTables: []*filter.Table{
				{Schema: "demo2foobar", Name: "t1"},
			},
			replicationState: StateFailure,
			errStr:           "lack of Select privilege: {`demo2foobar`.`t1`}; ",
		},
		{
			grants: []string{
				"GRANT SELECT ON `demo_`.* TO `dmuser`@`%`",
			},
			checkTables: []*filter.Table{
				{Schema: "demo1", Name: "t1"},
				{Schema: "demo2", Name: "t1"},
			},
			replicationState: StateSuccess,
		},
		{
			grants: []string{
				"GRANT SELECT ON `demo%`.* TO `dmuser`@`%`",
			},
			checkTables: []*filter.Table{
				{Schema: "demo_some", Name: "t1"},
				{Schema: "block_db", Name: "t1"},
			},
			replicationState: StateFailure,
			errStr:           "lack of Select privilege: {`block_db`.`t1`}; ",
		},
		{
			grants: []string{
				"GRANT SELECT ON `demo_db`.`t1` TO `dmuser`@`%`",
			},
			checkTables: []*filter.Table{
				{Schema: "demo_db", Name: "t1"},
				{Schema: "demo2db", Name: "t1"},
			},
			replicationState: StateFailure,
			errStr:           "lack of Select privilege: {`demo2db`.`t1`}; ",
		},
	}

	for i, cs := range cases {
		t.Logf("case %d", i)
		result := &Result{
			State: StateFailure,
		}
		requiredPrivs := genExpectPriv(map[mysql.PrivilegeType]struct{}{
			mysql.SelectPriv: {},
		}, cs.checkTables)
		err := verifyPrivileges(result, cs.grants, requiredPrivs)
		if cs.replicationState == StateSuccess {
			require.Nil(t, err, "grants: %v", cs.grants)
		} else {
			require.NotNil(t, err, "grants: %v", cs.grants)
			require.Equal(t, cs.errStr, err.ShortErr, "grants: %v", cs.grants)
		}
	}
}
