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

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/tests/util"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	pd "github.com/tikv/pd/client"
)

func main() {
	cfg := util.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg[0])
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()
	if err := prepare(sourceDB); err != nil {
		log.S().Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := addLock(ctx, cfg); err != nil {
		log.S().Fatal(err)
	}
	time.Sleep(5 * time.Second)
	if err := finishMark(sourceDB); err != nil {
		log.S().Fatal(err)
	}
}

func prepare(sourceDB *sql.DB) error {
	sqls := []string{
		"use test;",
		"create table t1 (a int primary key);"}
	for _, sql := range sqls {
		_, err := sourceDB.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func finishMark(sourceDB *sql.DB) error {
	sqls := []string{
		"use test;",
		"insert into t1 value (1);",
		"create table t2 (a int primary key);"}
	for _, sql := range sqls {
		_, err := sourceDB.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func addLock(ctx context.Context, cfg *util.Config) error {
	http.DefaultClient.Timeout = 10 * time.Second

	tableID, err := getTableID(cfg.SourceDBCfg[0].Host, "test", "t1")
	if err != nil {
		return errors.Trace(err)
	}

	pdcli, err := pd.NewClientWithContext(
		ctx, strings.Split(cfg.PDAddr, ","), pd.SecurityOption{})
	if err != nil {
		return errors.Trace(err)
	}
	defer pdcli.Close()

	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", cfg.PDAddr))
	if err != nil {
		return errors.Trace(err)
	}

	locker := Locker{
		tableID:   tableID,
		tableSize: 1000,
		lockTTL:   10 * time.Second,
		pdcli:     pdcli,
		kv:        store.(tikv.Storage),
	}
	return errors.Trace(locker.generateLocks(ctx, 10*time.Second))
}

// getTableID of the table with specified table name.
func getTableID(dbAddr, dbName, table string) (int64, error) {
	dbStatusAddr := net.JoinHostPort(dbAddr, "10080")
	url := fmt.Sprintf("http://%s/schema/%s/%s", dbStatusAddr, dbName, table)

	resp, err := http.Get(url)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if resp.StatusCode != 200 {
		return 0, errors.Errorf("HTTP request to TiDB status reporter returns %v. Body: %v", resp.StatusCode, string(body))
	}

	var data model.TableInfo
	err = json.Unmarshal(body, &data)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.ID, nil
}

// Locker leaves locks on a table.
type Locker struct {
	tableID   int64
	tableSize int64
	lockTTL   time.Duration

	pdcli pd.Client
	kv    tikv.Storage
}

// generateLocks sends Prewrite requests to TiKV to generate locks, without committing and rolling back.
func (c *Locker) generateLocks(ctx context.Context, genDur time.Duration) error {
	log.Info("genLock started")

	const maxTxnSize = 1000

	// How many keys should be in the next transaction.
	nextTxnSize := rand.Intn(maxTxnSize) + 1 // 0 is not allowed.

	// How many keys has been scanned since last time sending request.
	scannedKeys := 0
	var batch []int64

	// Send lock for 10 seconds
	timer := time.After(genDur)
	for rowID := int64(0); ; rowID = (rowID + 1) % c.tableSize {
		select {
		case <-timer:
			log.Info("genLock done")
			return nil
		default:
		}

		scannedKeys++

		// Randomly decide whether to lock current key.
		lockThis := rand.Intn(2) == 0

		if lockThis {
			batch = append(batch, rowID)

			if len(batch) >= nextTxnSize {
				// The batch is large enough to start the transaction
				err := c.lockKeys(ctx, batch)
				if err != nil {
					return errors.Annotate(err, "lock keys failed")
				}

				// Start the next loop
				batch = batch[:0]
				scannedKeys = 0
				nextTxnSize = rand.Intn(maxTxnSize) + 1
			}
		}
	}
}

func (c *Locker) lockKeys(ctx context.Context, rowIDs []int64) error {
	keys := make([][]byte, 0, len(rowIDs))

	keyPrefix := tablecodec.GenTableRecordPrefix(c.tableID)
	for _, rowID := range rowIDs {
		key := tablecodec.EncodeRecordKey(keyPrefix, rowID)
		keys = append(keys, key)
	}

	primary := keys[0]

	for len(keys) > 0 {
		lockedKeys, err := c.lockBatch(ctx, keys, primary)
		if err != nil {
			return errors.Trace(err)
		}
		keys = keys[lockedKeys:]
	}
	return nil
}

func (c *Locker) lockBatch(ctx context.Context, keys [][]byte, primary []byte) (int, error) {
	const maxBatchSize = 16 * 1024

	// TiKV client doesn't expose Prewrite interface directly. We need to manually locate the region and send the
	// Prewrite requests.

	bo := tikv.NewBackoffer(ctx, 20000)
	for {
		loc, err := c.kv.GetRegionCache().LocateKey(bo, keys[0])
		if err != nil {
			return 0, errors.Trace(err)
		}

		// Get a timestamp to use as the startTs
		physical, logical, err := c.pdcli.GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		startTs := oracle.ComposeTS(physical, logical)

		// Pick a batch of keys and make up the mutations
		var mutations []*kvrpcpb.Mutation
		batchSize := 0

		for _, key := range keys {
			if len(loc.EndKey) > 0 && bytes.Compare(key, loc.EndKey) >= 0 {
				break
			}
			if bytes.Compare(key, loc.StartKey) < 0 {
				break
			}

			value := randStr()
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Op_Put,
				Key:   key,
				Value: []byte(value),
			})
			batchSize += len(key) + len(value)

			if batchSize >= maxBatchSize {
				break
			}
		}

		lockedKeys := len(mutations)
		if lockedKeys == 0 {
			return 0, nil
		}

		prewrite := &kvrpcpb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  primary,
			StartVersion: startTs,
			LockTtl:      uint64(c.lockTTL.Milliseconds()),
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, prewrite)

		// Send the requests
		resp, err := c.kv.SendReq(bo, req, loc.Region, time.Second*20)
		if err != nil {
			return 0, errors.Annotatef(
				err,
				"send request failed. region: %+v [%+q, %+q), keys: %+q",
				loc.Region, loc.StartKey, loc.EndKey, keys[0:lockedKeys])
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}

		prewriteResp := resp.Resp
		if prewriteResp == nil {
			return 0, errors.Errorf("response body missing")
		}

		// Ignore key errors since we never commit the transaction and we don't need to keep consistency here.
		return lockedKeys, nil
	}
}
func randStr() string {
	length := rand.Intn(128)
	res := ""
	for i := 0; i < length; i++ {
		res += string('a' + rand.Intn(26))
	}
	return res
}
