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

package orchestrator

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type bankReactorState struct {
	t            *testing.T
	account      []int
	pendingPatch [][]DataPatch
	index        int
	notFirstTick bool
}

const bankTestPrefix = "/ticdc/test/bank/"

func (b *bankReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	require.True(b.t, strings.HasPrefix(key.String(), bankTestPrefix))
	indexStr := key.String()[len(bankTestPrefix):]
	b.account[b.atoi(indexStr)] = b.atoi(string(value))
	return nil
}

func (b *bankReactorState) GetPatches() [][]DataPatch {
	pendingPatches := b.pendingPatch
	b.pendingPatch = nil
	return pendingPatches
}

func (b *bankReactorState) Check() {
	var sum int
	for _, money := range b.account {
		sum += money
	}
	if sum != 0 {
		log.Info("show account", zap.Int("index", b.index), zap.Int("sum", sum), zap.Ints("account", b.account))
	}
	require.Equal(b.t, sum, 0, fmt.Sprintf("not ft:%t", b.notFirstTick))
}

func (b *bankReactorState) atoi(value string) int {
	i, err := strconv.Atoi(value)
	require.Nil(b.t, err)
	return i
}

func (b *bankReactorState) patchAccount(index int, fn func(int) int) DataPatch {
	return &SingleDataPatch{
		Key: util.NewEtcdKey(fmt.Sprintf("%s%d", bankTestPrefix, index)),
		Func: func(old []byte) (newValue []byte, changed bool, err error) {
			oldMoney := b.atoi(string(old))
			newMoney := fn(oldMoney)
			if oldMoney == newMoney {
				return old, false, nil
			}
			log.Debug("change money", zap.Int("account", index), zap.Int("from", oldMoney), zap.Int("to", newMoney))
			return []byte(strconv.Itoa(newMoney)), true, nil
		},
	}
}

func (b *bankReactorState) TransferRandomly(transferNumber int) {
	for i := 0; i < transferNumber; i++ {
		accountA := rand.Intn(len(b.account))
		accountB := rand.Intn(len(b.account))
		transferMoney := rand.Intn(100)
		b.pendingPatch = append(b.pendingPatch, []DataPatch{
			b.patchAccount(accountA, func(money int) int {
				return money - transferMoney
			}),
			b.patchAccount(accountB, func(money int) int {
				return money + transferMoney
			}),
		})
		log.Debug("transfer money", zap.Int("accountA", accountA), zap.Int("accountB", accountB), zap.Int("money", transferMoney))
	}
}

type bankReactor struct {
	accountNumber int
}

func (b *bankReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	bankState := (state).(*bankReactorState)
	bankState.Check()
	// transfer 20% of account
	bankState.TransferRandomly(rand.Intn(b.accountNumber/5 + 2))
	// there is a 20% chance of restarting etcd worker
	if rand.Intn(10) < 2 {
		err = cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	bankState.notFirstTick = true
	return state, err
}

func TestEtcdBank(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tiflow/pkg/orchestrator/InjectProgressRequestAfterCommit", "10%return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/orchestrator/InjectProgressRequestAfterCommit")
	}()

	totalAccountNumber := 25
	workerNumber := 10
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	newClient, closer := setUpTest(t)
	defer closer()

	cli := newClient()
	defer func() {
		_ = cli.Unwrap().Close()
	}()

	for i := 0; i < totalAccountNumber; i++ {
		_, err := cli.Put(ctx, fmt.Sprintf("%s%d", bankTestPrefix, i), "0")
		require.Nil(t, err)
	}

	for i := 0; i < workerNumber; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				worker, err := NewEtcdWorker(cli, bankTestPrefix, &bankReactor{
					accountNumber: totalAccountNumber,
				}, &bankReactorState{t: t, index: i, account: make([]int, totalAccountNumber)})
				require.Nil(t, err)
				err = worker.Run(ctx, nil, 100*time.Millisecond, "127.0.0.1", "")
				if err == nil || err.Error() == "etcdserver: request timed out" {
					continue
				}
				require.Contains(t, err.Error(), "context deadline exceeded")
				return
			}
		}()
	}
	wg.Wait()
}
