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

package notify

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestNotifyHub(t *testing.T) {
	t.Parallel()

	notifier := new(Notifier)
	r1, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	r2, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	r3, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	finishedCh := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify()
		}
		close(finishedCh)
	}()
	<-r1.C
	r1.Stop()
	<-r2.C
	<-r3.C

	r2.Stop()
	r3.Stop()
	require.Equal(t, 0, len(notifier.receivers))
	r4, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	<-r4.C
	r4.Stop()

	notifier2 := new(Notifier)
	r5, err := notifier2.NewReceiver(10 * time.Millisecond)
	require.Nil(t, err)
	<-r5.C
	r5.Stop()
	<-finishedCh // To make the leak checker happy
}

func TestContinusStop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	notifier := new(Notifier)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			notifier.Notify()
		}
	}()
	n := 50
	receivers := make([]*Receiver, n)
	var err error
	for i := 0; i < n; i++ {
		receivers[i], err = notifier.NewReceiver(10 * time.Millisecond)
		require.Nil(t, err)
	}
	for i := 0; i < n; i++ {
		i := i
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-receivers[i].C:
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		receivers[i].Stop()
	}
	<-ctx.Done()
}

func TestNewReceiverWithClosedNotifier(t *testing.T) {
	t.Parallel()

	notifier := new(Notifier)
	notifier.Close()
	_, err := notifier.NewReceiver(50 * time.Millisecond)
	require.True(t, errors.ErrOperateOnClosedNotifier.Equal(err))
}

func TestNotifierMultiple(t *testing.T) {
	t.Parallel()
	notifier := new(Notifier)

	receiver, err := notifier.NewReceiver(-1)
	require.NoError(t, err)
	counter := atomic.NewInt32(0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, ok := <-receiver.C
			if !ok {
				return
			}
			counter.Add(1)
		}
	}()

	receiver1, err := notifier.NewReceiver(time.Minute)
	require.NoError(t, err)
	counter1 := atomic.NewInt32(0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, ok := <-receiver1.C
			if !ok {
				return
			}
			counter1.Add(1)
		}
	}()

	N := 5
	for i := 0; i < N; i++ {
		notifier.Notify()
		time.Sleep(time.Millisecond * 100)
	}

	notifier.Close()
	wg.Wait()
	require.LessOrEqual(t, counter.Load(), int32(N))
	require.LessOrEqual(t, counter1.Load(), int32(N))
}
