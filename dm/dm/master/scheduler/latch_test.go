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

package scheduler

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOneAcquireSuccess(t *testing.T) {
	t.Parallel()

	var (
		l       = newLatches()
		fire    = make(chan struct{})
		success = make(chan string, 2)
		fail    = make(chan struct{}, 8)
		group1  = "group1"
		group2  = "group2"
		wg      sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		var group string
		if i < 5 {
			group = group1
		} else {
			group = group2
		}

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-fire
			_, err := l.tryAcquire(name)
			if err != nil {
				fail <- struct{}{}
			} else {
				success <- name
			}
		}(group)
	}

	for i := 0; i < 10; i++ {
		fire <- struct{}{}
	}
	wg.Wait()

	require.Len(t, fail, 8)

	var succNames []string
	succNames = append(succNames, <-success)
	succNames = append(succNames, <-success)
	sort.Strings(succNames)
	require.Equal(t, []string{group1, group2}, succNames)
}

func TestAcquireAfterRelease(t *testing.T) {
	t.Parallel()

	var (
		l       = newLatches()
		fire    = make(chan struct{})
		success int
		group   = "group1"
		wg      sync.WaitGroup
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-fire

			for {
				release, err := l.tryAcquire(group)
				if err != nil {
					time.Sleep(10 * time.Millisecond)
				} else {
					time.Sleep(20 * time.Millisecond)
					success++
					release()
					return
				}
			}
		}()
	}

	for i := 0; i < 5; i++ {
		fire <- struct{}{}
	}

	wg.Wait()
	require.Equal(t, 5, success)
}

func TestMultiRelease(t *testing.T) {
	t.Parallel()

	var (
		l     = newLatches()
		names = []string{"name1", "name2", "name3"}
		fire  = make(chan struct{}, len(names))
		wg    sync.WaitGroup
	)

	for repeat := 0; repeat < 3; repeat++ {
		for i := range names {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				<-fire
				release, err := l.tryAcquire(name)
				require.NoError(t, err)
				release()
				// will not panic or cause other error
				release()
			}(names[i])
		}

		for range names {
			fire <- struct{}{}
		}
		wg.Wait()
	}
}

func TestWontReleaseOther(t *testing.T) {
	t.Parallel()

	var (
		l     = newLatches()
		group = "group1"
	)

	release1, err := l.tryAcquire(group)
	require.NoError(t, err)
	release1()

	// because release1 is called, another tryAcquire should succeed
	release2, err := l.tryAcquire(group)
	require.NoError(t, err)

	// release1 should not release the latch of release2, we test this by tryAcquire
	release1()
	_, err = l.tryAcquire(group)
	require.Error(t, err)

	release2()
	_, err = l.tryAcquire(group)
	require.NoError(t, err)
}
