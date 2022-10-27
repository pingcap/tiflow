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
//
// ============================================================
// Forked from https://github.com/golang-design/chann.
// Copyright 2021 The golang.design Initiative Authors.
// All rights reserved. Use of this source code is governed
// by a MIT license that can be found in the LICENSE file.
//
// Written by Changkun Ou <changkun.de>

package chann

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChan(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	N := 200
	if testing.Short() {
		N = 20
	}
	for chanCap := 0; chanCap < N; chanCap++ {
		{
			// Ensure that receive from empty chan blocks.
			c := New[int](Cap(chanCap))
			recv1 := false
			go func() {
				<-c.Out()
				recv1 = true
			}()
			recv2 := false
			go func() {
				<-c.Out()
				recv2 = true
			}()
			time.Sleep(time.Millisecond)
			require.Falsef(t, recv1, "chan[%d]: receive from empty chan", chanCap)
			require.Falsef(t, recv2, "chan[%d]: receive from empty chan", chanCap)
			// Ensure that non-blocking receive does not block.
			select {
			case <-c.Out():
				t.Fatalf("chan[%d]: receive from empty chan", chanCap)
			default:
			}
			select {
			case <-c.Out():
				t.Fatalf("chan[%d]: receive from empty chan", chanCap)
			default:
			}
			c.In() <- 0
			c.In() <- 0
		}

		{
			// Ensure that send to full chan blocks.
			c := New[int](Cap(chanCap))
			for i := 0; i < chanCap; i++ {
				c.In() <- i
			}
			sent := uint32(0)
			go func() {
				c.In() <- 0
				atomic.StoreUint32(&sent, 1)
			}()
			time.Sleep(time.Millisecond)
			require.Equalf(t,
				uint32(0),
				atomic.LoadUint32(&sent),
				"chan[%d]: send to full chan", chanCap,
			)
			// Ensure that non-blocking send does not block.
			select {
			case c.In() <- 0:
				t.Fatalf("chan[%d]: send to full chan", chanCap)
			default:
			}
			<-c.Out()
		}

		{
			// Ensure that we receive 0 from closed chan.
			c := New[int](Cap(chanCap))
			for i := 0; i < chanCap; i++ {
				c.In() <- i
			}
			c.Close()
			for i := 0; i < chanCap; i++ {
				v := <-c.Out()
				require.Equalf(t, i, v, "chan[%d]", chanCap)
			}
			v := <-c.Out()
			require.Equalf(t, 0, v, "chan[%d]", chanCap)
			v, ok := <-c.Out()
			require.Equalf(t, 0, v, "chan[%d]", chanCap)
			require.Falsef(t, ok, "chan[%d]", chanCap)
		}

		{
			// Ensure that close unblocks receive.
			c := New[int](Cap(chanCap))
			done := make(chan bool)
			go func() {
				v, ok := <-c.Out()
				done <- v == 0 && ok == false
			}()
			time.Sleep(time.Millisecond)
			c.Close()
			require.Truef(t, <-done, "chan[%d]: received non zero from closed chan", chanCap)
		}

		{
			// Send 100 integers,
			// ensure that we receive them non-corrupted in FIFO order.
			c := New[int](Cap(chanCap))
			go func() {
				for i := 0; i < 100; i++ {
					c.In() <- i
				}
			}()
			for i := 0; i < 100; i++ {
				v := <-c.Out()
				require.Equalf(t, i, v, "chan[%d]", chanCap)
			}

			// Same, but using recv2.
			go func() {
				for i := 0; i < 100; i++ {
					c.In() <- i
				}
			}()
			for i := 0; i < 100; i++ {
				v, ok := <-c.Out()
				require.Truef(t, ok, "chan[%d]: receive failed, expected %v", chanCap, i)
				require.Equalf(t, i, v, "chan[%d]", chanCap)
			}

			// Send 1000 integers in 4 goroutines,
			// ensure that we receive what we send.
			const P = 4
			const L = 1000
			for p := 0; p < P; p++ {
				go func() {
					for i := 0; i < L; i++ {
						c.In() <- i
					}
				}()
			}
			done := New[map[int]int](Cap(0))
			for p := 0; p < P; p++ {
				go func() {
					recv := make(map[int]int)
					for i := 0; i < L; i++ {
						v := <-c.Out()
						recv[v] = recv[v] + 1
					}
					done.In() <- recv
				}()
			}
			recv := make(map[int]int)
			for p := 0; p < P; p++ {
				for k, v := range <-done.Out() {
					recv[k] = recv[k] + v
				}
			}
			require.Lenf(t, recv, L, "chan[%d]", chanCap)
			for _, v := range recv {
				require.Equalf(t, P, v, "chan[%d]", chanCap)
			}
		}

		{
			// Test len/cap.
			c := New[int](Cap(chanCap))
			require.Equalf(t, 0, c.Len(), "chan[%d]", chanCap)
			require.Equalf(t, chanCap, c.Cap(), "chan[%d]", chanCap)
			for i := 0; i < chanCap; i++ {
				c.In() <- i
			}
			require.Equalf(t, chanCap, c.Len(), "chan[%d]", chanCap)
			require.Equalf(t, chanCap, c.Cap(), "chan[%d]", chanCap)
		}
	}
}

func TestNonblockRecvRace(t *testing.T) {
	n := 10000
	if testing.Short() {
		n = 100
	}
	for i := 0; i < n; i++ {
		c := New[int](Cap(1))
		c.In() <- 1
		t.Log(i)
		go func() {
			select {
			case <-c.Out():
			default:
				t.Error("chan is not ready")
			}
		}()
		c.Close()
		<-c.Out()
		if t.Failed() {
			return
		}
	}
}

const internalCacheSize = 16 + 1<<10

// This test checks that select acts on the state of the channels at one
// moment in the execution, not over a smeared time window.
// In the test, one goroutine does:
//
//	create c1, c2
//	make c1 ready for receiving
//	create second goroutine
//	make c2 ready for receiving
//
// The second goroutine does a non-blocking select receiving from c1 and c2.
// From the time the second goroutine is created, at least one of c1 and c2
// is always ready for receiving, so the select in the second goroutine must
// always receive from one or the other. It must never execute the default case.
func TestNonblockSelectRace(t *testing.T) {
	n := 1000
	done := New[bool](Cap(1))
	for i := 0; i < n; i++ {
		c1 := New[int]()
		c2 := New[int]()
		// The input channel of an unbounded buffer have an internal
		// cache queue. When the input channel and the internal cache
		// queue both gets full, we are certain that once the next send
		// is complete, the out will be available for sure hence the
		// waiting time of a receive is bounded.
		for i := 0; i < internalCacheSize; i++ {
			c1.In() <- 1
		}
		c1.In() <- 1
		go func() {
			runtime.Gosched()
			select {
			case <-c1.Out():
			case <-c2.Out():
			default:
				done.In() <- false
				return
			}
			done.In() <- true
		}()
		// Same for c2
		for i := 0; i < internalCacheSize; i++ {
			c2.In() <- 1
		}
		c2.In() <- 1
		select {
		case <-c1.Out():
		default:
		}
		require.Truef(t, <-done.Out(), "no chan is ready")
		c1.Close()
		// Drop all events.
		for range c1.Out() {
		}
		c2.Close()
		for range c2.Out() {
		}
	}
}

// Same as TestNonblockSelectRace, but close(c2) replaces c2 <- 1.
func TestNonblockSelectRace2(t *testing.T) {
	n := 1000
	done := make(chan bool, 1)
	for i := 0; i < n; i++ {
		c1 := New[int]()
		c2 := New[int]()
		// See TestNonblockSelectRace.
		for i := 0; i < internalCacheSize; i++ {
			c1.In() <- 1
		}
		c1.In() <- 1
		go func() {
			select {
			case <-c1.Out():
			case <-c2.Out():
			default:
				done <- false
				return
			}
			done <- true
		}()
		c2.Close()
		select {
		case <-c1.Out():
		default:
		}
		require.Truef(t, <-done, "no chan is ready")
		c1.Close()
		// Drop all events.
		for range c1.Out() {
		}
	}
}

func TestUnboundedChann(t *testing.T) {
	N := 200
	if testing.Short() {
		N = 20
	}

	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		t.Run("interface{}", func(t *testing.T) {
			t.Run("send", func(t *testing.T) {
				// Ensure send to an unbounded channel does not block.
				c := New[interface{}]()
				blocked := false
				wg.Add(1)
				go func() {
					defer wg.Done()
					select {
					case c.In() <- true:
					default:
						blocked = true
					}
				}()
				wg.Wait()
				require.Falsef(t, blocked, "send op to an unbounded channel blocked")
				c.Close()
			})

			t.Run("recv", func(t *testing.T) {
				// Ensure that receive op from unbounded chan can happen on
				// the same goroutine of send op.
				c := New[interface{}]()
				wg.Add(1)
				go func() {
					defer wg.Done()
					c.In() <- true
					<-c.Out()
				}()
				wg.Wait()
				c.Close()
			})
			t.Run("order", func(t *testing.T) {
				// Ensure that the unbounded channel processes everything FIFO.
				c := New[interface{}]()
				for i := 0; i < 1<<11; i++ {
					c.In() <- i
				}
				for i := 0; i < 1<<11; i++ {
					val := <-c.Out()
					require.Equalf(
						t,
						i,
						val,
						"unbounded channel passes messages in a non-FIFO order",
					)
				}
				c.Close()
			})
		})
		t.Run("struct{}", func(t *testing.T) {
			t.Run("send", func(t *testing.T) {
				// Ensure send to an unbounded channel does not block.
				c := New[struct{}]()
				blocked := false
				wg.Add(1)
				go func() {
					defer wg.Done()
					select {
					case c.In() <- struct{}{}:
					default:
						blocked = true
					}
				}()
				<-c.Out()
				wg.Wait()
				require.Falsef(t, blocked, "send op to an unbounded channel blocked")
				c.Close()
			})

			t.Run("recv", func(t *testing.T) {
				// Ensure that receive op from unbounded chan can happen on
				// the same goroutine of send op.
				c := New[struct{}]()
				wg.Add(1)
				go func() {
					defer wg.Done()
					c.In() <- struct{}{}
					<-c.Out()
				}()
				wg.Wait()
				c.Close()
			})
			t.Run("order", func(t *testing.T) {
				// Ensure that the unbounded channel processes everything FIFO.
				c := New[struct{}]()
				for i := 0; i < 1<<11; i++ {
					c.In() <- struct{}{}
				}
				n := 0
				for i := 0; i < 1<<11; i++ {
					if _, ok := <-c.Out(); ok {
						n++
					}
				}
				require.Equalf(t, 1<<11, n, "unbounded channel missed a message")
				c.Close()
			})
		})
	}
}

func TestUnboundedChannClose(t *testing.T) {
	t.Run("close-status", func(t *testing.T) {
		ch := New[any]()
		for i := 0; i < 100; i++ {
			ch.In() <- 0
		}
		ch.Close()
		go func() {
			for range ch.Out() {
			}
		}()

		// Theoretically, this is not a dead loop. If the channel
		// is closed, then this loop must terminate at somepoint.
		// If not, we will meet timeout in the test.
		for !ch.isClosed() {
			t.Log("unbounded channel is still not entirely closed")
		}
	})
	t.Run("struct{}", func(t *testing.T) {
		grs := runtime.NumGoroutine()
		N := 10
		n := 0
		done := make(chan struct{})
		ch := New[struct{}]()
		for i := 0; i < N; i++ {
			ch.In() <- struct{}{}
		}
		go func() {
			for range ch.Out() {
				n++
			}
			done <- struct{}{}
		}()
		ch.Close()
		<-done
		runtime.GC()
		require.LessOrEqualf(t, runtime.NumGoroutine(), grs+2, "leaking goroutines: %v", n)
		require.Equalf(t, N, n, "After close, not all elements are received")
	})

	t.Run("interface{}", func(t *testing.T) {
		grs := runtime.NumGoroutine()
		N := 10
		n := 0
		done := make(chan struct{})
		ch := New[interface{}]()
		for i := 0; i < N; i++ {
			ch.In() <- true
		}
		go func() {
			for range ch.Out() {
				n++
			}
			done <- struct{}{}
		}()
		ch.Close()
		<-done
		runtime.GC()
		require.LessOrEqualf(t, runtime.NumGoroutine(), grs+2, "leaking goroutines: %v", n)
		require.Equalf(t, N, n, "After close, not all elements are received")
	})
}

func BenchmarkUnboundedChann(b *testing.B) {
	b.Run("interface{}", func(b *testing.B) {
		b.Run("sync", func(b *testing.B) {
			c := New[interface{}]()
			defer c.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.In() <- struct{}{}
				<-c.Out()
			}
		})
		b.Run("chann", func(b *testing.B) {
			c := New[interface{}]()
			defer c.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				go func() { c.In() <- struct{}{} }()
				<-c.Out()
			}
		})
	})
	b.Run("struct{}", func(b *testing.B) {
		b.Run("sync", func(b *testing.B) {
			c := New[struct{}]()
			defer c.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.In() <- struct{}{}
				<-c.Out()
			}
		})
		b.Run("chann", func(b *testing.B) {
			c := New[struct{}]()
			defer c.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				go func() { c.In() <- struct{}{} }()
				<-c.Out()
			}
		})
	})
}
