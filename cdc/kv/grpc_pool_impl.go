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

package kv

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	gbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	// The default max number of TiKV concurrent streams in each connection is 1024
	grpcConnCapacity = 1000

	// resizeBucket means how many buckets will be extended when resizing an conn array
	resizeBucketStep = 2

	updateMetricInterval = 1 * time.Minute
	recycleConnInterval  = 10 * time.Minute
)

// connHeap is an heap of sharedConn
type connHeap struct {
	// target is TiKV storage address
	target string

	mu    sync.Mutex
	conns []*sharedConn
}

// the following five methods implement heap.Interface
func (ch *connHeap) Len() int {
	return len(ch.conns)
}

func (ch *connHeap) Less(i, j int) bool {
	return ch.conns[i].active <= ch.conns[j].active
}

func (ch *connHeap) Swap(i, j int) {
	h := ch.conns
	h[i], h[j] = h[j], h[i]
}

func (ch *connHeap) Push(e interface{}) {
	ch.conns = append(ch.conns, e.(*sharedConn))
}

func (ch *connHeap) Pop() interface{} {
	n := len(ch.conns)
	e := ch.conns[n-1]
	ch.conns = ch.conns[:n-1]
	return e
}

func newConnHeap(target string) *connHeap {
	return &connHeap{target: target}
}

// resize increases conn array size by `size` parameter
func (ch *connHeap) resize(ctx context.Context, credential *security.Credential, size int) error {
	conns := make([]*sharedConn, 0, size)
	for i := 0; i < size; i++ {
		conn, err := createClientConn(ctx, credential, ch.target)
		if err != nil {
			return err
		}
		conns = append(conns, &sharedConn{ClientConn: conn, active: 0})
	}
	for _, conn := range conns {
		heap.Push(ch, conn)
	}
	return nil
}

// getNext gets next available sharedConn, if all conns are not available, scale
// the connArray to double size.
func (ch *connHeap) getNext(ctx context.Context, credential *security.Credential) (*sharedConn, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if len(ch.conns) == 0 {
		err := ch.resize(ctx, credential, resizeBucketStep)
		if err != nil {
			return nil, err
		}
	}

	if ch.conns[0].active >= grpcConnCapacity {
		heap.Init(ch)
		if ch.conns[0].active >= grpcConnCapacity {
			// if there is no available conn, increase connArray size by 2.
			err := ch.resize(ctx, credential, resizeBucketStep)
			if err != nil {
				return nil, err
			}
		}
	}
	conn := ch.conns[0]
	conn.active++
	heap.Fix(ch, 0)
	return conn, nil
}

// recycle removes idle sharedConn, return true if no active gPRC connections remained.
func (ch *connHeap) recycle() (empty bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	i := 0
	for _, conn := range ch.conns {
		if conn.active > 0 {
			ch.conns[i] = conn
			i++
		} else {
			// tear down this grpc.ClientConn, we don't use it anymore, the returned
			// not-nil error can be ignored
			conn.Close() //nolint:errcheck
		}
	}
	// erasing truncated values
	for j := i; j < len(ch.conns); j++ {
		ch.conns[j] = nil
	}
	ch.conns = ch.conns[:i]
	heap.Init(ch)
	return len(ch.conns) == 0
}

func (ch *connHeap) activeCount() (count int64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, conn := range ch.conns {
		count += conn.active
	}
	return
}

// close tears down all ClientConns maintained in connArray
func (ch *connHeap) close() {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, conn := range ch.conns {
		// tear down this grpc.ClientConn, we don't use it anymore, the returned
		// not-nil error can be ignored
		conn.Close() //nolint:errcheck
	}
}

func createClientConn(ctx context.Context, credential *security.Credential, target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		target,
		grpcTLSOption,
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
		grpc.WithUnaryInterceptor(grpcMetrics.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(grpcMetrics.StreamClientInterceptor()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: gbackoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGRPCDialFailed, err)
	}
	return conn, nil
}

// GrpcPoolImpl implement GrpcPool interface
type GrpcPoolImpl struct {
	poolMu sync.RWMutex
	// bucketConns maps from TiKV store address to a connArray, which stores a
	// a slice of gRPC connections.
	bucketConns map[string]*connHeap

	credential *security.Credential

	// lifecycles of all gPRC connections are bounded to this context
	ctx context.Context
}

// NewGrpcPoolImpl creates a new GrpcPoolImpl instance
func NewGrpcPoolImpl(ctx context.Context, credential *security.Credential) *GrpcPoolImpl {
	return &GrpcPoolImpl{
		credential:  credential,
		bucketConns: make(map[string]*connHeap),
		ctx:         ctx,
	}
}

// GetConn implements GrpcPool.GetConn
func (pool *GrpcPoolImpl) GetConn(addr string) (*sharedConn, error) {
	pool.poolMu.Lock()
	defer pool.poolMu.Unlock()
	if _, ok := pool.bucketConns[addr]; !ok {
		pool.bucketConns[addr] = newConnHeap(addr)
	}
	return pool.bucketConns[addr].getNext(pool.ctx, pool.credential)
}

// ReleaseConn implements GrpcPool.ReleaseConn
func (pool *GrpcPoolImpl) ReleaseConn(sc *sharedConn, addr string) {
	pool.poolMu.RLock()
	defer pool.poolMu.RUnlock()
	if bucket, ok := pool.bucketConns[addr]; !ok {
		log.Warn("resource is not found in grpc pool", zap.String("addr", addr))
	} else {
		bucket.mu.Lock()
		sc.active--
		bucket.mu.Unlock()
	}
}

// RecycleConn implements GrpcPool.RecycleConn
func (pool *GrpcPoolImpl) RecycleConn(ctx context.Context) {
	recycleTicker := time.NewTicker(recycleConnInterval)
	defer recycleTicker.Stop()
	metricTicker := time.NewTicker(updateMetricInterval)
	defer metricTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-recycleTicker.C:
			pool.poolMu.Lock()
			for addr, bucket := range pool.bucketConns {
				empty := bucket.recycle()
				if empty {
					log.Info("recycle connections in grpc pool", zap.String("address", addr))
					delete(pool.bucketConns, addr)
					grpcPoolStreamGauge.DeleteLabelValues(addr)
				}
			}
			pool.poolMu.Unlock()
		case <-metricTicker.C:
			pool.poolMu.RLock()
			for addr, bucket := range pool.bucketConns {
				grpcPoolStreamGauge.WithLabelValues(addr).Set(float64(bucket.activeCount()))
			}
			pool.poolMu.RUnlock()
		}
	}
}

// Close implements GrpcPool.Close
func (pool *GrpcPoolImpl) Close() {
	pool.poolMu.Lock()
	defer pool.poolMu.Unlock()
	for _, bucket := range pool.bucketConns {
		bucket.close()
	}
}
