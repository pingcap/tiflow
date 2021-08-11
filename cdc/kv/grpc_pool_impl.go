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
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
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

	recycleConnInterval = 10 * time.Minute
)

// connArray is an array of sharedConn
type connArray struct {
	// target is TiKV storage address
	target string

	mu    sync.Mutex
	conns []*sharedConn

	// next is used for fetching sharedConn in a round robin way
	next int
}

func newConnArray(target string) *connArray {
	return &connArray{target: target}
}

// resize increases conn array size by `size` parameter
func (ca *connArray) resize(ctx context.Context, credential *security.Credential, size int) error {
	conns := make([]*sharedConn, 0, size)
	for i := 0; i < size; i++ {
		conn, err := createClientConn(ctx, credential, ca.target)
		if err != nil {
			return err
		}
		conns = append(conns, &sharedConn{ClientConn: conn, active: 0})
	}
	ca.conns = append(ca.conns, conns...)
	return nil
}

func createClientConn(ctx context.Context, credential *security.Credential, target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

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
	cancel()

	if err != nil {
		err2 := conn.Close()
		if err2 != nil {
			log.Warn("close grpc conn", zap.Error(err2))
		}
		return nil, cerror.WrapError(cerror.ErrGRPCDialFailed, err)
	}
	return conn, nil
}

// getNext gets next available sharedConn, if all conns are not available, scale
// the connArray to double size.
func (ca *connArray) getNext(ctx context.Context, credential *security.Credential) (*sharedConn, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if len(ca.conns) == 0 {
		err := ca.resize(ctx, credential, resizeBucketStep)
		if err != nil {
			return nil, err
		}
	}
	for current := ca.next; current < ca.next+len(ca.conns); current++ {
		conn := ca.conns[current%len(ca.conns)]
		if conn.active < grpcConnCapacity {
			conn.active++
			ca.next = (current + 1) % len(ca.conns)
			return conn, nil
		}
	}

	current := len(ca.conns)
	// if there is no available conn, increase connArray size by 2.
	err := ca.resize(ctx, credential, resizeBucketStep)
	if err != nil {
		return nil, err
	}
	ca.conns[current].active++
	ca.next = current + 1
	return ca.conns[current], nil
}

// recycle removes idle sharedConn, return true if no active gPRC connections remained.
func (ca *connArray) recycle() (empty bool) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	i := 0
	for _, sc := range ca.conns {
		if sc.active > 0 {
			ca.conns[i] = sc
			i++
		}
	}
	// erasing truncated values
	for j := i; j < len(ca.conns); j++ {
		ca.conns[i] = nil
	}
	ca.conns = ca.conns[:i]
	return len(ca.conns) == 0
}

// close tears down all ClientConns maintained in connArray
func (ca *connArray) close() {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	for _, conn := range ca.conns {
		// tear down this grpc.ClientConn, we don't use it anymore, the returned
		// not-nil error can be ignored
		conn.Close() //nolint:errcheck
	}
}

// GrpcPoolImpl implement GrpcPool interface
type GrpcPoolImpl struct {
	poolMu sync.RWMutex
	// bucketConns maps from TiKV store address to a connArray, which stores a
	// a slice of gRPC connections.
	bucketConns map[string]*connArray
	credential  *security.Credential
}

// NewGrpcPoolImpl creates a new GrpcPoolImpl instance
func NewGrpcPoolImpl(credential *security.Credential) *GrpcPoolImpl {
	return &GrpcPoolImpl{
		credential:  credential,
		bucketConns: make(map[string]*connArray),
	}
}

// GetConn implements GrpcPool.GetConn
func (pool *GrpcPoolImpl) GetConn(ctx context.Context, addr string) (*sharedConn, error) {
	pool.poolMu.Lock()
	defer pool.poolMu.Unlock()
	if _, ok := pool.bucketConns[addr]; !ok {
		pool.bucketConns[addr] = newConnArray(addr)
	}
	return pool.bucketConns[addr].getNext(ctx, pool.credential)
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
	ticker := time.NewTicker(recycleConnInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pool.poolMu.Lock()
			for addr, bucket := range pool.bucketConns {
				empty := bucket.recycle()
				if empty {
					log.Info("recycle connections in grpc pool", zap.String("address", addr))
					delete(pool.bucketConns, addr)
				}
			}
			pool.poolMu.Unlock()
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
