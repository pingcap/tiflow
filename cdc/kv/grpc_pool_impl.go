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
	defaultConnBucket = 8
	defaultCapacity   = 1000
)

// connArray is an array of sharedConn
type connArray struct {
	// target is TiKV storage address
	target string
	mu     sync.Mutex
	conns  []*sharedConn
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
		err := ca.resize(ctx, credential, 2)
		if err != nil {
			return nil, err
		}
	}
	for current := ca.next; current < ca.next+len(ca.conns); current++ {
		conn := ca.conns[current%len(ca.conns)]
		if conn.active < defaultCapacity {
			conn.active++
			ca.next = (current + 1) % len(ca.conns)
			return conn, nil
		}
	}

	current := len(ca.conns)
	// if there is no available conn, increase connArray size by 2.
	err := ca.resize(ctx, credential, 2)
	if err != nil {
		return nil, err
	}
	ca.conns[current].active++
	ca.next = current + 1
	return ca.conns[current], nil
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

func getBucket(tableID int64) int {
	// plus defaultConnBucket in case of tableID = -1
	return int(tableID+defaultConnBucket) % defaultConnBucket
}

// GrpcPoolImpl implement GrpcPool interface
type GrpcPoolImpl struct {
	poolMu sync.RWMutex
	// bucketConns maps from TiKV store address to a slice of connArray, we call
	// it connArray bucket.
	// Each table will be mapped to a determinated bucket and GetConn will only
	// return gRPC connections within this bucket, which means regions from the
	// same table tend to use the same gRPC connection.
	bucketConns map[string][]*connArray
	credential  *security.Credential
}

// NewGrpcPoolImpl creates a new GrpcPoolImpl instance
func NewGrpcPoolImpl(credential *security.Credential) *GrpcPoolImpl {
	return &GrpcPoolImpl{
		credential:  credential,
		bucketConns: make(map[string][]*connArray),
	}
}

// GetConn implements GrpcPool.GetConn
func (pool *GrpcPoolImpl) GetConn(ctx context.Context, addr string, tableID int64) (*sharedConn, error) {
	pool.poolMu.Lock()
	defer pool.poolMu.Unlock()
	if _, ok := pool.bucketConns[addr]; !ok {
		bucketConn := make([]*connArray, 0, defaultConnBucket)
		for i := 0; i < defaultConnBucket; i++ {
			bucketConn = append(bucketConn, newConnArray(addr))
		}
		pool.bucketConns[addr] = bucketConn
	}
	index := getBucket(tableID)
	return pool.bucketConns[addr][index].getNext(ctx, pool.credential)
}

// ReleaseConn implements GrpcPool.ReleaseConn
func (pool *GrpcPoolImpl) ReleaseConn(sc *sharedConn, addr string, tableID int64) {
	pool.poolMu.RLock()
	defer pool.poolMu.RUnlock()
	if bucket, ok := pool.bucketConns[addr]; !ok {
		log.Warn("resource is not found in grpc pool", zap.String("addr", addr))
	} else {
		index := getBucket(tableID)
		array := bucket[index]
		array.mu.Lock()
		sc.active--
		array.mu.Unlock()
	}
}

// Close implements GrpcPool.Close
func (pool *GrpcPoolImpl) Close() {
	pool.poolMu.Lock()
	defer pool.poolMu.Unlock()
	for _, bucket := range pool.bucketConns {
		for _, ca := range bucket {
			ca.close()
		}
	}
}
