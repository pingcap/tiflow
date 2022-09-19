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

package s3

import (
	"context"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
)

// BucketName represents a name of an s3 bucket.
type BucketName = string

// BucketSelector is an interface for a selector of s3 bucket by scope.
type BucketSelector interface {
	GetBucket(ctx context.Context, scope internal.ResourceScope) (BucketName, error)
}

type constantBucketSelector struct {
	bucket BucketName
}

// NewConstantBucketSelector creates a BucketSelector that always return the same
// bucket name.
func NewConstantBucketSelector(bucketName BucketName) BucketSelector {
	return &constantBucketSelector{bucket: bucketName}
}

func (s *constantBucketSelector) GetBucket(
	_ context.Context,
	_ internal.ResourceScope,
) (BucketName, error) {
	return s.bucket, nil
}
