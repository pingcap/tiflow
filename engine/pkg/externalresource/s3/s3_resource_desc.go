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
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

type ResourceDescriptor struct {
	Options      *brStorage.S3BackendOptions
	Bucket       string
	ExecutorID   resModel.ExecutorID
	WorkerID     resModel.WorkerID
	ResourceName resModel.ResourceName
}

func (r *ResourceDescriptor) MakeExternalStorage(ctx context.Context) (brStorage.ExternalStorage, error) {
	uri := r.generateURI()
	opts := &brStorage.BackendOptions{
		S3: *r.Options,
	}
	backEnd, err := brStorage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret, err := brStorage.New(ctx, backEnd, &brStorage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func (r *ResourceDescriptor) URI() string {
	return r.generateURI()
}

func (r *ResourceDescriptor) generateURI() string {
	return fmt.Sprintf("s3:///%s/%s/%s/%s",
		url.QueryEscape(r.Bucket),
		url.QueryEscape(string(r.ExecutorID)),
		url.QueryEscape(r.WorkerID),
		url.QueryEscape(r.ResourceName))
}
