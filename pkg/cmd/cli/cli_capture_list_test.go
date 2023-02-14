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

package cli

import (
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	"github.com/pingcap/tiflow/pkg/api/v1/mock"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	v2mock "github.com/pingcap/tiflow/pkg/api/v2/mock"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/stretchr/testify/require"
)

type mockAPIV1Client struct {
	apiv1client.APIV1Interface
	changefeeds apiv1client.ChangefeedInterface
	processor   apiv1client.ProcessorInterface
	status      apiv2client.StatusInterface
}

func (f *mockAPIV1Client) Processors() apiv1client.ProcessorInterface {
	return f.processor
}

func (f *mockAPIV1Client) Changefeeds() apiv1client.ChangefeedInterface {
	return f.changefeeds
}

type mockAPIV2Client struct {
	apiv2client.APIV2Interface
	tso         apiv2client.TsoInterface
	changefeeds apiv2client.ChangefeedInterface
	unsafes     apiv2client.UnsafeInterface
	captures    apiv2client.CaptureInterface
	processors  apiv2client.ProcessorInterface
}

func (f *mockAPIV2Client) Changefeeds() apiv2client.ChangefeedInterface {
	return f.changefeeds
}

func (f *mockAPIV2Client) Tso() apiv2client.TsoInterface {
	return f.tso
}

func (f *mockAPIV2Client) Unsafe() apiv2client.UnsafeInterface {
	return f.unsafes
}

func (f *mockAPIV2Client) Captures() apiv2client.CaptureInterface {
	return f.captures
}

func (f *mockAPIV2Client) Processors() apiv2client.ProcessorInterface {
	return f.processors
}

type mockFactory struct {
	factory.Factory
	captures    *v2mock.MockCaptureInterface
	changefeeds *mock.MockChangefeedInterface
	processor   *mock.MockProcessorInterface
	status      *v2mock.MockStatusInterface

	changefeedsv2 *v2mock.MockChangefeedInterface
	tso           *v2mock.MockTsoInterface
	unsafes       *v2mock.MockUnsafeInterface
	processorsv2  *v2mock.MockProcessorInterface
}

func newMockFactory(ctrl *gomock.Controller) *mockFactory {
	cps := v2mock.NewMockCaptureInterface(ctrl)
	processor := mock.NewMockProcessorInterface(ctrl)
	cf := mock.NewMockChangefeedInterface(ctrl)
	status := v2mock.NewMockStatusInterface(ctrl)
	unsafes := v2mock.NewMockUnsafeInterface(ctrl)
	tso := v2mock.NewMockTsoInterface(ctrl)
	cfv2 := v2mock.NewMockChangefeedInterface(ctrl)
	processorsv2 := v2mock.NewMockProcessorInterface(ctrl)
	return &mockFactory{
		captures:      cps,
		changefeeds:   cf,
		processor:     processor,
		status:        status,
		changefeedsv2: cfv2,
		tso:           tso,
		unsafes:       unsafes,
		processorsv2:  processorsv2,
	}
}

func (f *mockFactory) APIV1Client() (apiv1client.APIV1Interface, error) {
	return &mockAPIV1Client{
		changefeeds: f.changefeeds,
		status:      f.status,
		processor:   f.processor,
	}, nil
}

func (f *mockFactory) APIV2Client() (apiv2client.APIV2Interface, error) {
	return &mockAPIV2Client{
		captures:    f.captures,
		changefeeds: f.changefeedsv2,
		tso:         f.tso,
		unsafes:     f.unsafes,
		processors:  f.processorsv2,
	}, nil
}

func TestCaptureListCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := v2mock.NewMockCaptureInterface(ctrl)
	f := &mockFactory{captures: cf}
	cmd := newCmdListCapture(f)
	cf.EXPECT().List(gomock.Any()).Return([]model.Capture{
		{
			ID:            "owner",
			IsOwner:       true,
			AdvertiseAddr: "127.0.0.1:8300",
		},
	}, nil)
	os.Args = []string{"list"}
	require.Nil(t, cmd.Execute())

	cf.EXPECT().List(gomock.Any()).Return(nil, errors.New("test"))
	o := newListCaptureOptions()
	o.complete(f)
	require.NotNil(t, o.run(cmd))
}
