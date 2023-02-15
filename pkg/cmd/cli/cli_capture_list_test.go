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
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/api/v2/mock"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/stretchr/testify/require"
)

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
	captures    *mock.MockCaptureInterface
	changefeeds *mock.MockChangefeedInterface
	processors  *mock.MockProcessorInterface
	status      *mock.MockStatusInterface
	tso         *mock.MockTsoInterface
	unsafes     *mock.MockUnsafeInterface
}

func newMockFactory(ctrl *gomock.Controller) *mockFactory {
	cps := mock.NewMockCaptureInterface(ctrl)
	processor := mock.NewMockProcessorInterface(ctrl)
	cf := mock.NewMockChangefeedInterface(ctrl)
	statuses := mock.NewMockStatusInterface(ctrl)
	unsafes := mock.NewMockUnsafeInterface(ctrl)
	tso := mock.NewMockTsoInterface(ctrl)
	return &mockFactory{
		captures:    cps,
		changefeeds: cf,
		processors:  processor,
		status:      statuses,
		tso:         tso,
		unsafes:     unsafes,
	}
}

func (f *mockFactory) APIV2Client() (apiv2client.APIV2Interface, error) {
	return &mockAPIV2Client{
		captures:    f.captures,
		changefeeds: f.changefeeds,
		tso:         f.tso,
		unsafes:     f.unsafes,
		processors:  f.processors,
	}, nil
}

func TestCaptureListCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockCaptureInterface(ctrl)
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
