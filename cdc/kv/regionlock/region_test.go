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

package regionlock

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/stretchr/testify/require"
)

func TestCheckRegionsLeftCover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		regions []*metapb.Region
		span    tablepb.Span
		cover   bool
	}{
		{
			regions: []*metapb.Region{},
			span:    tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}}, cover: false,
		},
		{regions: []*metapb.Region{
			{StartKey: nil, EndKey: nil},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}}, cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}}, cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{0}, EndKey: []byte{4}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}}, cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{3}}, cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{3}, EndKey: []byte{4}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{4}}, cover: false},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{4}}, cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{3}}, cover: false},
	}

	for _, tc := range cases {
		require.Equal(t, tc.cover, CheckRegionsLeftCover(tc.regions, tc.span))
	}
}

func TestCutRegionsLeftCoverSpan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		regions []*metapb.Region
		span    tablepb.Span
		covered []*metapb.Region
	}{
		{
			regions: []*metapb.Region{},
			span:    tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}},
			covered: nil,
		},
		{
			regions: []*metapb.Region{{StartKey: nil, EndKey: nil}},
			span:    tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}},
			covered: []*metapb.Region{{StartKey: nil, EndKey: nil}},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
			span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}},
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{0}, EndKey: []byte{4}},
			},
			span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{2}},
			covered: []*metapb.Region{
				{StartKey: []byte{0}, EndKey: []byte{4}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{3}},
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{3}, EndKey: []byte{4}},
			},
			span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{4}},
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span: tablepb.Span{StartKey: []byte{1}, EndKey: []byte{4}},
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span:    tablepb.Span{StartKey: []byte{1}, EndKey: []byte{3}},
			covered: nil,
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.covered, CutRegionsLeftCoverSpan(tc.regions, tc.span))
	}
}
