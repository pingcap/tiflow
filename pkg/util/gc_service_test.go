package util

import (
	"context"
	"math"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/tikv/pd/client"
)

type gcServiceuite struct {
	pdCli mockPdClientForServiceGCSafePoint
}

var _ = check.Suite(&gcServiceuite{
	mockPdClientForServiceGCSafePoint{serviceSafePoint: make(map[string]uint64)},
})

func (s *gcServiceuite) TestCheckSafetyOfStartTs(c *check.C) {
	ctx := context.Background()
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service1", 10, 60)
	err := CheckSafetyOfStartTs(ctx, s.pdCli, 50)
	c.Assert(err.Error(), check.Equals, "startTs less than gcSafePoint: [tikv:9006]GC life time is shorter than transaction duration, transaction starts at 50, GC safe point is 60")
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service2", 10, 80)
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service3", 10, 70)
	err = CheckSafetyOfStartTs(ctx, s.pdCli, 65)
	c.Assert(err, check.IsNil)
	c.Assert(s.pdCli.serviceSafePoint, check.DeepEquals, map[string]uint64{"service1": 60, "service2": 80, "service3": 70, "ticdc-changefeed-creating": 65})

}

type mockPdClientForServiceGCSafePoint struct {
	baseMockPdClient
	serviceSafePoint map[string]uint64
}

func (m mockPdClientForServiceGCSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	minSafePoint := uint64(math.MaxUint64)
	for _, safePoint := range m.serviceSafePoint {
		if minSafePoint > safePoint {
			minSafePoint = safePoint
		}
	}
	if safePoint < minSafePoint && len(m.serviceSafePoint) != 0 {
		return minSafePoint, nil
	}
	m.serviceSafePoint[serviceID] = safePoint
	return minSafePoint, nil
}

type baseMockPdClient struct {
}

// GetClusterID gets the cluster ID from PD.
func (m baseMockPdClient) GetClusterID(ctx context.Context) uint64 {
	panic("not implemented")
}

// GetLeaderAddr returns current leader's address. It returns "" before
// syncing leader from server.
func (m baseMockPdClient) GetLeaderAddr() string {
	panic("not implemented")
}

// GetTS gets a timestamp from PD.
func (m baseMockPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	panic("not implemented")
}

// GetTSAsync gets a timestamp from PD, without block the caller.
func (m baseMockPdClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	panic("not implemented")
}

// GetRegion gets a region and its leader Peer from PD by key.
// The region may expire after split. Caller is responsible for caching and
// taking care of region change.
// Also it may return nil if PD finds no Region for the key temporarily,
// client should retry later.
func (m baseMockPdClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	panic("not implemented")
}

// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
func (m baseMockPdClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	panic("not implemented")
}

// GetRegionByID gets a region and its leader Peer from PD by id.
func (m baseMockPdClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	panic("not implemented")
}

// ScanRegion gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
// If a region has no leader, corresponding leader will be placed by a peer
// with empty value (PeerID is 0).
func (m baseMockPdClient) ScanRegions(ctx context.Context, key []byte, endKey []byte, limit int) ([]*metapb.Region, []*metapb.Peer, error) {
	panic("not implemented")
}

// GetStore gets a store from PD by store id.
// The store may expire later. Caller is responsible for caching and taking care
// of store change.
func (m baseMockPdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	panic("not implemented")
}

// GetAllStores gets all stores from pd.
// The store may expire later. Caller is responsible for caching and taking care
// of store change.
func (m baseMockPdClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	panic("not implemented")
}

// Update GC safe point. TiKV will check it and do GC themselves if necessary.
// If the given safePoint is less than the current one, it will not be updated.
// Returns the new safePoint after updating.
func (m baseMockPdClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("not implemented")
}

// UpdateServiceGCSafePoint updates the safepoint for specific service and
// returns the minimum safepoint across all services, this value is used to
// determine the safepoint for multiple services, it does not tigger a GC
// job. Use UpdateGCSafePoint to trigger the GC job if needed.
func (m baseMockPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("not implemented")
}

// ScatterRegion scatters the specified region. Should use it for a batch of regions,
// and the distribution of these regions will be dispersed.
func (m baseMockPdClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	panic("not implemented")
}

// GetOperator gets the status of operator of the specified region.
func (m baseMockPdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	panic("not implemented")
}

// Close closes the client.
func (m baseMockPdClient) Close() {
	panic("not implemented")
}
