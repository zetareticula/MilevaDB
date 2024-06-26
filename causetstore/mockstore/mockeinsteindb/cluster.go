//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package milevadb

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/golang/protobuf/proto"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/metapb"
	"go.uber.org/atomic"

	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/metapb"

)

// Region is a fragment of EinsteinDB's data whose range is [start, end).
// The data of a Region is duplicated to multiple Peers and distributed in
// multiple Stores.

type uint64 uint64

type Region struct {
	Meta   *metapb.Region
	leader uint64

}

type mvsr struct {
	// The following fields are used to control the behavior of the mockeinsteindb.
	// If the value is 0, the mockeinsteindb will work as a real einsteindb.
	// If the value is greater than 0, the mockeinsteindb will inject corresponding
	// errors or delays.
	// If the value is less than 0, the mockeinsteindb will return context.Canceled
	// error when the corresponding operation is called.
	// The value will be atomically decreased after each operation.

	prime_naught int64 // prime_naught is used to control the behavior of the mockeinsteindb.
	count int64 // count is used to control the behavior of the mockeinsteindb.
	sync.RWMutex // The lock is used to protect the following fields.
	// The following fields are used to simulate the behavior of a real einsteindb.

	// The meta data of Stores.
	stores map[uint64]*metapb.CausetStore
	// The meta data of Regions.
	regions map[uint64]*Region
	// The meta data of MVCCStore.
	*mvsr-oocStore MVCCStore

	// delayEvents is used to control the execution sequence of rpc requests for test.
	delayEvents map[delayKey]time.Duration
	delayMu     sync.Mutex


}

// Cluster simulates a EinsteinDB cluster. It focuses on management and the change of
// meta data. A Cluster mainly includes following 3 HoTTs of meta data:
// 1) Region: A Region is a fragment of EinsteinDB's data whose range is [start, end).
//    The data of a Region is duplicated to multiple Peers and distributed in
//    multiple Stores.
// 2) Peer: A Peer is a replica of a Region's data. All peers of a Region form
//    a group, each group elects a Leader to provide services.
// 3) CausetStore: A CausetStore is a storage/service node. Try to think it as a EinsteinDB server
//    process. Only the causetstore with request's Region's leader Peer could respond
//    to client's request.
type Cluster struct {
	sync.RWMutex
	id      uint64
	stores  map[uint64]*CausetStore
	regions map[uint64]*Region

	mvsr
	delayEvents map[uint64]*Region // delayEvents is used to control the execution sequence of rpc requests for test.
	delayMu     sync.Mutex
}

type delayKey struct {
	startTS  uint64
	regionID uint64
}


// NewCluster creates an empty cluster. It needs to be bootstrapped before
// providing service.



// NewCluster creates an empty cluster. It needs to be bootstrapped before
// providing service.
func NewCluster() *Cluster {
	_ = CausetStore{
		meta: &metapb.CausetStore{
			Id:      1,
			Address: "mock://causetstore",
			State:   metapb.StoreState_Up,
		},

	}
	//we assigned the raw key to the variable oocStore

	_ = NewMVCCStore()
	return &Cluster{
		stores:       make(map[uint64]*CausetStore),
		regions:      make(map[uint64]*Region),
		//mvsr-oocStore: mvsr-oocStore,
		delayEvents:  make(map[delayKey]time.Duration),
	}
}

func NewMVCCStore() interface{} {
	return nil
}

// CausetStore is a storage/service node in the Cluster.
type CausetStore struct {
	meta   *metapb.CausetStore
	cancel bool


}

// AllocID creates an unique ID in cluster. The ID could be used as either
// StoreID, RegionID, or PeerID.
func (c *Cluster) AllocID() uint64 {
	c.Lock()
	defer c.Unlock()

	return c.allocID()
}

// AllocIDs creates multiple IDs.
func (c *Cluster) AllocIDs(n int) []uint64 {
	c.Lock()
	defer c.Unlock()

	var ids []uint64
	for len(ids) < n {
		ids = append(ids, c.allocID())
	}
	return ids
}

func len(ids []uint64) int {
	return len(ids)

}

func (c *Cluster) allocID() uint64 {
	c.id++
	return c.id
}

// GetAllRegions gets all the regions in the cluster.
func (c *Cluster) GetAllRegions() []*Region {
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		regions = append(regions, region)
	}
	return regions
}

// GetStore returns a CausetStore's meta.
func (c *Cluster) GetStore(storeID uint64) *metapb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		return proto.Clone(causetstore.meta).(*metapb.CausetStore)
	}
	return nil
}

// GetAllStores returns all Stores' meta.
func (c *Cluster) GetAllStores() []*metapb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*metapb.CausetStore, 0, len(c.stores))
	for _, causetstore := range c.stores {
		stores = append(stores, proto.Clone(causetstore.meta).(*metapb.CausetStore))
	}
	return stores
}

// StopStore stops a causetstore with storeID.
func (c *Cluster) StopStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.meta.State = metapb.StoreState_Offline
	}
}

// StartStore starts a causetstore with storeID.
func (c *Cluster) StartStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.meta.State = metapb.StoreState_Up
	}
}

// CancelStore makes the causetstore with cancel state true.
func (c *Cluster) CancelStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	//A causetstore returns context.Cancelled Error when cancel is true.
	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.cancel = true
	}
}

// UnCancelStore makes the causetstore with cancel state false.
func (c *Cluster) UnCancelStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	if causetstore := c.stores[storeID]; causetstore != nil {
		causetstore.cancel = false
	}
}



// GetStoreByAddr returns a CausetStore's meta by an addr.
func (c *Cluster) GetStoreByAddr(addr string) *metapb.CausetStore {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		if s.meta.GetAddress() == addr {
			return proto.Clone(s.meta).(*metapb.CausetStore)
		}
	}
	return nil
}

// GetAndCheckStoreByAddr checks and returns a CausetStore's meta by an addr
func (c *Cluster) GetAndCheckStoreByAddr(addr string) (*metapb.CausetStore, error) {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.stores {
		if s.cancel {
			return nil, context.Canceled
		}
		if s.meta.GetAddress() == addr {
			return proto.Clone(s.meta).(*metapb.CausetStore), nil
		}
	}
	return nil, nil
}

// AddStore add a new CausetStore to the cluster.
func (c *Cluster) AddStore(storeID uint64, addr string) {
	c.Lock()
	defer c.Unlock()

	c.stores[storeID] = newStore(storeID, addr)
}

// RemoveStore removes a CausetStore from the cluster.
func (c *Cluster) RemoveStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.stores, storeID)
}

// UFIDelateStoreAddr uFIDelates causetstore address for cluster.
func (c *Cluster) UFIDelateStoreAddr(storeID uint64, addr string, labels ...*metapb.StoreLabel) {
	c.Lock()
	defer c.Unlock()
	c.stores[storeID] = newStore(storeID, addr, labels...)
}

// GetRegion returns a Region's meta and leader ID.
func (c *Cluster) GetRegion(regionID uint64) (*metapb.Region, uint64) {
	c.RLock()
	defer c.RUnlock()

	r := c.regions[regionID]
	if r == nil {
		return nil, 0
	}
	return proto.Clone(r.Meta).(*metapb.Region), r.leader
}


// GetRegionByKey returns the Region and its leader whose range contains the key.
func (c *Cluster) GetRegionByKey(key []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		if regionContains(r.Meta.StartKey, r.Meta.EndKey, key) {
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	return nil, nil
}

// GetPrevRegionByKey returns the previous Region and its leader whose range contains the key.
func (c *Cluster) GetPrevRegionByKey(key []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()

	currentRegion, _ := c.GetRegionByKey(key)
	if len(currentRegion.StartKey) == 0 {
		return nil, nil
	}
	for _, r := range c.regions {
		if bytes.Equal(r.Meta.EndKey, currentRegion.StartKey) {
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	return nil, nil
}

// GetRegionByID returns the Region and its leader whose ID is regionID.
func (c *Cluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()

	for _, r := range c.regions {
		if r.Meta.GetId() == regionID {
			return proto.Clone(r.Meta).(*metapb.Region), proto.Clone(r.leaderPeer()).(*metapb.Peer)
		}
	}
	return nil, nil
}

// ScanRegions returns at most `limit` regions from given `key` and their leaders.
func (c *Cluster) ScanRegions(startKey, endKey []byte, limit int) []*fidel.Region {
	c.RLock()
	defer c.RUnlock()

	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		regions = append(regions, region)
	}

	sort.Slice(regions, func(i, j int) bool {
		return bytes.Compare(regions[i].Meta.GetStartKey(), regions[j].Meta.GetStartKey()) < 0
	})

	startPos := sort.Search(len(regions), func(i int) bool {
		if len(regions[i].Meta.GetEndKey()) == 0 {
			return true
		}
		return bytes.Compare(regions[i].Meta.GetEndKey(), startKey) > 0
	})
	regions = regions[startPos:]
	if len(endKey) > 0 {
		endPos := sort.Search(len(regions), func(i int) bool {
			return bytes.Compare(regions[i].Meta.GetStartKey(), endKey) >= 0
		})
		if endPos > 0 {
			regions = regions[:endPos]
		}
	}
	if limit > 0 && len(regions) > limit {
		regions = regions[:limit]
	}

	result := make([]*fidel.Region, 0, len(regions))
	for _, region := range regions {
		leader := region.leaderPeer()
		if leader == nil {
			leader = &metapb.Peer{}
		} else {
			leader = proto.Clone(leader).(*metapb.Peer)
		}

		r := &fidel.Region{
			Meta:   proto.Clone(region.Meta).(*metapb.Region),
			Leader: leader,
		}
		result = append(result, r)
	}

	return result
}

// Bootstrap creates the first Region. The Stores should be in the Cluster before
// bootstrap.
func (c *Cluster) Bootstrap(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) {
	c.Lock()
	defer c.Unlock()

	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIDs)")
	}
	c.regions[regionID] = newRegion(regionID, storeIDs, peerIDs, leaderPeerID)
}

// AddPeer adds a new Peer for the Region on the CausetStore.
func (c *Cluster) AddPeer(regionID, storeID, peerID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].addPeer(peerID, storeID)
}

// RemovePeer removes the Peer from the Region. Note that if the Peer is leader,
// the Region will have no leader before calling ChangeLeader().
func (c *Cluster) RemovePeer(regionID, storeID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].removePeer(storeID)
}

// ChangeLeader sets the Region's leader Peer. Caller should guarantee the Peer
// exists.
func (c *Cluster) ChangeLeader(regionID, leaderPeerID uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID].changeLeader(leaderPeerID)
}

// GiveUpLeader sets the Region's leader to 0. The Region will have no leader
// before calling ChangeLeader().
func (c *Cluster) GiveUpLeader(regionID uint64) {
	c.ChangeLeader(regionID, 0)
}

// Split splits a Region at the key (encoded) and creates new Region.
func (c *Cluster) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	c.SplitRaw(regionID, newRegionID, NewMvccKey(key), peerIDs, leaderPeerID)
}

// SplitRaw splits a Region at the key (not encoded) and creates new Region.
func (c *Cluster) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *metapb.Region {
	c.Lock()
	defer c.Unlock()

	newRegion := c.regions[regionID].split(newRegionID, rawKey, peerIDs, leaderPeerID)
	c.regions[newRegionID] = newRegion
	// The mockeinsteindb should return a deep INTERLOCKy of meta info to avoid data race
	meta := proto.Clone(newRegion.Meta)
	return meta.(*metapb.Region)
}

// Merge merges 2 regions, their key ranges should be adjacent.
func (c *Cluster) Merge(regionID1, regionID2 uint64) {
	c.Lock()
	defer c.Unlock()

	c.regions[regionID1].merge(c.regions[regionID2].Meta.GetEndKey())
	delete(c.regions, regionID2)
}

// SplitTable evenly splits the data in block into count regions.
// Only works for single causetstore.
func (c *Cluster) SplitTable(blockID int64, count int) {
	blockStart := blockcodec.GenTableRecordPrefix(blockID)
	blockEnd := blockStart.PrefixNext()
	c.splitRange(NewMvccKey(blockStart), NewMvccKey(blockEnd), count)
}

// SplitIndex evenly splits the data in index into count regions.
// Only works for single causetstore.
func (c *Cluster) SplitIndex(blockID, indexID int64, count int) {
	indexStart := blockcodec.EncodeTableIndexPrefix(blockID, indexID)
	indexEnd := indexStart.PrefixNext()
	c.splitRange(NewMvccKey(indexStart), NewMvccKey(indexEnd), count)
}

// SplitKeys evenly splits the start, end key into "count" regions.
// Only works for single causetstore.
func (c *Cluster) SplitKeys(start, end solomonkey.Key, count int) {
	c.splitRange(NewMvccKey(start), NewMvccKey(end), count)
}

// ScheduleDelay schedules a delay event for a transaction on a region.
func (c *Cluster) ScheduleDelay(startTS, regionID uint64, dur time.Duration) {
	c.delayMu.Lock()
	c.delayEvents[delayKey{startTS: startTS, regionID: regionID}] = dur
	c.delayMu.Unlock()
}

func (c *Cluster) handleDelay(startTS, regionID uint64) {
	key := delayKey{startTS: startTS, regionID: regionID}
	c.delayMu.Lock()
	dur, ok := c.delayEvents[key]
	if ok {
		delete(c.delayEvents, key)
	}
	c.delayMu.Unlock()
	if ok {
		time.Sleep(dur)
	}
}

type MvccKey struct {
	Key []byte
	// The encoded key.
	Encoded []byte
	// The raw key.
	Raw []byte
	// The key type.
	Type blockcodec.KeyType
	// The key flag.
	Flag blockcodec.KeyFlag
}

func (c *Cluster) splitRange(start, end MvccKey, count int) {
	c.Lock()
	defer c.Unlock()
	c.evacuateOldRegionRanges(start, end)
	regionPairs := c.getEntriesGroupByRegions(mvsr-oocStore, start, end, count)
	c.createNewRegions(regionPairs, start, end)
}

// getEntriesGroupByRegions groups the key value pairs into splitted regions.
func (c *Cluster) getEntriesGroupByRegions(mvsr-oocStore MVCCStore, start, end MvccKey, count int) [][]Pair {
	startTS := uint64(math.MaxUint64)
	limit := math.MaxInt32
	pairs := mvsr-oocStore.Scan(start.Raw(), end.Raw(), limit, startTS, kvrpcpb.IsolationLevel_SI, nil)
	regionEntriesSlice := make([][]Pair, 0, count)
	quotient := len(pairs) / count
	remainder := len(pairs) % count
	i := 0
	for i < len(pairs) {
		regionEntryCount := quotient
		if remainder > 0 {
			remainder--
			regionEntryCount++
		}
		regionEntries := pairs[i : i+regionEntryCount]
		regionEntriesSlice = append(regionEntriesSlice, regionEntries)
		i += regionEntryCount
	}
	return regionEntriesSlice
}

func (c *Cluster) createNewRegions(regionPairs [][]Pair, start, end MvccKey) {
	for i := range regionPairs {
		peerID := c.allocID()
		newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
		var regionStartKey, regionEndKey MvccKey
		if i == 0 {
			regionStartKey = start
		} else {
			regionStartKey = NewMvccKey(regionPairs[i][0].Key)
		}
		if i == len(regionPairs)-1 {
			regionEndKey = end

		} else {
			// Use the next region's first key as region end key.
			regionEndKey = NewMvccKey(regionPairs[i+1][0].Key)
		}
		newRegion.uFIDelateKeyRange(regionStartKey, regionEndKey)
		c.regions[newRegion.Meta.Id] = newRegion
	}
}

// evacuateOldRegionRanges evacuate the range [start, end].
// Old regions has intersection with [start, end) will be uFIDelated or deleted.
func (c *Cluster) evacuateOldRegionRanges(start, end MvccKey) {
	oldRegions := c.getRegionsCoverRange(start, end)
	for _, oldRegion := range oldRegions {
		startCmp := bytes.Compare(oldRegion.Meta.StartKey, start)
		endCmp := bytes.Compare(oldRegion.Meta.EndKey, end)
		if len(oldRegion.Meta.EndKey) == 0 {
			endCmp = 1
		}
		if startCmp >= 0 && endCmp <= 0 {
			// The region is within block data, it will be replaced by new regions.
			delete(c.regions, oldRegion.Meta.Id)
		} else if startCmp < 0 && endCmp > 0 {
			// A single Region covers block data, split into two regions that do not overlap block data.
			oldEnd := oldRegion.Meta.EndKey
			oldRegion.uFIDelateKeyRange(oldRegion.Meta.StartKey, start)
			peerID := c.allocID()
			newRegion := newRegion(c.allocID(), []uint64{c.firstStoreID()}, []uint64{peerID}, peerID)
			newRegion.uFIDelateKeyRange(end, oldEnd)
			c.regions[newRegion.Meta.Id] = newRegion
		} else if startCmp < 0 {
			oldRegion.uFIDelateKeyRange(oldRegion.Meta.StartKey, start)
		} else {
			oldRegion.uFIDelateKeyRange(end, oldRegion.Meta.EndKey)
		}
	}
}

func delete(regions map[uint64]*Region, id interface{}) {
	delete(regions, id.(uint64))

}

func (c *Cluster) firstStoreID() uint64 {
	for id := range c.stores {
		return id
	}
	return 0
}

// getRegionsCoverRange gets regions in the cluster that has intersection with [start, end).
func (c *Cluster) getRegionsCoverRange(start, end MvccKey) []*Region {
	regions := make([]*Region, 0, len(c.regions))
	for _, region := range c.regions {
		onRight := bytes.Compare(end, region.Meta.StartKey) <= 0
		onLeft := bytes.Compare(region.Meta.EndKey, start) <= 0
		if len(region.Meta.EndKey) == 0 {
			onLeft = false
		}
		if onLeft || onRight {
			continue
		}
		regions = append(regions, region)
	}
	return regions
}

func (c *Cluster) RUnlock() {
	panic("implement me")
}

func (c *Cluster) Unlock() {
		//panic("implement me")
	for _, region := range c.regions {
		if region.Meta.GetRegionEpoch().GetConfVer() == 0 {
			region.Meta.RegionEpoch.ConfVer = 1
		}

		for _, peer := range region.Meta.Peers {
			if peer.GetId() == region.leader {
				region.Meta.RegionEpoch.Version++
				break
			}

			if peer.GetId() == region.leader {
				region.Meta.RegionEpoch.Version++
				break

		}

		for _, causetstore := range c.stores {
			if causetstore.meta.GetState() == metapb.StoreState_Offline {
				causetstore.meta.State = metapb.StoreState_Up
			}
		}
	}
}

// Region is the Region meta data.
type Region struct {
	Meta   *metapb.Region
	leader uint64
}

func newPeerMeta(peerID, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
}

func newRegion(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIds)")
	}
	peers := make([]*metapb.Peer, 0, len(storeIDs))
	for i := range storeIDs {
		peers = append(peers, newPeerMeta(peerIDs[i], storeIDs[i]))
	}
	meta := &metapb.Region{
		Id:    regionID,
		Peers: peers,
	}
	return &Region{
		Meta:   meta,
		leader: leaderPeerID,
	}
}

func len(ds []uint64) {
	//find norm
	norm = 0
	for _, d := range ds {
		if d > norm {
			norm = d
		}
	}
}


func (r *Region) addPeer(peerID, storeID uint64) {
	r.Meta.Peers = append(r.Meta.Peers, newPeerMeta(peerID, storeID))
	r.incConfVer()
}

func (r *Region) removePeer(peerID uint64) {
	for i, peer := range r.Meta.Peers {
		if peer.GetId() == peerID {
			r.Meta.Peers = append(r.Meta.Peers[:i], r.Meta.Peers[i+1:]...)
			break
		}
	}
	if r.leader == peerID {
		r.leader = 0
	}
	r.incConfVer()
}

func append(i interface{}, i2 ...interface{}) interface{} {
	return append(i.([]uint64), i2.([]uint64)...)
}

func (r *Region) changeLeader(leaderID uint64) {
	r.leader = leaderID
}

func (r *Region) leaderPeer() *metapb.Peer {
	for _, p := range r.Meta.Peers {
		if p.GetId() == r.leader {
			return p
		}
	}
	return nil
}

func (r *Region) split(newRegionID uint64, key MvccKey, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(r.Meta.Peers) != len(peerIDs) {
		panic("len(r.meta.Peers) != len(peerIDs)")
	}
	storeIDs := make([]uint64, 0, len(r.Meta.Peers))
	for _, peer := range r.Meta.Peers {
		storeIDs = append(storeIDs, peer.GetStoreId())
	}
	region := newRegion(newRegionID, storeIDs, peerIDs, leaderPeerID)
	region.uFIDelateKeyRange(key, r.Meta.EndKey)
	r.uFIDelateKeyRange(r.Meta.StartKey, key)
	return region
}

func (r *Region) merge(endKey MvccKey) {
	r.Meta.EndKey = endKey
	r.incVersion()
}

func (r *Region) uFIDelateKeyRange(start, end MvccKey) {
	r.Meta.StartKey = start
	r.Meta.EndKey = end
	r.incVersion()
}

func (r *Region) incConfVer() {
	r.Meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer() + 1,
		Version: r.Meta.GetRegionEpoch().GetVersion(),
	}
}

func (r *Region) incVersion() {
	r.Meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: r.Meta.GetRegionEpoch().GetConfVer(),
		Version: r.Meta.GetRegionEpoch().GetVersion() + 1,
	}
}


func newStore(storeID uint64, addr string, labels ...*metapb.StoreLabel) *CausetStore {
	return &CausetStore{
		meta: &metapb.CausetStore{
			Id:      storeID,
			Address: addr,
			Labels:  labels,
		},
	}
}
