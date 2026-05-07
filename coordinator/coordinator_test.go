package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

type fakeShardClient struct {
	upload1Calls int
	upload2Calls int
	upload3Calls int

	lastUpload1 db.UploadArgs1
	lastUpload2 db.UploadArgs2
	lastUpload3 db.UploadArgs3

	startCalls  int
	startReply  db.StartEpochReply
	startErr    error
	abortCalls  int
	abortErr    error
	statusCalls int
	statusReply db.StatusReply
	statusErr   error
	statusDelay time.Duration
	closeCalls  int
}

func (f *fakeShardClient) Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error {
	f.upload1Calls++
	f.lastUpload1 = *args
	reply.Uuid = int64(100 + f.upload1Calls)
	reply.HashKey[0] = byte(10 + f.upload1Calls)
	return nil
}

func (f *fakeShardClient) Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error {
	f.upload2Calls++
	f.lastUpload2 = *args
	return nil
}

func (f *fakeShardClient) Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error {
	f.upload3Calls++
	f.lastUpload3 = *args
	return nil
}

func (f *fakeShardClient) StartEpoch(args *db.StartEpochArgs, reply *db.StartEpochReply) error {
	f.startCalls++
	if f.startErr != nil {
		return f.startErr
	}
	*reply = f.startReply
	return nil
}

func (f *fakeShardClient) EpochStatus(args *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	return nil
}

func (f *fakeShardClient) Status(args *db.StatusArgs, reply *db.StatusReply) error {
	f.statusCalls++
	if f.statusDelay > 0 {
		time.Sleep(f.statusDelay)
	}
	if f.statusErr != nil {
		return f.statusErr
	}
	*reply = f.statusReply
	return nil
}

func (f *fakeShardClient) AbortEpoch(args *db.AbortEpochArgs, reply *db.AbortEpochReply) error {
	f.abortCalls++
	return f.abortErr
}

func (f *fakeShardClient) Close() error {
	f.closeCalls++
	return nil
}

func mustCoordinator(t *testing.T, shards []ShardConfig, clients map[int]shardClient) *Coordinator {
	t.Helper()
	coord, err := NewCoordinator(shards, clients)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func mustCoordinatorWithLease(t *testing.T, shards []ShardConfig, clients map[int]shardClient, store ControlStore, holder string, ttl time.Duration, renewInterval time.Duration) *Coordinator {
	t.Helper()
	coord, err := newCoordinatorWithLeaseConfig(shards, clients, store, holder, ttl, renewInterval)
	if err != nil {
		t.Fatalf("newCoordinatorWithLeaseConfig failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func setCoordinatorActiveEpoch(t *testing.T, coord *Coordinator, epoch db.EpochMeta) {
	t.Helper()
	coord.epoch = epoch
	if err := coord.controlStore.StartEpoch(epoch, coord.controlStore.ShardConfigVersion()); err != nil {
		t.Fatalf("control store StartEpoch failed: %v", err)
	}
}

func activeOnlyShard(id, start, end int) ShardConfig {
	return ShardConfig{
		ID:       id,
		StartRow: start,
		EndRow:   end,
		Active: PairConfig{
			LeaderAddr:   "127.0.0.1:8000",
			FollowerAddr: "127.0.0.1:8001",
		},
	}
}

func activeStandbyShard(id, start, end int) ShardConfig {
	shard := activeOnlyShard(id, start, end)
	shard.Standby = &PairConfig{
		LeaderAddr:   "127.0.0.1:9000",
		FollowerAddr: "127.0.0.1:9001",
	}
	return shard
}

func TestParseShardConfigRequiresActivePairAndAllowsMissingStandby(t *testing.T) {
	shard, err := parseShardConfig("0,0,128,127.0.0.1:8090,127.0.0.1:8091")
	if err != nil {
		t.Fatalf("parseShardConfig failed: %v", err)
	}
	if shard.Active.LeaderAddr != "127.0.0.1:8090" || shard.Active.FollowerAddr != "127.0.0.1:8091" {
		t.Fatalf("unexpected active pair: %+v", shard.Active)
	}
	if shard.Standby != nil {
		t.Fatalf("expected nil standby, got %+v", shard.Standby)
	}

	shard, err = parseShardConfig("1,128,256,127.0.0.1:8190,127.0.0.1:8191,127.0.0.1:8290|127.0.0.1:8291")
	if err != nil {
		t.Fatalf("parseShardConfig with standby failed: %v", err)
	}
	if shard.Standby == nil || shard.Standby.LeaderAddr != "127.0.0.1:8290" || shard.Standby.FollowerAddr != "127.0.0.1:8291" {
		t.Fatalf("unexpected standby pair: %+v", shard.Standby)
	}

	_, err = parseShardConfig("2,0,128,127.0.0.1:8090,")
	if err == nil {
		t.Fatal("expected missing active follower to fail")
	}
}

func TestValidateShardMapRejectsOutOfRangeGapAndOverlap(t *testing.T) {
	_, err := validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, 128),
		activeOnlyShard(1, 128, 300),
	})
	if err == nil {
		t.Fatal("expected out-of-range shard map to fail")
	}

	_, err = validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, 100),
		activeOnlyShard(1, 120, db.TABLE_HEIGHT),
	})
	if err == nil {
		t.Fatal("expected gap shard map to fail")
	}

	_, err = validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, 140),
		activeOnlyShard(1, 120, db.TABLE_HEIGHT),
	})
	if err == nil {
		t.Fatal("expected overlap shard map to fail")
	}
}

func TestValidateShardMapAcceptsContiguousCoverage(t *testing.T) {
	_, err := validateShardMap([]ShardConfig{
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
	})
	if err != nil {
		t.Fatalf("expected contiguous shard map to pass, got %v", err)
	}
}

func TestCoordinatorRoutesBoundaryRowsAndPreservesSessionMapping(t *testing.T) {
	left := &fakeShardClient{}
	right := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: left,
		1: right,
	})
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	var up1Left db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT/2 - 1}, &up1Left); err != nil {
		t.Fatalf("Upload1 left failed: %v", err)
	}
	var up1Right db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT / 2}, &up1Right); err != nil {
		t.Fatalf("Upload1 right failed: %v", err)
	}
	if left.upload1Calls != 1 || right.upload1Calls != 1 {
		t.Fatalf("expected one routed upload1 per shard, got left=%d right=%d", left.upload1Calls, right.upload1Calls)
	}

	up2 := db.UploadArgs2{Uuid: up1Right.Uuid, HashKey: up1Right.HashKey}
	if err := coord.Upload2(&up2, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed: %v", err)
	}
	if right.lastUpload2.Uuid != 101 {
		t.Fatalf("expected local uuid 101, got %d", right.lastUpload2.Uuid)
	}

	up3 := db.UploadArgs3{Uuid: up1Right.Uuid, HashKey: up1Right.HashKey}
	if err := coord.Upload3(&up3, &db.UploadReply3{}); err != nil {
		t.Fatalf("Upload3 failed: %v", err)
	}
	if right.lastUpload3.Uuid != 101 {
		t.Fatalf("expected local uuid 101, got %d", right.lastUpload3.Uuid)
	}
	if _, ok := coord.sessions[up1Right.Uuid]; ok {
		t.Fatalf("expected coordinator session %d to be deleted after upload3", up1Right.Uuid)
	}
}

func TestCoordinatorRejectsWritesWithoutActiveEpoch(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}})

	err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
}

func TestCoordinatorAcquiresStandaloneLease(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, defaultCoordinatorLeaseHolder, defaultCoordinatorLeaseTTL, time.Hour)

	lease, ok := store.CurrentLease(time.Now().UTC())
	if !ok {
		t.Fatal("expected active coordinator lease")
	}
	if lease.Holder != defaultCoordinatorLeaseHolder {
		t.Fatalf("expected holder %q, got %+v", defaultCoordinatorLeaseHolder, lease)
	}
	if lease.FencingToken != coord.lease.FencingToken {
		t.Fatalf("expected coordinator fencing token %d, got lease %+v", coord.lease.FencingToken, lease)
	}
}

func TestCoordinatorRejectsSecondHolderWithActiveLease(t *testing.T) {
	store := newMemoryControlStore(1)
	mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", time.Minute, time.Hour)

	_, err := newCoordinatorWithLeaseConfig([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-b", time.Minute, time.Hour)
	if !errors.Is(err, errLeaseHeld) {
		t.Fatalf("expected held lease error, got %v", err)
	}
}

func TestCoordinatorDifferentHolderCanAcquireAfterLeaseExpiry(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", time.Minute, time.Hour)
	initialToken := coord.lease.FencingToken
	coord.Close()

	next, err := store.AcquireLease(time.Now().UTC().Add(time.Hour), "coord-b", time.Minute)
	if err != nil {
		t.Fatalf("expected second holder to acquire expired lease, got %v", err)
	}
	if next.Holder != "coord-b" || next.FencingToken <= initialToken {
		t.Fatalf("expected coord-b with newer fencing token, got %+v", next)
	}
}

func TestCoordinatorRenewLeaseExtendsExpiry(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", time.Minute, time.Hour)
	initialExpiry := coord.lease.ExpiresAt
	time.Sleep(time.Millisecond)

	if err := coord.renewCoordinatorLease(); err != nil {
		t.Fatalf("renewCoordinatorLease failed: %v", err)
	}
	if !coord.lease.ExpiresAt.After(initialExpiry) {
		t.Fatalf("expected renewed expiry after %s, got %s", initialExpiry, coord.lease.ExpiresAt)
	}
}

func TestCoordinatorCloseStopsRenewal(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", time.Minute, time.Millisecond)

	coord.Close()
	coord.Close()
}

func TestCoordinatorStartEpochRequiresAllShardsAndAbortsStartedShardOnFailure(t *testing.T) {
	startTime := time.Unix(1000, 0).UTC()
	left := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	right := &fakeShardClient{startErr: errors.New("boom")}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: left, 1: right})

	err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{})
	if err == nil {
		t.Fatal("expected start failure")
	}
	if left.abortCalls != 1 {
		t.Fatalf("expected started shard to be aborted once, got %d", left.abortCalls)
	}
	if coord.epoch.State != db.EpochStateNoActive {
		t.Fatalf("expected coordinator epoch to remain closed, got %+v", coord.epoch)
	}
}

func TestCoordinatorStartEpochUsesOnlyActivePairWhenStandbyConfigured(t *testing.T) {
	startTime := time.Unix(1000, 0).UTC()
	active := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})

	var reply db.StartEpochReply
	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &reply); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	if active.startCalls != 1 {
		t.Fatalf("expected active shard start call, got %d", active.startCalls)
	}
}

func stealCoordinatorLease(t *testing.T, store *memoryControlStore, holder string) CoordinatorLease {
	t.Helper()
	lease, err := store.AcquireLease(time.Now().UTC().Add(time.Hour), holder, time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease(%q) failed: %v", holder, err)
	}
	return lease
}

func TestCoordinatorStartEpochFailsWithStaleLease(t *testing.T) {
	store := newMemoryControlStore(1)
	startTime := time.Now().UTC().Truncate(time.Second)
	client := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: client}, store, "coord-a", time.Minute, time.Hour)
	stealCoordinatorLease(t, store, "coord-b")

	err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{})
	if err == nil || !strings.Contains(err.Error(), "coordinator lease unavailable") {
		t.Fatalf("expected coordinator lease error, got %v", err)
	}
	if client.startCalls != 0 {
		t.Fatalf("expected stale coordinator not to contact shard, got %d StartEpoch calls", client.startCalls)
	}
}

func TestCoordinatorStartEpochProducesSharedMetadata(t *testing.T) {
	startTime := time.Now().UTC().Truncate(time.Second)
	reply := db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(2 * time.Minute).Unix(),
		DurationSecs: 120,
	}
	left := &fakeShardClient{startReply: reply}
	right := &fakeShardClient{startReply: reply}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: left, 1: right})

	var startReply db.StartEpochReply
	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 120, StartUnix: startTime.Unix()}, &startReply); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	if startReply != reply {
		t.Fatalf("unexpected coordinator start reply: %+v", startReply)
	}
	var status db.EpochStatusReply
	if err := coord.EpochStatus(&db.EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("EpochStatus failed: %v", err)
	}
	if status.EpochID != reply.EpochID || status.State != db.EpochStateActive.String() || !status.Accepting {
		t.Fatalf("unexpected coordinator status: %+v", status)
	}
	epoch, ok := coord.controlStore.CurrentEpoch()
	if !ok || epoch.ID != reply.EpochID || epoch.State != db.EpochStateActive {
		t.Fatalf("expected active control-store epoch, ok=%t epoch=%+v", ok, epoch)
	}
	accepting, err := coord.controlStore.Accepting(reply.EpochID)
	if err != nil || !accepting {
		t.Fatalf("expected control store accepting epoch, accepting=%t err=%v", accepting, err)
	}
}

func TestCoordinatorCompleteEpochMirrorsControlStore(t *testing.T) {
	startTime := time.Now().UTC().Truncate(time.Second)
	reply := db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{startReply: reply}})

	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	coord.completeEpoch()

	epoch, ok := coord.controlStore.CurrentEpoch()
	if !ok || epoch.ID != reply.EpochID || epoch.State != db.EpochStateCompleted {
		t.Fatalf("expected completed control-store epoch, ok=%t epoch=%+v", ok, epoch)
	}
	accepting, err := coord.controlStore.Accepting(reply.EpochID)
	if err != nil || accepting {
		t.Fatalf("expected completed control-store epoch not accepting, accepting=%t err=%v", accepting, err)
	}
}

func TestCoordinatorCompleteEpochSkipsStoreMutationWithStaleLease(t *testing.T) {
	store := newMemoryControlStore(1)
	startTime := time.Now().UTC().Truncate(time.Second)
	reply := db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{startReply: reply}}, store, "coord-a", time.Minute, time.Hour)
	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	stealCoordinatorLease(t, store, "coord-b")

	coord.completeEpoch()

	epoch, ok := store.CurrentEpoch()
	if !ok || epoch.State != db.EpochStateActive {
		t.Fatalf("expected stale coordinator not to complete store epoch, ok=%t epoch=%+v", ok, epoch)
	}
	if coord.epoch.State != db.EpochStateActive {
		t.Fatalf("expected stale coordinator local epoch to remain active, got %+v", coord.epoch)
	}
}

func TestCoordinatorUploadsUseOnlyActivePairWhenStandbyConfigured(t *testing.T) {
	active := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)

	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 1}, &up1); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed: %v", err)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("Upload3 failed: %v", err)
	}
	if active.upload1Calls != 1 || active.upload2Calls != 1 || active.upload3Calls != 1 {
		t.Fatalf("expected active-only uploads, got upload1=%d upload2=%d upload3=%d", active.upload1Calls, active.upload2Calls, active.upload3Calls)
	}
}

func TestCoordinatorUpload1RejectsWhenControlStoreNotAccepting(t *testing.T) {
	fakeClient := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient})
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	if err := coord.controlStore.SetAccepting(epoch.ID, false); err != nil {
		t.Fatalf("SetAccepting failed: %v", err)
	}

	err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
	if fakeClient.upload1Calls != 0 {
		t.Fatalf("expected Upload1 not to reach shard when control store is not accepting, got %d calls", fakeClient.upload1Calls)
	}
}

func TestCoordinatorUpload1RejectsWithStaleLease(t *testing.T) {
	store := newMemoryControlStore(1)
	fakeClient := &fakeShardClient{}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient}, store, "coord-a", time.Minute, time.Hour)
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	stealCoordinatorLease(t, store, "coord-b")

	err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
	if fakeClient.upload1Calls != 0 {
		t.Fatalf("expected Upload1 not to reach shard with stale lease, got %d calls", fakeClient.upload1Calls)
	}
}

func TestCoordinatorStatusIncludesConfiguredShardsAndShardStatus(t *testing.T) {
	startTime := time.Unix(1200, 0).UTC()
	leftStatus := db.StatusReply{
		Healthy:      true,
		IsLeader:     true,
		ServerIndex:  0,
		ShardID:      0,
		EpochID:      3,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
		Accepting:    true,
		PeerState:    db.PeerConnectionsReady.String(),
	}
	rightStatus := leftStatus
	rightStatus.ShardID = 1

	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: leftStatus},
		1: &fakeShardClient{statusReply: rightStatus},
	})
	coord.epoch = db.EpochMeta{
		ID:              3,
		State:           db.EpochStateActive,
		StartTime:       startTime,
		EndTime:         startTime.Add(time.Minute),
		DurationSeconds: 60,
	}

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	if reply.Role != "standalone" || reply.EpochID != 3 || reply.State != db.EpochStateActive.String() || !reply.Accepting {
		t.Fatalf("unexpected coordinator status: %+v", reply)
	}
	if len(reply.Shards) != 2 {
		t.Fatalf("expected two shard statuses, got %d", len(reply.Shards))
	}
	if !reply.Shards[0].Reachable || reply.Shards[0].Status.ShardID != 0 {
		t.Fatalf("unexpected shard 0 status: %+v", reply.Shards[0])
	}
	if !reply.Shards[0].ActiveReachable || reply.Shards[0].ActiveStatus.ShardID != 0 || reply.Shards[0].ActiveLastChecked == 0 {
		t.Fatalf("unexpected shard 0 active health: %+v", reply.Shards[0])
	}
	if !reply.Shards[1].Reachable || reply.Shards[1].Status.ShardID != 1 {
		t.Fatalf("unexpected shard 1 status: %+v", reply.Shards[1])
	}
}

func TestCoordinatorStatusIncludesStandbyHealthWhenConfigured(t *testing.T) {
	active := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, IsLeader: true, ShardID: 0}}
	standby := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, IsLeader: true, ShardID: 0, PeerState: db.PeerConnectionsReady.String()}}
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.standbyLeaderDialer = func(addr string, timeout time.Duration) (closeableStatusClient, error) {
		if addr != "127.0.0.1:9000" {
			t.Fatalf("unexpected standby leader address: %s", addr)
		}
		return standby, nil
	}

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	shard := reply.Shards[0]
	if !shard.HasStandby || shard.StandbyLeaderAddr != "127.0.0.1:9000" || shard.StandbyFollowerAddr != "127.0.0.1:9001" {
		t.Fatalf("unexpected standby assignment: %+v", shard)
	}
	if !shard.ActiveReachable || !shard.StandbyReachable || shard.StandbyLastChecked == 0 {
		t.Fatalf("expected active and standby reachable health, got %+v", shard)
	}
	if standby.statusCalls != 1 || standby.closeCalls != 1 {
		t.Fatalf("expected standby status and close calls, got status=%d close=%d", standby.statusCalls, standby.closeCalls)
	}
}

func TestCoordinatorStatusRecordsUnreachableStandbyHealth(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
	})
	coord.standbyLeaderDialer = func(addr string, timeout time.Duration) (closeableStatusClient, error) {
		return nil, errors.New("standby dial failed")
	}

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	shard := reply.Shards[0]
	if !shard.ActiveReachable {
		t.Fatalf("expected active reachable health, got %+v", shard)
	}
	if shard.StandbyReachable || shard.StandbyStatusError != "standby dial failed" || shard.StandbyLastChecked == 0 {
		t.Fatalf("expected standby error health, got %+v", shard)
	}
}

func TestCoordinatorCloseStopsHealthLoop(t *testing.T) {
	coord, err := NewCoordinator([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
	})
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		coord.Close()
		coord.Close()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Coordinator.Close did not stop background loops promptly")
	}
}

func TestCoordinatorStatusRecordsUnreachableShardErrors(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
		1: &fakeShardClient{statusErr: errors.New("dial failed")},
	})

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	if !reply.Shards[0].Reachable {
		t.Fatalf("expected shard 0 reachable: %+v", reply.Shards[0])
	}
	if reply.Shards[1].Reachable || reply.Shards[1].StatusError != "dial failed" {
		t.Fatalf("expected shard 1 status error, got %+v", reply.Shards[1])
	}
}

func TestCoordinatorStatusTimeoutDoesNotFailWholeCall(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT/2),
		activeOnlyShard(1, db.TABLE_HEIGHT/2, db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
		1: &fakeShardClient{statusDelay: 50 * time.Millisecond},
	})

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{ShardTimeoutMs: 5}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	if !reply.Shards[0].Reachable {
		t.Fatalf("expected shard 0 reachable: %+v", reply.Shards[0])
	}
	if reply.Shards[1].Reachable || reply.Shards[1].StatusError == "" {
		t.Fatalf("expected shard 1 timeout error, got %+v", reply.Shards[1])
	}
}
