package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/controlstore"
	"bitbucket.org/henrycg/riposte/db"
)

const (
	testLeaseTTL           = time.Minute
	testLeaseRenewInterval = 30 * time.Second
)

type fakeShardClient struct {
	upload1Calls int
	upload2Calls int
	upload3Calls int
	upload1Err   error
	upload2Err   error
	upload3Err   error

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

type collidingSessionStore struct {
	inner       *memorySessionStore
	collisions  int
	putAttempts int
}

type failingEpochShardConfigStore struct {
	ControlStore
	err error
}

func (s *failingEpochShardConfigStore) PutEpochShardConfig(epochID int64, config ShardConfigRecord) error {
	return s.err
}

func (s *collidingSessionStore) PutSession(ctx context.Context, session SessionRecord) error {
	s.putAttempts++
	if s.collisions > 0 {
		s.collisions--
		return errSessionExists
	}
	return s.inner.PutSession(ctx, session)
}

func (s *collidingSessionStore) GetSession(ctx context.Context, globalUUID int64) (SessionRecord, error) {
	return s.inner.GetSession(ctx, globalUUID)
}

func (s *collidingSessionStore) DeleteSession(ctx context.Context, globalUUID int64) error {
	return s.inner.DeleteSession(ctx, globalUUID)
}

func (f *fakeShardClient) Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error {
	f.upload1Calls++
	if f.upload1Err != nil {
		return f.upload1Err
	}
	f.lastUpload1 = *args
	if args.UseAssignedSession {
		reply.Uuid = args.AssignedUUID
		reply.HashKey = args.AssignedHashKey
	} else {
		reply.Uuid = int64(100 + f.upload1Calls)
		reply.HashKey[0] = byte(10 + f.upload1Calls)
	}
	return nil
}

func (f *fakeShardClient) Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error {
	f.upload2Calls++
	if f.upload2Err != nil {
		return f.upload2Err
	}
	f.lastUpload2 = *args
	return nil
}

func (f *fakeShardClient) Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error {
	f.upload3Calls++
	if f.upload3Err != nil {
		return f.upload3Err
	}
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
	coord, err := newCoordinatorWithLeaseConfig(shards, clients, store, newMemorySessionStore(), "memory", holder, ttl, renewInterval)
	if err != nil {
		t.Fatalf("newCoordinatorWithLeaseConfig failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func mustCoordinatorWithLeaseAndSessionStore(t *testing.T, shards []ShardConfig, clients map[int]shardClient, store ControlStore, sessionStore SessionStore, holder string, ttl time.Duration, renewInterval time.Duration) *Coordinator {
	t.Helper()
	coord, err := newCoordinatorWithLeaseConfig(shards, clients, store, sessionStore, "memory", holder, ttl, renewInterval)
	if err != nil {
		t.Fatalf("newCoordinatorWithLeaseConfig failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func mustStandbyCoordinator(t *testing.T, shards []ShardConfig, clients map[int]shardClient, store ControlStore, holder string, ttl time.Duration, renewInterval time.Duration) *Coordinator {
	t.Helper()
	coord, err := newCoordinatorWithStandbyConfig(shards, clients, store, newMemorySessionStore(), "memory", holder, ttl, renewInterval, true)
	if err != nil {
		t.Fatalf("newCoordinatorWithStandbyConfig failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func mustStandbyCoordinatorWithSessionStore(t *testing.T, shards []ShardConfig, clients map[int]shardClient, store ControlStore, sessionStore SessionStore, holder string, ttl time.Duration, renewInterval time.Duration) *Coordinator {
	t.Helper()
	coord, err := newCoordinatorWithStandbyConfig(shards, clients, store, sessionStore, "memory", holder, ttl, renewInterval, true)
	if err != nil {
		t.Fatalf("newCoordinatorWithStandbyConfig failed: %v", err)
	}
	t.Cleanup(coord.Close)
	return coord
}

func setCoordinatorActiveEpoch(t *testing.T, coord *Coordinator, epoch db.EpochMeta) {
	t.Helper()
	coord.epoch = epoch
	if err := coord.controlStore.StartEpoch(epoch, currentShardConfigVersion(coord.controlStore)); err != nil {
		t.Fatalf("control store StartEpoch failed: %v", err)
	}
}

func setEpochCycleRecommendationReady(t *testing.T, store ControlStore, epochID int64, shardConfigVersion int64) {
	t.Helper()
	if cycle, ok, err := store.GetEpochCycle(); err != nil {
		t.Fatalf("GetEpochCycle failed: %v", err)
	} else if ok && cycle.State == controlstore.EpochCycleStateRecommendationReady {
		return
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateIdle, controlstore.EpochCycleStateActive, EpochCycleRecord{
		EpochID:            epochID,
		ShardConfigVersion: shardConfigVersion,
	}); err != nil {
		t.Fatalf("cycle idle -> active failed: %v", err)
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateActive, controlstore.EpochCycleStateRecommendationReady, EpochCycleRecord{
		EpochID:                      epochID,
		ShardConfigVersion:           shardConfigVersion,
		ScalingRecommendationEpochID: epochID,
	}); err != nil {
		t.Fatalf("cycle active -> recommendation_ready failed: %v", err)
	}
}

func setEpochCycleReadyForNext(t *testing.T, store ControlStore, epochID int64, shardConfigVersion int64) {
	t.Helper()
	setEpochCycleRecommendationReady(t, store, epochID, shardConfigVersion)
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleStateScalingInProgress, EpochCycleRecord{
		EpochID:                      epochID,
		ShardConfigVersion:           shardConfigVersion,
		ScalingRecommendationEpochID: epochID,
	}); err != nil {
		t.Fatalf("cycle recommendation_ready -> scaling_in_progress failed: %v", err)
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateScalingInProgress, controlstore.EpochCycleStateScalingApplied, EpochCycleRecord{
		EpochID:                      epochID,
		ShardConfigVersion:           shardConfigVersion,
		ScalingRecommendationEpochID: epochID,
	}); err != nil {
		t.Fatalf("cycle scaling_in_progress -> scaling_applied failed: %v", err)
	}
}

func assertEpochCycleState(t *testing.T, store ControlStore, want string) {
	t.Helper()
	cycle, ok, err := store.GetEpochCycle()
	if err != nil || !ok || cycle.State != want {
		t.Fatalf("expected epoch cycle %s, ok=%t err=%v cycle=%+v", want, ok, err, cycle)
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

func startCoordinatorEpoch(t *testing.T, coord *Coordinator, client *fakeShardClient, epochID int64, startTime time.Time, durationSeconds int64) {
	t.Helper()
	client.startReply = db.StartEpochReply{
		EpochID:      epochID,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Duration(durationSeconds) * time.Second).Unix(),
		DurationSecs: durationSeconds,
	}
	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: durationSeconds, StartUnix: startTime.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
}

func completeCoordinatorUpload(t *testing.T, coord *Coordinator, routeRow int) {
	t.Helper()
	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: routeRow}, &up1); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed: %v", err)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("Upload3 failed: %v", err)
	}
}

func testScalingRecommendation(epochID int64, shardConfigVersion int64, action string, currentShards int, recommendedShards int) ScalingRecommendationRecord {
	return ScalingRecommendationRecord{
		Key:                       epochScalingRecommendationKey(epochID),
		EpochID:                   epochID,
		AcceptedRequestCount:      int64(currentShards * db.TABLE_HEIGHT * 8),
		DurationSeconds:           60,
		CurrentShardCount:         currentShards,
		RecommendedShardCount:     recommendedShards,
		TargetRowsPerShard:        db.TABLE_HEIGHT,
		RequestDensity:            8,
		Action:                    action,
		Reason:                    "test recommendation",
		ProposedGlobalTableHeight: recommendedShards * db.TABLE_HEIGHT,
		ShardConfigVersion:        shardConfigVersion,
		CreatedAt:                 time.Now().UTC(),
	}
}

func TestParseShardConfigRequiresActivePairAndAllowsMissingStandby(t *testing.T) {
	shard, err := parseShardConfig("0,0,256,127.0.0.1:8090,127.0.0.1:8091")
	if err != nil {
		t.Fatalf("parseShardConfig failed: %v", err)
	}
	if shard.Active.LeaderAddr != "127.0.0.1:8090" || shard.Active.FollowerAddr != "127.0.0.1:8091" {
		t.Fatalf("unexpected active pair: %+v", shard.Active)
	}
	if shard.Standby != nil {
		t.Fatalf("expected nil standby, got %+v", shard.Standby)
	}

	shard, err = parseShardConfig("1,256,512,127.0.0.1:8190,127.0.0.1:8191,127.0.0.1:8290|127.0.0.1:8291")
	if err != nil {
		t.Fatalf("parseShardConfig with standby failed: %v", err)
	}
	if shard.Standby == nil || shard.Standby.LeaderAddr != "127.0.0.1:8290" || shard.Standby.FollowerAddr != "127.0.0.1:8291" {
		t.Fatalf("unexpected standby pair: %+v", shard.Standby)
	}

	_, err = parseShardConfig("2,0,256,127.0.0.1:8090,")
	if err == nil {
		t.Fatal("expected missing active follower to fail")
	}
}

func TestValidateShardMapRejectsBadHeightGapAndOverlap(t *testing.T) {
	_, err := validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, 128),
		activeOnlyShard(1, 128, 300),
	})
	if err == nil {
		t.Fatal("expected wrong-height shard map to fail")
	}

	_, err = validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT+10, 2*db.TABLE_HEIGHT+10),
	})
	if err == nil {
		t.Fatal("expected gap shard map to fail")
	}

	_, err = validateShardMap([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT-10, 2*db.TABLE_HEIGHT-10),
	})
	if err == nil {
		t.Fatal("expected overlap shard map to fail")
	}
}

func TestValidateShardMapAcceptsContiguousCoverage(t *testing.T) {
	_, err := validateShardMap([]ShardConfig{
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	})
	if err != nil {
		t.Fatalf("expected contiguous shard map to pass, got %v", err)
	}
}

func TestCoordinatorRoutesBoundaryRowsAndPreservesSessionMapping(t *testing.T) {
	left := &fakeShardClient{}
	right := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: left,
		1: right,
	})
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	var up1Left db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT - 1}, &up1Left); err != nil {
		t.Fatalf("Upload1 left failed: %v", err)
	}
	var up1Right db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT}, &up1Right); err != nil {
		t.Fatalf("Upload1 right failed: %v", err)
	}
	if left.upload1Calls != 1 || right.upload1Calls != 1 {
		t.Fatalf("expected one routed upload1 per shard, got left=%d right=%d", left.upload1Calls, right.upload1Calls)
	}
	if left.lastUpload1.RouteRow != db.TABLE_HEIGHT-1 {
		t.Fatalf("expected shard 0 local route row %d, got %d", db.TABLE_HEIGHT-1, left.lastUpload1.RouteRow)
	}
	if right.lastUpload1.RouteRow != 0 {
		t.Fatalf("expected shard 1 local route row 0, got %d", right.lastUpload1.RouteRow)
	}
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 2 * db.TABLE_HEIGHT}, &db.UploadReply1{}); err == nil {
		t.Fatal("expected global route row outside the configured table to fail")
	}

	up2 := db.UploadArgs2{Uuid: up1Right.Uuid, HashKey: up1Right.HashKey}
	if err := coord.Upload2(&up2, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed: %v", err)
	}
	if !right.lastUpload1.UseAssignedSession || right.lastUpload1.AssignedUUID != up1Right.Uuid || right.lastUpload1.AssignedHashKey != up1Right.HashKey {
		t.Fatalf("expected assigned shard session to match coordinator reply, args=%+v reply=%+v", right.lastUpload1, up1Right)
	}
	if right.lastUpload2.Uuid != up1Right.Uuid {
		t.Fatalf("expected local uuid %d, got %d", up1Right.Uuid, right.lastUpload2.Uuid)
	}

	up3 := db.UploadArgs3{Uuid: up1Right.Uuid, HashKey: up1Right.HashKey}
	if err := coord.Upload3(&up3, &db.UploadReply3{}); err != nil {
		t.Fatalf("Upload3 failed: %v", err)
	}
	if right.lastUpload3.Uuid != up1Right.Uuid {
		t.Fatalf("expected local uuid %d, got %d", up1Right.Uuid, right.lastUpload3.Uuid)
	}
	if _, ok := coord.sessions[up1Right.Uuid]; ok {
		t.Fatalf("expected coordinator session %d to be deleted after upload3", up1Right.Uuid)
	}
}

func TestCoordinatorUpload1PersistsAssignedSessionBeforeShardAdmission(t *testing.T) {
	store := newMemorySessionStore()
	shard := &fakeShardClient{}
	coord, err := newCoordinatorWithStores([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: shard}, newMemoryControlStore(1), store, "memory")
	if err != nil {
		t.Fatalf("newCoordinatorWithStores failed: %v", err)
	}
	t.Cleanup(coord.Close)
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	var reply db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 7}, &reply); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	if !shard.lastUpload1.UseAssignedSession || shard.lastUpload1.AssignedUUID != reply.Uuid || shard.lastUpload1.AssignedHashKey != reply.HashKey {
		t.Fatalf("expected coordinator-assigned shard session, args=%+v reply=%+v", shard.lastUpload1, reply)
	}
	session, err := store.GetSession(context.Background(), reply.Uuid)
	if err != nil {
		t.Fatalf("expected persisted session: %v", err)
	}
	if session.GlobalUUID != reply.Uuid || session.LocalUUID != reply.Uuid || session.HashKey != reply.HashKey || session.LocalRow != 7 {
		t.Fatalf("unexpected persisted session: %+v reply=%+v", session, reply)
	}
}

func TestCoordinatorUpload1RetriesSessionUUIDCollision(t *testing.T) {
	store := &collidingSessionStore{inner: newMemorySessionStore(), collisions: 1}
	shard := &fakeShardClient{}
	coord, err := newCoordinatorWithStores([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: shard}, newMemoryControlStore(1), store, "memory")
	if err != nil {
		t.Fatalf("newCoordinatorWithStores failed: %v", err)
	}
	t.Cleanup(coord.Close)
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	var reply db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 7}, &reply); err != nil {
		t.Fatalf("Upload1 failed after collision retry: %v", err)
	}
	if store.putAttempts < 2 {
		t.Fatalf("expected collision retry, got %d put attempts", store.putAttempts)
	}
	if _, err := store.GetSession(context.Background(), reply.Uuid); err != nil {
		t.Fatalf("expected persisted session after retry: %v", err)
	}
}

func TestCoordinatorUpload1DeletesPersistedSessionOnShardFailure(t *testing.T) {
	store := newMemorySessionStore()
	shard := &fakeShardClient{upload1Err: errors.New("shard down")}
	coord, err := newCoordinatorWithStores([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: shard}, newMemoryControlStore(1), store, "memory")
	if err != nil {
		t.Fatalf("newCoordinatorWithStores failed: %v", err)
	}
	t.Cleanup(coord.Close)
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	err = coord.Upload1(&db.UploadArgs1{RouteRow: 7}, &db.UploadReply1{})
	if err == nil || err.Error() != "shard down" {
		t.Fatalf("expected shard error, got %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("expected persisted session cleanup after shard failure, got %d session(s)", store.Len())
	}
}

func TestCoordinatorUpload2AndUpload3RecoverSessionFromStore(t *testing.T) {
	store := newMemorySessionStore()
	shard := &fakeShardClient{}
	coord, err := newCoordinatorWithStores([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: shard}, newMemoryControlStore(1), store, "memory")
	if err != nil {
		t.Fatalf("newCoordinatorWithStores failed: %v", err)
	}
	t.Cleanup(coord.Close)
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 1}, &up1); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	coord.mu.Lock()
	coord.sessions = make(map[int64]SessionRecord)
	coord.mu.Unlock()
	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed using stored session: %v", err)
	}
	if shard.lastUpload2.Uuid != up1.Uuid {
		t.Fatalf("expected stored local uuid %d, got %d", up1.Uuid, shard.lastUpload2.Uuid)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("Upload3 failed using stored session: %v", err)
	}
	if _, err := store.GetSession(context.Background(), up1.Uuid); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected session deleted after Upload3, got %v", err)
	}
}

func TestDifferentCoordinatorRecoversSessionFromSharedStore(t *testing.T) {
	controlStore := newMemoryControlStore(1)
	sessionStore := newMemorySessionStore()
	activeShard := &fakeShardClient{}
	passiveShard := &fakeShardClient{}
	shards := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
	active := mustCoordinatorWithLeaseAndSessionStore(t, shards, map[int]shardClient{0: activeShard}, controlStore, sessionStore, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	passive := mustStandbyCoordinatorWithSessionStore(t, shards, map[int]shardClient{0: passiveShard}, controlStore, sessionStore, "coord-b", testLeaseTTL, testLeaseRenewInterval)
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, active, epoch)

	var up1 db.UploadReply1
	if err := active.Upload1(&db.UploadArgs1{RouteRow: 3}, &up1); err != nil {
		t.Fatalf("active Upload1 failed: %v", err)
	}
	if err := passive.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("passive Upload2 failed using shared session: %v", err)
	}
	if passiveShard.lastUpload2.Uuid != up1.Uuid {
		t.Fatalf("expected passive Upload2 to forward local uuid %d, got %d", up1.Uuid, passiveShard.lastUpload2.Uuid)
	}
	if err := passive.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("passive Upload3 failed using shared session: %v", err)
	}
	if _, err := sessionStore.GetSession(context.Background(), up1.Uuid); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected shared session deleted after Upload3, got %v", err)
	}
}

func TestCoordinatorRejectsWritesWithoutActiveEpoch(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}})

	err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != coordinatorWireNoActiveEpoch {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
}

func TestCoordinatorUpload2AndUpload3RejectBadSession(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}})
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{
		ID:              1,
		State:           db.EpochStateActive,
		StartTime:       time.Now().UTC(),
		EndTime:         time.Now().UTC().Add(time.Minute),
		DurationSeconds: 60,
	})

	err := coord.Upload2(&db.UploadArgs2{Uuid: 12345}, &db.UploadReply2{})
	if err == nil || err.Error() != coordinatorWireBogusUUID {
		t.Fatalf("expected bogus uuid from Upload2, got %v", err)
	}
	err = coord.Upload3(&db.UploadArgs3{Uuid: 12345}, &db.UploadReply3{})
	if err == nil || err.Error() != coordinatorWireBogusUUID {
		t.Fatalf("expected bogus uuid from Upload3, got %v", err)
	}
}

func TestCoordinatorAcquiresStandaloneLease(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, defaultCoordinatorLeaseHolder, defaultCoordinatorLeaseTTL, defaultCoordinatorLeaseRenewInterval)

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
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	_, err := newCoordinatorWithLeaseConfig([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, newMemorySessionStore(), "memory", "coord-b", testLeaseTTL, testLeaseRenewInterval)
	if !errors.Is(err, errLeaseHeld) {
		t.Fatalf("expected held lease error, got %v", err)
	}
}

func TestStandbyCoordinatorStartsPassiveWithActiveLease(t *testing.T) {
	store := newMemoryControlStore(1)
	mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	standby := mustStandbyCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-b", testLeaseTTL, testLeaseRenewInterval)

	if standby.role != coordinatorRolePassive {
		t.Fatalf("expected passive standby, got %s", standby.role)
	}
	var status db.CoordinatorStatusReply
	if err := standby.Status(&db.CoordinatorStatusArgs{}, &status); err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.Role != coordinatorRolePassive || status.ActiveHolder != "coord-a" {
		t.Fatalf("expected passive status with coord-a active holder, got %+v", status)
	}
}

func TestPassiveCoordinatorRejectsControlMutationsButCanRouteAcceptedUploads(t *testing.T) {
	store := newMemoryControlStore(1)
	shards := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	sessionStore := newMemorySessionStore()
	activeClient := &fakeShardClient{}
	active := mustCoordinatorWithLeaseAndSessionStore(t, shards, map[int]shardClient{0: activeClient}, store, sessionStore, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	fakeClient := &fakeShardClient{}
	standby := mustStandbyCoordinatorWithSessionStore(t, shards, map[int]shardClient{0: fakeClient}, store, sessionStore, "coord-b", testLeaseTTL, testLeaseRenewInterval)
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, active, epoch)

	err := standby.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60}, &db.StartEpochReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active error, got %v", err)
	}
	if fakeClient.startCalls != 0 {
		t.Fatalf("expected passive coordinator not to contact shard, got %d calls", fakeClient.startCalls)
	}

	var up1 db.UploadReply1
	if err := standby.Upload1(&db.UploadArgs1{RouteRow: 0}, &up1); err != nil {
		t.Fatalf("expected passive Upload1 to route while store accepts writes, got %v", err)
	}
	if fakeClient.upload1Calls != 1 {
		t.Fatalf("expected passive Upload1 to reach shard once, got %d calls", fakeClient.upload1Calls)
	}
	if err := standby.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("expected passive Upload2 to route while store accepts writes, got %v", err)
	}
	if err := standby.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("expected passive Upload3 to route while store accepts writes, got %v", err)
	}
}

func TestPassiveCoordinatorBecomesActiveAfterLeaseExpiry(t *testing.T) {
	store := newMemoryControlStore(1)
	active := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", 30*time.Millisecond, 10*time.Millisecond)
	initialToken := active.lease.FencingToken
	active.Close()

	standby := mustStandbyCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-b", testLeaseTTL, time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		standby.mu.Lock()
		role := standby.role
		token := standby.lease.FencingToken
		standby.mu.Unlock()
		if role == coordinatorRoleActive && token > initialToken {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("standby did not become active with newer token; role=%s lease=%+v", standby.role, standby.lease)
}

func TestCoordinatorDifferentHolderCanAcquireAfterLeaseExpiry(t *testing.T) {
	store := newMemoryControlStore(1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
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
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
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
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, time.Millisecond)

	coord.Close()
	coord.Close()
}

func TestValidateCoordinatorLeaseConfigRejectsInvalidTiming(t *testing.T) {
	tests := []struct {
		name          string
		holder        string
		ttl           time.Duration
		renewInterval time.Duration
	}{
		{name: "missing holder", holder: "", ttl: testLeaseTTL, renewInterval: testLeaseRenewInterval},
		{name: "zero ttl", holder: "coord-a", ttl: 0, renewInterval: testLeaseRenewInterval},
		{name: "zero renew", holder: "coord-a", ttl: testLeaseTTL, renewInterval: 0},
		{name: "renew equal ttl", holder: "coord-a", ttl: testLeaseTTL, renewInterval: testLeaseTTL},
		{name: "renew greater than ttl", holder: "coord-a", ttl: testLeaseTTL, renewInterval: testLeaseTTL + time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateCoordinatorLeaseConfig(tt.holder, tt.ttl, tt.renewInterval); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
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
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
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

func TestCoordinatorStartEpochAbortsStartedShardsWhenShardConfigSnapshotFails(t *testing.T) {
	startTime := time.Unix(1000, 0).UTC()
	shard := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	store := &failingEpochShardConfigStore{
		ControlStore: newMemoryControlStore(1),
		err:          errors.New("snapshot failed"),
	}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: shard}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{})
	if err == nil || !strings.Contains(err.Error(), "write epoch shard config") {
		t.Fatalf("expected shard config snapshot failure, got %v", err)
	}
	if shard.abortCalls != 1 {
		t.Fatalf("expected started shard to be aborted once, got %d", shard.abortCalls)
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
	}, map[int]shardClient{0: client}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	stealCoordinatorLease(t, store, "coord-b")

	err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active error, got %v", err)
	}
	if client.startCalls != 0 {
		t.Fatalf("expected stale coordinator not to contact shard, got %d StartEpoch calls", client.startCalls)
	}
	if coord.role != coordinatorRoleStale {
		t.Fatalf("expected stale coordinator role, got %s", coord.role)
	}
}

func TestCoordinatorStartEpochRejectsScalingInProgressCycle(t *testing.T) {
	store := newMemoryControlStore(1)
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateIdle, controlstore.EpochCycleStateActive, EpochCycleRecord{EpochID: 1, ShardConfigVersion: 1}); err != nil {
		t.Fatalf("cycle idle -> active failed: %v", err)
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateActive, controlstore.EpochCycleStateRecommendationReady, EpochCycleRecord{EpochID: 1, ShardConfigVersion: 1}); err != nil {
		t.Fatalf("cycle active -> recommendation_ready failed: %v", err)
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleStateScalingInProgress, EpochCycleRecord{EpochID: 1, ShardConfigVersion: 1}); err != nil {
		t.Fatalf("cycle recommendation_ready -> scaling_in_progress failed: %v", err)
	}
	startTime := time.Now().UTC().Truncate(time.Second)
	client := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      2,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: client}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{})
	if err == nil || !strings.Contains(err.Error(), controlstore.EpochCycleStateScalingInProgress) {
		t.Fatalf("expected scaling-in-progress cycle rejection, got %v", err)
	}
	if client.startCalls != 0 {
		t.Fatalf("expected shard not to be contacted, got %d calls", client.startCalls)
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
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
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
	config, ok, err := coord.controlStore.GetEpochShardConfig(reply.EpochID)
	if err != nil || !ok || config.Key != epochShardConfigKey(reply.EpochID) || config.ShardCount != 2 {
		t.Fatalf("expected epoch shard config snapshot, ok=%t err=%v config=%+v", ok, err, config)
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
	cycle, ok, err := coord.controlStore.GetEpochCycle()
	if err != nil || !ok || cycle.State != controlstore.EpochCycleStateScalingSkipped || cycle.EpochID != reply.EpochID {
		t.Fatalf("expected scaling-skipped cycle, ok=%t err=%v cycle=%+v", ok, err, cycle)
	}
	if !strings.Contains(cycle.Reason, "keep") {
		t.Fatalf("expected keep skip reason, got %+v", cycle)
	}
}

func TestCoordinatorStartEpochSucceedsAfterAutoSkippedKeepRecommendation(t *testing.T) {
	startTime := time.Now().UTC().Truncate(time.Second)
	active := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})

	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	coord.completeEpoch()
	assertEpochCycleState(t, coord.controlStore, controlstore.EpochCycleStateScalingSkipped)

	secondStart := startTime.Add(2 * time.Minute)
	active.startReply = db.StartEpochReply{
		EpochID:      2,
		State:        db.EpochStateActive.String(),
		StartUnix:    secondStart.Unix(),
		EndUnix:      secondStart.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}
	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: secondStart.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("second StartEpoch failed after auto skip: %v", err)
	}
}

func TestCoordinatorCompleteEpochCachesScalingRecommendation(t *testing.T) {
	active := &fakeShardClient{}
	startTime := time.Now().UTC().Truncate(time.Second)
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.scalingConfig = ScalingPolicyConfig{
		MinShards:                 1,
		MaxShards:                 4,
		TargetRowsPerShard:        2,
		ScaleUpDensityThreshold:   4,
		ScaleDownDensityThreshold: 1,
		MaxShardMultiplier:        2,
	}
	startCoordinatorEpoch(t, coord, active, 1, startTime, 60)
	for i := 0; i < 8; i++ {
		completeCoordinatorUpload(t, coord, 1)
	}

	coord.completeEpoch()

	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.hasLastScalingMetrics {
		t.Fatal("expected last scaling metrics")
	}
	if coord.lastScalingMetrics.EpochID != 1 || coord.lastScalingMetrics.AcceptedRequestCount != 8 || coord.lastScalingMetrics.DurationSeconds != 60 {
		t.Fatalf("unexpected last scaling metrics: %+v", coord.lastScalingMetrics)
	}
	if coord.lastScalingRecommendation.Action != scalingActionGrow || coord.lastScalingRecommendation.RecommendedShardCount != 2 {
		t.Fatalf("expected grow recommendation, got %+v", coord.lastScalingRecommendation)
	}
	if coord.hasActiveScalingMetrics {
		t.Fatalf("expected active scaling metrics to be cleared after completion, got %+v", coord.activeScalingMetrics)
	}
}

func TestCoordinatorCompleteEpochPersistsScalingRecommendation(t *testing.T) {
	store := newMemoryControlStore(1)
	active := &fakeShardClient{}
	startTime := time.Now().UTC().Truncate(time.Second)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	coord.scalingConfig = ScalingPolicyConfig{
		MinShards:                 1,
		MaxShards:                 4,
		TargetRowsPerShard:        2,
		ScaleUpDensityThreshold:   4,
		ScaleDownDensityThreshold: 1,
		MaxShardMultiplier:        2,
	}
	beforeConfig, ok, err := store.GetShardConfig()
	if err != nil || !ok {
		t.Fatalf("expected initial shard config, ok=%t err=%v", ok, err)
	}
	startCoordinatorEpoch(t, coord, active, 1, startTime, 60)
	for i := 0; i < 8; i++ {
		completeCoordinatorUpload(t, coord, 1)
	}

	coord.completeEpoch()

	record, ok, err := store.GetEpochScalingRecommendation(1)
	if err != nil || !ok {
		t.Fatalf("expected persisted epoch scaling recommendation, ok=%t err=%v", ok, err)
	}
	if record.Action != scalingActionGrow || record.RecommendedShardCount != 2 || record.ProposedGlobalTableHeight != 4 || record.ShardConfigVersion != beforeConfig.Version {
		t.Fatalf("unexpected persisted scaling recommendation: %+v", record)
	}
	latest, ok, err := store.GetLatestScalingRecommendation()
	if err != nil || !ok || latest.EpochID != 1 {
		t.Fatalf("expected latest scaling recommendation for epoch 1, ok=%t err=%v record=%+v", ok, err, latest)
	}
	afterConfig, ok, err := store.GetShardConfig()
	if err != nil || !ok || !shardConfigRecordsEqual(afterConfig, beforeConfig) {
		t.Fatalf("expected shard config unchanged, ok=%t err=%v before=%+v after=%+v", ok, err, beforeConfig, afterConfig)
	}
	assertEpochCycleState(t, store, controlstore.EpochCycleStateRecommendationReady)
}

func TestCoordinatorApplyScalingRecommendationWritesNextShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	snapshot := epochShardConfigRecord(shardConfigRecordFromShards(active, 1), 1)
	if err := store.PutEpochShardConfig(1, snapshot); err != nil {
		t.Fatalf("PutEpochShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{},
		1: &fakeShardClient{},
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	var status db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &status); err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.ScalingApplyStatus != scalingApplyStatusApplicable || status.LatestScalingEpochID != 1 || status.LatestScalingRecommendedShards != 2 {
		t.Fatalf("expected applicable scaling status, got %+v", status)
	}

	var reply db.ApplyScalingRecommendationReply
	if err := coord.ApplyScalingRecommendation(&db.ApplyScalingRecommendationArgs{}, &reply); err != nil {
		t.Fatalf("ApplyScalingRecommendation failed: %v", err)
	}
	if !reply.Applied || reply.RecommendationEpochID != 1 || reply.PreviousVersion != 1 || reply.NewVersion != 2 || reply.PreviousShardCount != 1 || reply.NewShardCount != 2 {
		t.Fatalf("unexpected apply reply: %+v", reply)
	}
	if reply.PreviousGlobalTableHeight != db.TABLE_HEIGHT || reply.NewGlobalTableHeight != 2*db.TABLE_HEIGHT {
		t.Fatalf("unexpected apply global heights: %+v", reply)
	}
	config, ok, err := store.GetShardConfig()
	if err != nil || !ok {
		t.Fatalf("GetShardConfig failed after apply, ok=%t err=%v", ok, err)
	}
	if config.Version != 2 || config.ShardCount != 2 || config.GlobalTableHeight != 2*db.TABLE_HEIGHT {
		t.Fatalf("unexpected applied shard config: %+v", config)
	}
	cycle, ok, err := store.GetEpochCycle()
	if err != nil || !ok || cycle.State != controlstore.EpochCycleStateScalingApplied {
		t.Fatalf("expected scaling-applied cycle after apply, ok=%t err=%v cycle=%+v", ok, err, cycle)
	}
	epochSnapshot, ok, err := store.GetEpochShardConfig(1)
	if err != nil || !ok || !shardConfigRecordsEqual(epochSnapshot, snapshot) {
		t.Fatalf("expected epoch snapshot unchanged, ok=%t err=%v got=%+v want=%+v", ok, err, epochSnapshot, snapshot)
	}
}

func TestCoordinatorDryRunScalingRecommendationDoesNotWriteNextShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	initialConfig := shardConfigRecordFromShards(active, 1)
	if err := store.PutShardConfig(initialConfig); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{},
		1: &fakeShardClient{},
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	var reply db.ApplyScalingRecommendationReply
	if err := coord.ApplyScalingRecommendation(&db.ApplyScalingRecommendationArgs{DryRun: true}, &reply); err != nil {
		t.Fatalf("dry-run ApplyScalingRecommendation failed: %v", err)
	}
	if reply.Applied || reply.RecommendationEpochID != 1 || reply.PreviousVersion != 1 || reply.NewVersion != 2 || reply.PreviousShardCount != 1 || reply.NewShardCount != 2 {
		t.Fatalf("unexpected dry-run reply: %+v", reply)
	}
	if reply.PreviousGlobalTableHeight != db.TABLE_HEIGHT || reply.NewGlobalTableHeight != 2*db.TABLE_HEIGHT {
		t.Fatalf("unexpected dry-run global heights: %+v", reply)
	}
	config, ok, err := store.GetShardConfig()
	if err != nil || !ok {
		t.Fatalf("GetShardConfig failed after dry-run, ok=%t err=%v", ok, err)
	}
	if !shardConfigRecordsEqual(config, initialConfig) {
		t.Fatalf("dry-run mutated shard config: got %+v want %+v", config, initialConfig)
	}
	cycle, ok, err := store.GetEpochCycle()
	if err != nil || !ok || cycle.State != controlstore.EpochCycleStateRecommendationReady {
		t.Fatalf("expected dry-run to leave cycle recommendation_ready, ok=%t err=%v cycle=%+v", ok, err, cycle)
	}
}

func TestCoordinatorApplyScalingRecommendationRejectsWithoutLease(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	activeCoord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	_ = activeCoord
	passive := mustStandbyCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-b", testLeaseTTL, testLeaseRenewInterval)

	err := passive.ApplyScalingRecommendation(&db.ApplyScalingRecommendationArgs{}, &db.ApplyScalingRecommendationReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active, got %v", err)
	}
}

func TestCoordinatorSkipScalingRecommendationTransitionsToSkipped(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
	initialConfig := shardConfigRecordFromShards(active, 1)
	if err := store.PutShardConfig(initialConfig); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	var reply db.SkipScalingRecommendationReply
	if err := coord.SkipScalingRecommendation(&db.SkipScalingRecommendationArgs{}, &reply); err != nil {
		t.Fatalf("SkipScalingRecommendation failed: %v", err)
	}
	if !reply.Skipped || reply.RecommendationEpochID != 1 || reply.ShardConfigVersion != 1 || reply.Action != scalingActionGrow {
		t.Fatalf("unexpected skip reply: %+v", reply)
	}
	assertEpochCycleState(t, store, controlstore.EpochCycleStateScalingSkipped)
	config, ok, err := store.GetShardConfig()
	if err != nil || !ok || !shardConfigRecordsEqual(config, initialConfig) {
		t.Fatalf("expected skip to leave shard config unchanged, ok=%t err=%v got=%+v want=%+v", ok, err, config, initialConfig)
	}
}

func TestCoordinatorSkipScalingRecommendationRejectsWithoutLease(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	activeCoord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	_ = activeCoord
	passive := mustStandbyCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-b", testLeaseTTL, testLeaseRenewInterval)

	err := passive.SkipScalingRecommendation(&db.SkipScalingRecommendationArgs{}, &db.SkipScalingRecommendationReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active, got %v", err)
	}
}

func TestCoordinatorSkipScalingRecommendationRejectsActiveEpoch(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setEpochCycleRecommendationReady(t, store, 1, 1)
	epoch := db.EpochMeta{ID: 2, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	if err := store.StartEpoch(epoch, 1); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	err := coord.SkipScalingRecommendation(&db.SkipScalingRecommendationArgs{}, &db.SkipScalingRecommendationReply{})
	if err == nil || !strings.Contains(err.Error(), "epoch is active") {
		t.Fatalf("expected active epoch rejection, got %v", err)
	}
}

func TestCoordinatorSkipScalingRecommendationRejectsWrongCycle(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}, 1: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	err := coord.SkipScalingRecommendation(&db.SkipScalingRecommendationArgs{}, &db.SkipScalingRecommendationReply{})
	if err == nil || !strings.Contains(err.Error(), controlstore.EpochCycleStateIdle) {
		t.Fatalf("expected wrong cycle rejection, got %v", err)
	}
}

func TestValidateAdminFlagsRejectsApplyAndDryRunTogether(t *testing.T) {
	err := validateAdminFlags(0, false, false, true, true, false, false, -1, "127.0.0.1:7000")
	if err == nil || !strings.Contains(err.Error(), "only one admin command") {
		t.Fatalf("expected mutually exclusive apply flags error, got %v", err)
	}
}

func TestCoordinatorApplyScalingRecommendationRejectsBlockedStates(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*testing.T, *memoryControlStore)
		wantStatus string
	}{
		{
			name: "missing latest",
			setup: func(t *testing.T, store *memoryControlStore) {
			},
			wantStatus: scalingApplyStatusMissing,
		},
		{
			name: "keep",
			setup: func(t *testing.T, store *memoryControlStore) {
				if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionKeep, 1, 1)); err != nil {
					t.Fatalf("PutScalingRecommendation failed: %v", err)
				}
			},
			wantStatus: scalingApplyStatusNotApplicable,
		},
		{
			name: "stale recommendation",
			setup: func(t *testing.T, store *memoryControlStore) {
				if err := store.PutScalingRecommendation(testScalingRecommendation(1, 2, scalingActionGrow, 1, 2)); err != nil {
					t.Fatalf("PutScalingRecommendation failed: %v", err)
				}
			},
			wantStatus: scalingApplyStatusNotApplicable,
		},
		{
			name: "active epoch",
			setup: func(t *testing.T, store *memoryControlStore) {
				if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
					t.Fatalf("PutScalingRecommendation failed: %v", err)
				}
				epoch := db.EpochMeta{ID: 2, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
				if err := store.StartEpoch(epoch, 1); err != nil {
					t.Fatalf("StartEpoch failed: %v", err)
				}
			},
			wantStatus: scalingApplyStatusBlockedActiveEpoch,
		},
		{
			name: "missing shards",
			setup: func(t *testing.T, store *memoryControlStore) {
				if err := store.PutScalingRecommendation(testScalingRecommendation(1, 1, scalingActionGrow, 1, 2)); err != nil {
					t.Fatalf("PutScalingRecommendation failed: %v", err)
				}
			},
			wantStatus: scalingApplyStatusBlockedMissingShards,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMemoryControlStore(1)
			active := []ShardConfig{activeOnlyShard(0, 0, db.TABLE_HEIGHT)}
			if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
				t.Fatalf("PutShardConfig failed: %v", err)
			}
			tt.setup(t, store)
			inventory := active
			if tt.wantStatus != scalingApplyStatusBlockedMissingShards {
				inventory = []ShardConfig{
					activeOnlyShard(0, 0, db.TABLE_HEIGHT),
					activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
				}
			}
			coord := mustCoordinatorWithLease(t, inventory, map[int]shardClient{
				0: &fakeShardClient{},
				1: &fakeShardClient{},
			}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

			var status db.CoordinatorStatusReply
			if err := coord.Status(&db.CoordinatorStatusArgs{}, &status); err != nil {
				t.Fatalf("Status failed: %v", err)
			}
			if status.ScalingApplyStatus != tt.wantStatus {
				t.Fatalf("expected status %q, got %+v", tt.wantStatus, status)
			}
			err := coord.ApplyScalingRecommendation(&db.ApplyScalingRecommendationArgs{}, &db.ApplyScalingRecommendationReply{})
			if err == nil || !strings.Contains(err.Error(), tt.wantStatus) {
				t.Fatalf("expected apply rejection containing %q, got %v", tt.wantStatus, err)
			}
		})
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
	}, map[int]shardClient{0: &fakeShardClient{startReply: reply}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
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
	if _, ok, err := store.GetEpochScalingRecommendation(1); err != nil || ok {
		t.Fatalf("expected no scaling recommendation after stale completion, ok=%t err=%v", ok, err)
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

func TestCoordinatorScalingMetricsIncrementAfterSuccessfulUpload3(t *testing.T) {
	active := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	startCoordinatorEpoch(t, coord, active, 1, time.Now().UTC(), 60)

	completeCoordinatorUpload(t, coord, 1)

	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.hasActiveScalingMetrics {
		t.Fatal("expected active scaling metrics")
	}
	if coord.activeScalingMetrics.EpochID != 1 || coord.activeScalingMetrics.AcceptedRequestCount != 1 {
		t.Fatalf("unexpected active scaling metrics: %+v", coord.activeScalingMetrics)
	}
}

func TestCoordinatorScalingMetricsSkipFailedAndRejectedUploads(t *testing.T) {
	active := &fakeShardClient{upload3Err: errors.New("upload3 failed")}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	startCoordinatorEpoch(t, coord, active, 1, time.Now().UTC(), 60)

	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 1}, &up1); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("Upload2 failed: %v", err)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err == nil {
		t.Fatal("expected Upload3 failure")
	}
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT}, &db.UploadReply1{}); err == nil {
		t.Fatal("expected rejected Upload1 for out-of-range row")
	}

	coord.mu.Lock()
	defer coord.mu.Unlock()
	if coord.activeScalingMetrics.AcceptedRequestCount != 0 {
		t.Fatalf("expected failed/rejected uploads not to increment metrics, got %+v", coord.activeScalingMetrics)
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
	if err == nil || err.Error() != coordinatorWireNoActiveEpoch {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
	if fakeClient.upload1Calls != 0 {
		t.Fatalf("expected Upload1 not to reach shard when control store is not accepting, got %d calls", fakeClient.upload1Calls)
	}
}

func TestCoordinatorUpload2AndUpload3FinishAdmittedSessionWhenControlStoreNotAccepting(t *testing.T) {
	fakeClient := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient})
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &up1); err != nil {
		t.Fatalf("Upload1 failed: %v", err)
	}
	if err := coord.controlStore.SetAccepting(epoch.ID, false); err != nil {
		t.Fatalf("SetAccepting failed: %v", err)
	}

	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("expected Upload2 to finish admitted session after accepting closed, got %v", err)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("expected Upload3 to finish admitted session after accepting closed, got %v", err)
	}
	if fakeClient.upload2Calls != 1 || fakeClient.upload3Calls != 1 {
		t.Fatalf("expected shard Upload2/3 to be called once, got upload2=%d upload3=%d", fakeClient.upload2Calls, fakeClient.upload3Calls)
	}
}

func TestStaleCoordinatorRoutesUploadsWhenControlStoreAccepts(t *testing.T) {
	store := newMemoryControlStore(1)
	fakeClient := &fakeShardClient{}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	stealCoordinatorLease(t, store, "coord-b")

	var up1 db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &up1); err != nil {
		t.Fatalf("expected stale coordinator Upload1 to route while store accepts writes, got %v", err)
	}
	if fakeClient.upload1Calls != 1 {
		t.Fatalf("expected Upload1 to reach shard with stale lease, got %d calls", fakeClient.upload1Calls)
	}
	if err := coord.Upload2(&db.UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply2{}); err != nil {
		t.Fatalf("expected stale coordinator Upload2 to route while store accepts writes, got %v", err)
	}
	if err := coord.Upload3(&db.UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &db.UploadReply3{}); err != nil {
		t.Fatalf("expected stale coordinator Upload3 to route while store accepts writes, got %v", err)
	}
}

func TestCoordinatorStartupWritesInitialShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	shards := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}
	coord := mustCoordinatorWithLease(t, shards, map[int]shardClient{
		0: &fakeShardClient{},
		1: &fakeShardClient{},
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	_ = coord

	config, ok, err := store.GetShardConfig()
	if err != nil || !ok {
		t.Fatalf("expected startup to write shard config, ok=%t err=%v", ok, err)
	}
	want := shardConfigRecordFromShards(shards, 1)
	if !shardConfigRecordsEqual(config, want) {
		t.Fatalf("unexpected shard config: got %+v want %+v", config, want)
	}
}

func TestCoordinatorStartupValidatesStoredShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	shards := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(shards, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	coord := mustCoordinatorWithLease(t, shards, map[int]shardClient{
		0: &fakeShardClient{},
		1: &fakeShardClient{},
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	_ = coord
}

func TestCoordinatorStartupAcceptsShardInventorySuperset(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	inventory := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}
	coord := mustCoordinatorWithLease(t, inventory, map[int]shardClient{
		0: &fakeShardClient{},
		1: &fakeShardClient{},
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if reply.CurrentShardCount != 1 || reply.GlobalTableHeight != db.TABLE_HEIGHT || len(reply.Shards) != 1 {
		t.Fatalf("expected active topology to remain one shard, got %+v", reply)
	}
}

func TestCoordinatorUploadsUseActiveShardConfigNotSpareInventory(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	left := &fakeShardClient{}
	spare := &fakeShardClient{}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: left,
		1: spare,
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	setCoordinatorActiveEpoch(t, coord, db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60})

	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{}); err != nil {
		t.Fatalf("Upload1 to active shard failed: %v", err)
	}
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: db.TABLE_HEIGHT}, &db.UploadReply1{}); err == nil {
		t.Fatal("expected Upload1 to spare shard range to fail while shard is inactive")
	}
	if left.upload1Calls != 1 || spare.upload1Calls != 0 {
		t.Fatalf("unexpected routing calls: active=%d spare=%d", left.upload1Calls, spare.upload1Calls)
	}
}

func TestCoordinatorStartEpochUsesActiveShardConfigNotSpareInventory(t *testing.T) {
	store := newMemoryControlStore(1)
	active := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(active, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	startTime := time.Now().UTC().Truncate(time.Second)
	activeClient := &fakeShardClient{startReply: db.StartEpochReply{
		EpochID:      1,
		State:        db.EpochStateActive.String(),
		StartUnix:    startTime.Unix(),
		EndUnix:      startTime.Add(time.Minute).Unix(),
		DurationSecs: 60,
	}}
	spareClient := &fakeShardClient{}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: activeClient,
		1: spareClient,
	}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)

	if err := coord.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60, StartUnix: startTime.Unix()}, &db.StartEpochReply{}); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	if activeClient.startCalls != 1 || spareClient.startCalls != 0 {
		t.Fatalf("expected only active shard to start epoch, active=%d spare=%d", activeClient.startCalls, spareClient.startCalls)
	}
}

func TestCoordinatorStartupRejectsMismatchedStoredShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	stored := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards(stored, 1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	configured := []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}
	_, err := newCoordinatorWithLeaseConfig(configured, map[int]shardClient{
		0: &fakeShardClient{},
	}, store, newMemorySessionStore(), "memory", "coord-a", testLeaseTTL, testLeaseRenewInterval)
	if err == nil || !strings.Contains(err.Error(), "configured shard inventory missing active shard") {
		t.Fatalf("expected shard config mismatch, got %v", err)
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
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
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
	if reply.Role != coordinatorRoleActive || reply.EpochID != 3 || reply.State != db.EpochStateActive.String() || !reply.Accepting {
		t.Fatalf("unexpected coordinator status: %+v", reply)
	}
	if reply.LeaseHolder != defaultCoordinatorLeaseHolder || reply.LeaseFencingToken == 0 || reply.LeaseExpiresUnixMs == 0 || !reply.LeaseActive {
		t.Fatalf("expected active lease status, got %+v", reply)
	}
	if reply.ActiveHolder != defaultCoordinatorLeaseHolder {
		t.Fatalf("expected active holder %q, got %+v", defaultCoordinatorLeaseHolder, reply)
	}
	if reply.SessionStoreBackend != "memory" {
		t.Fatalf("expected memory session store backend, got %+v", reply)
	}
	if reply.CurrentShardCount != 2 || reply.RecommendedNextShardCount != 2 {
		t.Fatalf("unexpected default scaling shard counts: current=%d recommended=%d", reply.CurrentShardCount, reply.RecommendedNextShardCount)
	}
	if reply.GlobalTableHeight != 2*db.TABLE_HEIGHT {
		t.Fatalf("unexpected global table height: %d", reply.GlobalTableHeight)
	}
	if reply.TargetRowsPerShard != db.TABLE_HEIGHT {
		t.Fatalf("unexpected target rows per shard: %d", reply.TargetRowsPerShard)
	}
	if reply.ScalingAction != scalingActionKeep || reply.RequestDensity != 2 {
		t.Fatalf("unexpected default scaling recommendation: action=%q density=%.2f reason=%q", reply.ScalingAction, reply.RequestDensity, reply.ScalingReason)
	}
	if reply.ScalingEpochID != 0 || reply.ScalingAcceptedRequests != int64(2*db.TABLE_HEIGHT*2) || reply.ScalingDurationSecs != 1 {
		t.Fatalf("unexpected default scaling metrics: epoch=%d accepted=%d duration=%d", reply.ScalingEpochID, reply.ScalingAcceptedRequests, reply.ScalingDurationSecs)
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
	standby := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, IsLeader: true, ShardID: 0, PeerState: db.PeerConnectionsReady.String(), ReplicaID: db.CompletedUploadReplicaStandby}}
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
	if !shard.StandbyPromotable || shard.StandbyPromotionStatus != standbyPromotionStatusPromotable {
		t.Fatalf("expected promotable standby, got %+v", shard)
	}
}

func TestPromoteShardStandbyRejectsWithoutLease(t *testing.T) {
	store := newMemoryControlStore(1)
	active := mustCoordinatorWithLease(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	passive := mustStandbyCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-b", testLeaseTTL, testLeaseRenewInterval)
	_ = active

	err := passive.PromoteShardStandby(&db.PromoteShardStandbyArgs{ShardID: 0}, &db.PromoteShardStandbyReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected Coordinator not active, got %v", err)
	}
}

func TestPromoteShardStandbyRejectsMissingStandby(t *testing.T) {
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaActive}}})

	var reply db.PromoteShardStandbyReply
	err := coord.PromoteShardStandby(&db.PromoteShardStandbyArgs{ShardID: 0, Force: true}, &reply)
	if err == nil || reply.Status != standbyPromotionStatusMissingStandby {
		t.Fatalf("expected missing standby rejection, err=%v reply=%+v", err, reply)
	}
}

func TestPromoteShardStandbyRejectsHealthyActiveWithoutForce(t *testing.T) {
	active := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaActive, CompletedUploadCommittedCount: 1}}
	standby := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaStandby, CompletedUploadCommittedCount: 1}}
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.standbyLeaderDialer = func(addr string, timeout time.Duration) (closeableStatusClient, error) {
		return standby, nil
	}

	var reply db.PromoteShardStandbyReply
	err := coord.PromoteShardStandby(&db.PromoteShardStandbyArgs{ShardID: 0}, &reply)
	if err == nil || reply.Status != shardPromotionStatusActiveHealthy {
		t.Fatalf("expected healthy-active rejection, err=%v reply=%+v", err, reply)
	}
}

func TestPromoteShardStandbyForceSwapsConfigAndRefreshesClient(t *testing.T) {
	active := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaActive, CompletedUploadCommittedCount: 2}}
	standbyStatus := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaStandby, CompletedUploadCommittedCount: 2}}
	promoted := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaStandby}}
	store := newMemoryControlStore(1)
	shards := []ShardConfig{activeStandbyShard(0, 0, db.TABLE_HEIGHT)}
	coord := mustCoordinatorWithLease(t, shards, map[int]shardClient{0: active}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	coord.standbyLeaderDialer = func(addr string, timeout time.Duration) (closeableStatusClient, error) {
		return standbyStatus, nil
	}
	coord.shardLeaderDialer = func(addr string) (shardClient, error) {
		if addr != "127.0.0.1:9000" {
			t.Fatalf("unexpected promoted leader address: %s", addr)
		}
		return promoted, nil
	}
	snapshot := epochShardConfigRecord(shardConfigRecordFromShards(shards, 1), 7)
	if err := store.PutEpochShardConfig(7, snapshot); err != nil {
		t.Fatalf("PutEpochShardConfig failed: %v", err)
	}

	var reply db.PromoteShardStandbyReply
	if err := coord.PromoteShardStandby(&db.PromoteShardStandbyArgs{ShardID: 0, Force: true}, &reply); err != nil {
		t.Fatalf("PromoteShardStandby failed: %v", err)
	}
	if !reply.Promoted || reply.PreviousVersion != 1 || reply.NewVersion != 2 || reply.OldActiveLeaderAddr != "127.0.0.1:8000" || reply.NewActiveLeaderAddr != "127.0.0.1:9000" {
		t.Fatalf("unexpected promotion reply: %+v", reply)
	}
	config, ok, err := store.GetShardConfig()
	if err != nil || !ok {
		t.Fatalf("GetShardConfig failed: ok=%t err=%v", ok, err)
	}
	if config.Version != 2 || config.Shards[0].Active.LeaderAddr != "127.0.0.1:9000" || config.Shards[0].Standby == nil || config.Shards[0].Standby.LeaderAddr != "127.0.0.1:8000" {
		t.Fatalf("unexpected promoted shard config: %+v", config)
	}
	if coord.clients[0] != promoted {
		t.Fatalf("expected active client to be refreshed to promoted standby")
	}
	epochSnapshot, ok, err := store.GetEpochShardConfig(7)
	if err != nil || !ok || !shardConfigRecordsEqual(epochSnapshot, snapshot) {
		t.Fatalf("expected epoch snapshot unchanged, ok=%t err=%v snapshot=%+v", ok, err, epochSnapshot)
	}

	epoch := db.EpochMeta{ID: 8, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().Add(time.Minute).UTC(), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	var uploadReply db.UploadReply1
	if err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &uploadReply); err != nil {
		t.Fatalf("Upload1 after promotion failed: %v", err)
	}
	if promoted.upload1Calls != 1 || active.upload1Calls != 0 {
		t.Fatalf("expected promoted client to receive upload, active=%d promoted=%d", active.upload1Calls, promoted.upload1Calls)
	}
}

func TestPromoteShardStandbyForceAllowsActiveUnreachable(t *testing.T) {
	active := &fakeShardClient{statusErr: errors.New("active down")}
	standbyStatus := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ReplicaID: db.CompletedUploadReplicaStandby, CompletedUploadCommittedCount: 2}}
	promoted := &fakeShardClient{}
	coord := mustCoordinator(t, []ShardConfig{
		activeStandbyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.standbyLeaderDialer = func(addr string, timeout time.Duration) (closeableStatusClient, error) {
		return standbyStatus, nil
	}
	coord.shardLeaderDialer = func(addr string) (shardClient, error) {
		return promoted, nil
	}

	var reply db.PromoteShardStandbyReply
	if err := coord.PromoteShardStandby(&db.PromoteShardStandbyArgs{ShardID: 0, Force: true}, &reply); err != nil {
		t.Fatalf("forced promotion with active unreachable failed: %v", err)
	}
	if !reply.Promoted {
		t.Fatalf("expected promotion, got %+v", reply)
	}
}

func TestUploadShardBogusUUIDMapsToShardSessionLost(t *testing.T) {
	client := &fakeShardClient{upload2Err: errors.New(coordinatorWireBogusUUID), upload3Err: errors.New(coordinatorWireBogusUUID)}
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: client})
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().Add(time.Minute).UTC(), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	session := SessionRecord{EpochID: 1, ShardID: 0, GlobalUUID: 101, LocalUUID: 202, CreatedAt: time.Now().UTC()}
	session.HashKey[0] = 9
	if err := coord.sessionStore.PutSession(context.Background(), session); err != nil {
		t.Fatalf("PutSession failed: %v", err)
	}

	err := coord.Upload2(&db.UploadArgs2{Uuid: session.GlobalUUID, HashKey: session.HashKey}, &db.UploadReply2{})
	if err == nil || err.Error() != coordinatorWireShardSessionLost {
		t.Fatalf("expected Shard session lost from Upload2, got %v", err)
	}
	err = coord.Upload3(&db.UploadArgs3{Uuid: session.GlobalUUID, HashKey: session.HashKey}, &db.UploadReply3{})
	if err == nil || err.Error() != coordinatorWireShardSessionLost {
		t.Fatalf("expected Shard session lost from Upload3, got %v", err)
	}

	err = coord.Upload2(&db.UploadArgs2{Uuid: 999, HashKey: session.HashKey}, &db.UploadReply2{})
	if err == nil || err.Error() != coordinatorWireBogusUUID {
		t.Fatalf("expected missing coordinator session to remain Bogus UUID, got %v", err)
	}
}

func TestStandbyPromotionReadinessStatuses(t *testing.T) {
	promotable := db.CoordinatorShardStatus{
		HasStandby:       true,
		ActiveReachable:  true,
		StandbyReachable: true,
		ActiveStatus: db.StatusReply{
			CompletedUploadCommittedCount: 3,
		},
		StandbyStatus: db.StatusReply{
			ReplicaID:                     db.CompletedUploadReplicaStandby,
			CompletedUploadCommittedCount: 3,
		},
	}

	tests := []struct {
		name           string
		update         func(*db.CoordinatorShardStatus)
		wantStatus     string
		wantPromotable bool
	}{
		{
			name:           "promotable",
			wantStatus:     standbyPromotionStatusPromotable,
			wantPromotable: true,
		},
		{
			name: "missing standby",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.HasStandby = false
			},
			wantStatus: standbyPromotionStatusMissingStandby,
		},
		{
			name: "active unreachable",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.ActiveReachable = false
			},
			wantStatus: standbyPromotionStatusUnknownActiveUnreachable,
		},
		{
			name: "standby unreachable",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyReachable = false
			},
			wantStatus: standbyPromotionStatusStandbyUnreachable,
		},
		{
			name: "standby is active replica",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.ReplicaID = db.CompletedUploadReplicaActive
			},
			wantStatus: standbyPromotionStatusStandbyNotReplica,
		},
		{
			name: "standby replica id empty",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.ReplicaID = ""
			},
			wantStatus: standbyPromotionStatusStandbyNotReplica,
		},
		{
			name: "standby queue not drained",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.IngestionQueueDepth = 1
			},
			wantStatus: standbyPromotionStatusStandbyQueueNotDrained,
		},
		{
			name: "standby inflight not drained",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.IngestionInflightCount = 1
			},
			wantStatus: standbyPromotionStatusStandbyQueueNotDrained,
		},
		{
			name: "standby ingestion errors",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.IngestionProcessErrors = 1
			},
			wantStatus: standbyPromotionStatusStandbyErrors,
		},
		{
			name: "standby ledger errors",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.CompletedUploadLedgerCompleteErrors = 1
			},
			wantStatus: standbyPromotionStatusStandbyErrors,
		},
		{
			name: "standby behind",
			update: func(entry *db.CoordinatorShardStatus) {
				entry.StandbyStatus.CompletedUploadCommittedCount = 2
			},
			wantStatus: standbyPromotionStatusStandbyBehind,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := promotable
			if test.update != nil {
				test.update(&entry)
			}
			populateStandbyPromotionReadiness(&entry)
			if entry.StandbyPromotionStatus != test.wantStatus || entry.StandbyPromotable != test.wantPromotable {
				t.Fatalf("unexpected readiness: got status=%q promotable=%t reason=%q", entry.StandbyPromotionStatus, entry.StandbyPromotable, entry.StandbyPromotionReason)
			}
			if entry.ActiveCompletedUploadCommittedCount != entry.ActiveStatus.CompletedUploadCommittedCount {
				t.Fatalf("active committed count was not copied: %+v", entry)
			}
			if entry.StandbyCompletedUploadCommittedCount != entry.StandbyStatus.CompletedUploadCommittedCount {
				t.Fatalf("standby committed count was not copied: %+v", entry)
			}
			if entry.StandbyIngestionQueueDepth != entry.StandbyStatus.IngestionQueueDepth {
				t.Fatalf("standby queue depth was not copied: %+v", entry)
			}
			if entry.StandbyIngestionInflightCount != entry.StandbyStatus.IngestionInflightCount {
				t.Fatalf("standby inflight count was not copied: %+v", entry)
			}
		})
	}
}

func TestCoordinatorStatusScalingDoesNotMutateRoutingOrControlStore(t *testing.T) {
	store := newMemoryControlStore(7)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
		1: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 1}},
	}, store, defaultCoordinatorLeaseHolder, testLeaseTTL, testLeaseRenewInterval)

	beforeConfig, beforeOK, err := store.GetShardConfig()
	if err != nil {
		t.Fatalf("GetShardConfig failed: %v", err)
	}
	beforeShard, err := routeShard(beforeConfig.Shards, db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("routeShard failed before status: %v", err)
	}

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}

	afterConfig, afterOK, err := store.GetShardConfig()
	if err != nil {
		t.Fatalf("GetShardConfig after status failed: %v", err)
	}
	afterShard, err := routeShard(afterConfig.Shards, db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("routeShard failed after status: %v", err)
	}
	if beforeShard.ID != afterShard.ID {
		t.Fatalf("status changed routing: before shard %d after shard %d", beforeShard.ID, afterShard.ID)
	}
	if beforeOK != afterOK || (beforeOK && !shardConfigRecordsEqual(beforeConfig, afterConfig)) {
		t.Fatalf("status changed shard config: before ok=%t config=%+v after ok=%t config=%+v", beforeOK, beforeConfig, afterOK, afterConfig)
	}
	if reply.ScalingAction != scalingActionKeep {
		t.Fatalf("expected default scaling status to keep, got %+v", reply)
	}
}

func TestCoordinatorStatusReportsCompletedScalingMetrics(t *testing.T) {
	active := &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}}
	startTime := time.Now().UTC().Truncate(time.Second)
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.scalingConfig = ScalingPolicyConfig{
		MinShards:                 1,
		MaxShards:                 4,
		TargetRowsPerShard:        2,
		ScaleUpDensityThreshold:   4,
		ScaleDownDensityThreshold: 1,
		MaxShardMultiplier:        2,
	}
	startCoordinatorEpoch(t, coord, active, 1, startTime, 60)
	for i := 0; i < 8; i++ {
		completeCoordinatorUpload(t, coord, 1)
	}
	coord.completeEpoch()

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}
	if reply.ScalingEpochID != 1 || reply.ScalingAcceptedRequests != 8 || reply.ScalingDurationSecs != 60 {
		t.Fatalf("unexpected status scaling metrics: %+v", reply)
	}
	if reply.ScalingAction != scalingActionGrow || reply.RecommendedNextShardCount != 2 || reply.RequestDensity != 4 {
		t.Fatalf("unexpected status scaling recommendation: %+v", reply)
	}
}

func TestCoordinatorStartEpochResetsActiveScalingMetricsAndKeepsLastRecommendation(t *testing.T) {
	active := &fakeShardClient{}
	startTime := time.Now().UTC().Truncate(time.Second)
	coord := mustCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: active})
	coord.scalingConfig = ScalingPolicyConfig{
		MinShards:                 1,
		MaxShards:                 4,
		TargetRowsPerShard:        2,
		ScaleUpDensityThreshold:   4,
		ScaleDownDensityThreshold: 1,
		MaxShardMultiplier:        2,
	}
	startCoordinatorEpoch(t, coord, active, 1, startTime, 60)
	completeCoordinatorUpload(t, coord, 1)
	coord.completeEpoch()
	lastRecommendation := coord.lastScalingRecommendation

	startCoordinatorEpoch(t, coord, active, 2, startTime.Add(2*time.Minute), 60)

	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.hasActiveScalingMetrics || coord.activeScalingMetrics.EpochID != 2 || coord.activeScalingMetrics.AcceptedRequestCount != 0 {
		t.Fatalf("unexpected active metrics after second epoch start: %+v", coord.activeScalingMetrics)
	}
	if !coord.hasLastScalingMetrics || coord.lastScalingRecommendation != lastRecommendation {
		t.Fatalf("expected last recommendation to remain cached, got %+v", coord.lastScalingRecommendation)
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
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
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
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
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
