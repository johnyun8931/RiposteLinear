package main

import (
	"context"
	"errors"
	"testing"
	"time"

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

func mustStandbyCoordinator(t *testing.T, shards []ShardConfig, clients map[int]shardClient, store ControlStore, holder string, ttl time.Duration, renewInterval time.Duration) *Coordinator {
	t.Helper()
	coord, err := newCoordinatorWithStandbyConfig(shards, clients, store, newMemorySessionStore(), "memory", holder, ttl, renewInterval, true)
	if err != nil {
		t.Fatalf("newCoordinatorWithStandbyConfig failed: %v", err)
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
	if len(store.sessions) != 0 {
		t.Fatalf("expected persisted session cleanup after shard failure, got %+v", store.sessions)
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

func TestPassiveCoordinatorRejectsMutations(t *testing.T) {
	store := newMemoryControlStore(1)
	mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: &fakeShardClient{}}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	fakeClient := &fakeShardClient{}
	standby := mustStandbyCoordinator(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient}, store, "coord-b", testLeaseTTL, testLeaseRenewInterval)

	err := standby.StartEpoch(&db.StartEpochArgs{DurationSeconds: 60}, &db.StartEpochReply{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active error, got %v", err)
	}
	if fakeClient.startCalls != 0 {
		t.Fatalf("expected passive coordinator not to contact shard, got %d calls", fakeClient.startCalls)
	}

	err = standby.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active error, got %v", err)
	}
	if fakeClient.upload1Calls != 0 {
		t.Fatalf("expected passive Upload1 not to reach shard, got %d calls", fakeClient.upload1Calls)
	}

	err = standby.Upload2(&db.UploadArgs2{Uuid: 12345}, &db.UploadReply2{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active from Upload2, got %v", err)
	}
	err = standby.Upload3(&db.UploadArgs3{Uuid: 12345}, &db.UploadReply3{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active from Upload3, got %v", err)
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

func TestCoordinatorUpload1RejectsWithStaleLease(t *testing.T) {
	store := newMemoryControlStore(1)
	fakeClient := &fakeShardClient{}
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, map[int]shardClient{0: fakeClient}, store, "coord-a", testLeaseTTL, testLeaseRenewInterval)
	epoch := db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	setCoordinatorActiveEpoch(t, coord, epoch)
	stealCoordinatorLease(t, store, "coord-b")

	err := coord.Upload1(&db.UploadArgs1{RouteRow: 0}, &db.UploadReply1{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active error, got %v", err)
	}
	if fakeClient.upload1Calls != 0 {
		t.Fatalf("expected Upload1 not to reach shard with stale lease, got %d calls", fakeClient.upload1Calls)
	}

	err = coord.Upload2(&db.UploadArgs2{Uuid: 12345}, &db.UploadReply2{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active from Upload2, got %v", err)
	}
	err = coord.Upload3(&db.UploadArgs3{Uuid: 12345}, &db.UploadReply3{})
	if err == nil || err.Error() != coordinatorWireNotActive {
		t.Fatalf("expected coordinator not active from Upload3, got %v", err)
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

func TestCoordinatorStatusScalingDoesNotMutateRoutingOrControlStore(t *testing.T) {
	store := newMemoryControlStore(7)
	coord := mustCoordinatorWithLease(t, []ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		activeOnlyShard(1, db.TABLE_HEIGHT, 2*db.TABLE_HEIGHT),
	}, map[int]shardClient{
		0: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 0}},
		1: &fakeShardClient{statusReply: db.StatusReply{Healthy: true, ShardID: 1}},
	}, store, defaultCoordinatorLeaseHolder, testLeaseTTL, testLeaseRenewInterval)

	beforeVersion := store.ShardConfigVersion()
	beforeShard, err := coord.routeShard(db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("routeShard failed before status: %v", err)
	}

	var reply db.CoordinatorStatusReply
	if err := coord.Status(&db.CoordinatorStatusArgs{}, &reply); err != nil {
		t.Fatalf("Coordinator.Status failed: %v", err)
	}

	afterShard, err := coord.routeShard(db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("routeShard failed after status: %v", err)
	}
	if beforeShard.ID != afterShard.ID {
		t.Fatalf("status changed routing: before shard %d after shard %d", beforeShard.ID, afterShard.ID)
	}
	if version := store.ShardConfigVersion(); version != beforeVersion {
		t.Fatalf("status changed shard config version: before %d after %d", beforeVersion, version)
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
