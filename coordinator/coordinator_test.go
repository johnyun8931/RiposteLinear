package main

import (
	"errors"
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

	startReply db.StartEpochReply
	startErr   error
	abortCalls int
	abortErr   error
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
	if f.startErr != nil {
		return f.startErr
	}
	*reply = f.startReply
	return nil
}

func (f *fakeShardClient) EpochStatus(args *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	return nil
}

func (f *fakeShardClient) AbortEpoch(args *db.AbortEpochArgs, reply *db.AbortEpochReply) error {
	f.abortCalls++
	return f.abortErr
}

func mustCoordinator(t *testing.T, shards []ShardConfig, clients map[int]shardClient) *Coordinator {
	t.Helper()
	coord, err := NewCoordinator(shards, clients)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	return coord
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
	coord.epoch = db.EpochMeta{ID: 1, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}

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

func TestCoordinatorStartEpochProducesSharedMetadata(t *testing.T) {
	startTime := time.Unix(1200, 0).UTC()
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
}
