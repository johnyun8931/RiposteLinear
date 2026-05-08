package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

func TestMemoryControlStoreLeaseAcquireRenewAndStaleFence(t *testing.T) {
	store := newMemoryControlStore(1)
	now := time.Unix(1000, 0).UTC()

	lease, err := store.AcquireLease(now, "coord-a", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if lease.Holder != "coord-a" || lease.FencingToken != 1 || !lease.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	_, err = store.AcquireLease(now.Add(time.Second), "coord-b", time.Minute)
	if !errors.Is(err, errLeaseHeld) {
		t.Fatalf("expected held lease error, got %v", err)
	}

	renewed, err := store.RenewLease(now.Add(2*time.Second), "coord-a", lease.FencingToken, 2*time.Minute)
	if err != nil {
		t.Fatalf("RenewLease failed: %v", err)
	}
	if renewed.FencingToken != lease.FencingToken || !renewed.ExpiresAt.Equal(now.Add(2*time.Second).Add(2*time.Minute)) {
		t.Fatalf("unexpected renewed lease: %+v", renewed)
	}

	_, err = store.RenewLease(now.Add(3*time.Second), "coord-b", lease.FencingToken, time.Minute)
	if !errors.Is(err, errStaleFence) {
		t.Fatalf("expected stale fence error, got %v", err)
	}

	next, err := store.AcquireLease(now.Add(3*time.Minute), "coord-b", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease after expiry failed: %v", err)
	}
	if next.Holder != "coord-b" || next.FencingToken != 2 {
		t.Fatalf("expected new holder with incremented fencing token, got %+v", next)
	}
}

func TestMemoryControlStoreEpochAndAcceptingState(t *testing.T) {
	store := newMemoryControlStore(1)
	start := time.Unix(2000, 0).UTC()
	epoch := db.EpochMeta{
		ID:              7,
		State:           db.EpochStateActive,
		StartTime:       start,
		EndTime:         start.Add(time.Hour),
		DurationSeconds: int64(time.Hour / time.Second),
	}

	if err := store.StartEpoch(epoch, 3); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	got, ok := store.CurrentEpoch()
	if !ok || got.ID != epoch.ID || got.State != db.EpochStateActive {
		t.Fatalf("unexpected current epoch ok=%t epoch=%+v", ok, got)
	}
	accepting, err := store.Accepting(epoch.ID)
	if err != nil || !accepting {
		t.Fatalf("expected accepting active epoch, accepting=%t err=%v", accepting, err)
	}

	if err := store.SetAccepting(epoch.ID, false); err != nil {
		t.Fatalf("SetAccepting failed: %v", err)
	}
	accepting, err = store.Accepting(epoch.ID)
	if err != nil || accepting {
		t.Fatalf("expected accepting false, accepting=%t err=%v", accepting, err)
	}

	completed, err := store.CompleteEpoch(epoch.ID)
	if err != nil {
		t.Fatalf("CompleteEpoch failed: %v", err)
	}
	if completed.State != db.EpochStateCompleted {
		t.Fatalf("expected completed epoch, got %+v", completed)
	}
	accepting, err = store.Accepting(epoch.ID)
	if err != nil || accepting {
		t.Fatalf("expected completed epoch not accepting, accepting=%t err=%v", accepting, err)
	}

	if err := store.SetAccepting(epoch.ID+1, true); !errors.Is(err, errEpochMismatch) {
		t.Fatalf("expected epoch mismatch, got %v", err)
	}
}

func TestMemoryControlStoreShardConfig(t *testing.T) {
	store := newMemoryControlStore(1)
	if _, ok, err := store.GetShardConfig(); err != nil || ok {
		t.Fatalf("expected no initial shard config, ok=%t err=%v", ok, err)
	}
	config := shardConfigRecordFromShards([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, 4)
	if err := store.PutShardConfig(config); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	got, ok, err := store.GetShardConfig()
	if err != nil || !ok || !shardConfigRecordsEqual(got, config) {
		t.Fatalf("unexpected shard config ok=%t err=%v config=%+v", ok, err, got)
	}
	stale := config
	stale.Version = 3
	if err := store.PutShardConfig(stale); err == nil {
		t.Fatal("expected stale shard config version to fail")
	}
}

func TestMemoryControlStoreEpochShardConfigSnapshot(t *testing.T) {
	store := newMemoryControlStore(1)
	config := shardConfigRecordFromShards([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, 2)
	snapshot := epochShardConfigRecord(config, 9)

	if err := store.PutEpochShardConfig(9, snapshot); err != nil {
		t.Fatalf("PutEpochShardConfig failed: %v", err)
	}
	got, ok, err := store.GetEpochShardConfig(9)
	if err != nil || !ok || !shardConfigRecordsEqual(got, snapshot) {
		t.Fatalf("unexpected epoch shard config ok=%t err=%v config=%+v", ok, err, got)
	}
	if err := store.PutEpochShardConfig(9, snapshot); err != nil {
		t.Fatalf("idempotent PutEpochShardConfig failed: %v", err)
	}
	changed := snapshot
	changed.Shards[0].Active.LeaderAddr = "127.0.0.1:9999"
	if err := store.PutEpochShardConfig(9, changed); err == nil {
		t.Fatal("expected conflicting epoch shard config snapshot to fail")
	}
}

func TestMemoryIngestionQueueEnqueueReceiveAck(t *testing.T) {
	queue := newMemoryIngestionQueue()
	ctx := context.Background()

	firstID, err := queue.Enqueue(ctx, IngestionMessage{EpochID: 1, ShardID: 0, GlobalUUID: 10, LocalUUID: 20, RouteRow: 7})
	if err != nil {
		t.Fatalf("Enqueue first failed: %v", err)
	}
	secondID, err := queue.Enqueue(ctx, IngestionMessage{EpochID: 1, ShardID: 1, GlobalUUID: 11, LocalUUID: 21, RouteRow: 128})
	if err != nil {
		t.Fatalf("Enqueue second failed: %v", err)
	}
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatalf("unexpected message ids: first=%q second=%q", firstID, secondID)
	}

	items, err := queue.Receive(ctx, 1)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if len(items) != 1 || items[0].Message.ID != firstID || items[0].ReceiptHandle == "" {
		t.Fatalf("unexpected first receive: %+v", items)
	}

	items, err = queue.Receive(ctx, 10)
	if err != nil {
		t.Fatalf("Receive second failed: %v", err)
	}
	if len(items) != 1 || items[0].Message.ID != secondID {
		t.Fatalf("unexpected second receive: %+v", items)
	}

	if err := queue.Ack(ctx, items[0].ReceiptHandle); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	items, err = queue.Receive(ctx, 10)
	if err != nil {
		t.Fatalf("Receive after ack failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no available messages after receive/ack, got %+v", items)
	}
}

func TestMemoryIngestionQueueValidationAndContextCancellation(t *testing.T) {
	queue := newMemoryIngestionQueue()
	ctx := context.Background()

	if _, err := queue.Enqueue(ctx, IngestionMessage{}); err == nil {
		t.Fatal("expected enqueue without epoch to fail")
	}
	if _, err := queue.Receive(ctx, 0); err == nil {
		t.Fatal("expected receive with non-positive max to fail")
	}
	if err := queue.Ack(ctx, "missing"); err == nil {
		t.Fatal("expected unknown ack receipt to fail")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := queue.Enqueue(cancelled, IngestionMessage{EpochID: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected enqueue context cancellation, got %v", err)
	}
	if _, err := queue.Receive(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected receive context cancellation, got %v", err)
	}
	if err := queue.Ack(cancelled, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ack context cancellation, got %v", err)
	}
}

func TestMemorySessionStorePutGetDelete(t *testing.T) {
	store := newMemorySessionStore()
	ctx := context.Background()
	createdAt := time.Unix(3000, 0).UTC()
	var hashKey [32]byte
	hashKey[0] = 7
	session := SessionRecord{
		EpochID:       5,
		ShardID:       1,
		GlobalUUID:    10,
		LocalUUID:     20,
		HashKey:       hashKey,
		GlobalRow:     300,
		LocalRow:      44,
		ShardStartRow: 256,
		CreatedAt:     createdAt,
	}

	if err := store.PutSession(ctx, session); err != nil {
		t.Fatalf("PutSession failed: %v", err)
	}
	if err := store.PutSession(ctx, session); !errors.Is(err, errSessionExists) {
		t.Fatalf("expected duplicate session error, got %v", err)
	}

	got, err := store.GetSession(ctx, session.GlobalUUID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if got != session {
		t.Fatalf("unexpected session: got %+v want %+v", got, session)
	}

	if err := store.DeleteSession(ctx, session.GlobalUUID); err != nil {
		t.Fatalf("DeleteSession failed: %v", err)
	}
	if _, err := store.GetSession(ctx, session.GlobalUUID); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected missing session after delete, got %v", err)
	}
	if err := store.DeleteSession(ctx, session.GlobalUUID); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected missing session delete error, got %v", err)
	}
}

func TestMemorySessionStoreValidationAndContextCancellation(t *testing.T) {
	store := newMemorySessionStore()
	ctx := context.Background()

	if err := store.PutSession(ctx, SessionRecord{}); err == nil {
		t.Fatal("expected invalid session to fail")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	session := SessionRecord{EpochID: 1, GlobalUUID: 1, LocalUUID: 1}
	if err := store.PutSession(cancelled, session); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected put context cancellation, got %v", err)
	}
	if _, err := store.GetSession(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected get context cancellation, got %v", err)
	}
	if err := store.DeleteSession(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected delete context cancellation, got %v", err)
	}
}
