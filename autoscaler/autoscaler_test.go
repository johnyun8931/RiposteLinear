package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/controlstore"
	"bitbucket.org/henrycg/riposte/db"
)

type fakeCoordinatorAdmin struct {
	dryRunReply db.ApplyScalingRecommendationReply
	dryRunErr   error
	applyReply  db.ApplyScalingRecommendationReply
	applyErr    error
	calls       []bool
}

func (f *fakeCoordinatorAdmin) ApplyScalingRecommendation(_ context.Context, dryRun bool) (db.ApplyScalingRecommendationReply, error) {
	f.calls = append(f.calls, dryRun)
	if dryRun {
		return f.dryRunReply, f.dryRunErr
	}
	return f.applyReply, f.applyErr
}

func testAutoscaler(control controlstore.ControlStore, coordinator *fakeCoordinatorAdmin, apply bool) (*autoscaler, *bytes.Buffer) {
	var buf bytes.Buffer
	return &autoscaler{
		control:     control,
		coordinator: coordinator,
		apply:       apply,
		logger:      log.New(&buf, "", 0),
	}, &buf
}

func activeOnlyShard(id, start, end int) controlstore.ShardConfig {
	return controlstore.ShardConfig{
		ID:       id,
		StartRow: start,
		EndRow:   end,
		Active: controlstore.PairConfig{
			LeaderAddr:   "127.0.0.1:8000",
			FollowerAddr: "127.0.0.1:8001",
		},
	}
}

func shardConfigRecord(version int64) controlstore.ShardConfigRecord {
	return controlstore.ShardConfigRecord{
		Key:               "shard-config",
		Version:           version,
		ShardCount:        1,
		RowsPerShard:      db.TABLE_HEIGHT,
		GlobalTableHeight: db.TABLE_HEIGHT,
		Shards: []controlstore.ShardConfig{
			activeOnlyShard(0, 0, db.TABLE_HEIGHT),
		},
	}
}

func scalingRecommendationRecord(epochID int64, version int64, action string) controlstore.ScalingRecommendationRecord {
	return controlstore.ScalingRecommendationRecord{
		Key:                       "scaling#epoch#1",
		EpochID:                   epochID,
		AcceptedRequestCount:      8,
		DurationSeconds:           60,
		CurrentShardCount:         1,
		RecommendedShardCount:     2,
		TargetRowsPerShard:        db.TABLE_HEIGHT,
		RequestDensity:            4,
		Action:                    action,
		Reason:                    action,
		ProposedGlobalTableHeight: 2 * db.TABLE_HEIGHT,
		ShardConfigVersion:        version,
		CreatedAt:                 time.Unix(1000, 0).UTC(),
	}
}

func applicableControl(t *testing.T) *controlstore.MemoryControlStore {
	t.Helper()
	store := controlstore.NewMemoryControlStore(1)
	if err := store.PutShardConfig(shardConfigRecord(1)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := store.PutScalingRecommendation(scalingRecommendationRecord(1, 1, "grow")); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setRecommendationReadyCycle(t, store)
	return store
}

func setRecommendationReadyCycle(t *testing.T, store controlstore.ControlStore) {
	t.Helper()
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateIdle, controlstore.EpochCycleStateActive, controlstore.EpochCycleRecord{
		EpochID:            1,
		ShardConfigVersion: 1,
	}); err != nil {
		t.Fatalf("cycle idle -> active failed: %v", err)
	}
	if err := store.PutEpochCycleTransition(controlstore.EpochCycleStateActive, controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleRecord{
		EpochID:                      1,
		ShardConfigVersion:           1,
		ScalingRecommendationEpochID: 1,
	}); err != nil {
		t.Fatalf("cycle active -> recommendation_ready failed: %v", err)
	}
}

func applicableDryRunReply() db.ApplyScalingRecommendationReply {
	return db.ApplyScalingRecommendationReply{
		RecommendationEpochID: 1,
		PreviousVersion:       1,
		NewVersion:            2,
		PreviousShardCount:    1,
		NewShardCount:         2,
		Status:                scalingApplyStatusOK,
	}
}

func TestEvaluateOnceSkipsMissingRecommendation(t *testing.T) {
	coord := &fakeCoordinatorAdmin{}
	control := controlstore.NewMemoryControlStore(1)
	setRecommendationReadyCycle(t, control)
	scaler, logs := testAutoscaler(control, coord, false)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("evaluateOnce failed: %v", err)
	}
	if decision != decisionMissing || len(coord.calls) != 0 || !strings.Contains(logs.String(), "decision=missing") {
		t.Fatalf("unexpected decision=%q calls=%v logs=%q", decision, coord.calls, logs.String())
	}
}

func TestEvaluateOnceSkipsStaleRecommendation(t *testing.T) {
	control := controlstore.NewMemoryControlStore(1)
	if err := control.PutShardConfig(shardConfigRecord(2)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
	if err := control.PutScalingRecommendation(scalingRecommendationRecord(1, 1, "grow")); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	setRecommendationReadyCycle(t, control)
	coord := &fakeCoordinatorAdmin{}
	scaler, logs := testAutoscaler(control, coord, false)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("evaluateOnce failed: %v", err)
	}
	if decision != decisionStale || len(coord.calls) != 0 || !strings.Contains(logs.String(), "current_version=2") {
		t.Fatalf("unexpected decision=%q calls=%v logs=%q", decision, coord.calls, logs.String())
	}
}

func TestEvaluateOnceSkipsActiveEpoch(t *testing.T) {
	control := applicableControl(t)
	epoch := db.EpochMeta{ID: 2, State: db.EpochStateActive, StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Minute), DurationSeconds: 60}
	if err := control.StartEpoch(epoch, 1); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	coord := &fakeCoordinatorAdmin{}
	scaler, logs := testAutoscaler(control, coord, false)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("evaluateOnce failed: %v", err)
	}
	if decision != decisionBlockedActive || len(coord.calls) != 0 || !strings.Contains(logs.String(), "decision=blocked_active_epoch") {
		t.Fatalf("unexpected decision=%q calls=%v logs=%q", decision, coord.calls, logs.String())
	}
}

func TestEvaluateOnceDryRunOnly(t *testing.T) {
	coord := &fakeCoordinatorAdmin{dryRunReply: applicableDryRunReply()}
	scaler, logs := testAutoscaler(applicableControl(t), coord, false)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("evaluateOnce failed: %v", err)
	}
	if decision != decisionDryRunApplicable || len(coord.calls) != 1 || coord.calls[0] != true || !strings.Contains(logs.String(), "decision=dry_run_applicable") {
		t.Fatalf("unexpected decision=%q calls=%v logs=%q", decision, coord.calls, logs.String())
	}
}

func TestEvaluateOnceAppliesWhenEnabled(t *testing.T) {
	coord := &fakeCoordinatorAdmin{
		dryRunReply: applicableDryRunReply(),
		applyReply:  applicableDryRunReply(),
	}
	scaler, logs := testAutoscaler(applicableControl(t), coord, true)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("evaluateOnce failed: %v", err)
	}
	if decision != decisionApplied || len(coord.calls) != 2 || coord.calls[0] != true || coord.calls[1] != false || !strings.Contains(logs.String(), "decision=applied") {
		t.Fatalf("unexpected decision=%q calls=%v logs=%q", decision, coord.calls, logs.String())
	}
}

func TestEvaluateOnceApplyFailureDoesNotReturnError(t *testing.T) {
	coord := &fakeCoordinatorAdmin{
		dryRunReply: applicableDryRunReply(),
		applyErr:    errors.New("apply failed"),
	}
	scaler, logs := testAutoscaler(applicableControl(t), coord, true)

	decision, err := scaler.evaluateOnce(context.Background())
	if err != nil {
		t.Fatalf("apply failure should not crash loop mode: %v", err)
	}
	if decision != decisionApplyFailed || !strings.Contains(logs.String(), "decision=apply_failed") {
		t.Fatalf("unexpected decision=%q logs=%q", decision, logs.String())
	}
}

func TestRunOnceExitsAfterOneCycle(t *testing.T) {
	coord := &fakeCoordinatorAdmin{dryRunReply: applicableDryRunReply()}
	scaler, _ := testAutoscaler(applicableControl(t), coord, false)
	if err := scaler.run(context.Background(), 1, true); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	if len(coord.calls) != 1 {
		t.Fatalf("expected one coordinator call, got %d", len(coord.calls))
	}
}

func TestValidateFlags(t *testing.T) {
	if err := validateFlags("", "table", 1); err == nil {
		t.Fatal("expected missing coordinator error")
	}
	if err := validateFlags("127.0.0.1:8630", "", 1); err == nil {
		t.Fatal("expected missing control table error")
	}
	if err := validateFlags("127.0.0.1:8630", "table", 0); err == nil {
		t.Fatal("expected interval error")
	}
}
