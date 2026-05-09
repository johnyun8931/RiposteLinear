package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	latestScalingRecommendationKey = "scaling#latest"
)

func epochScalingRecommendationKey(epochID int64) string {
	return fmt.Sprintf("scaling#epoch#%d", epochID)
}

func scalingRecommendationRecord(metrics EpochScalingMetrics, rec ScalingRecommendation, shardConfigVersion int64, createdAt time.Time) ScalingRecommendationRecord {
	return ScalingRecommendationRecord{
		Key:                       epochScalingRecommendationKey(metrics.EpochID),
		EpochID:                   metrics.EpochID,
		AcceptedRequestCount:      metrics.AcceptedRequestCount,
		DurationSeconds:           metrics.DurationSeconds,
		CurrentShardCount:         metrics.CurrentShardCount,
		RecommendedShardCount:     rec.RecommendedShardCount,
		TargetRowsPerShard:        rec.TargetRowsPerShard,
		RequestDensity:            rec.RequestDensity,
		Action:                    rec.Action,
		Reason:                    rec.Reason,
		ProposedGlobalTableHeight: rec.RecommendedShardCount * rec.TargetRowsPerShard,
		ShardConfigVersion:        shardConfigVersion,
		CreatedAt:                 createdAt,
	}
}

func validateScalingRecommendationRecord(record ScalingRecommendationRecord) error {
	if record.EpochID <= 0 {
		return errors.New("scaling recommendation epoch id must be positive")
	}
	if record.AcceptedRequestCount < 0 {
		return errors.New("scaling recommendation accepted request count must be non-negative")
	}
	if record.DurationSeconds <= 0 {
		return errors.New("scaling recommendation duration must be positive")
	}
	if record.CurrentShardCount <= 0 {
		return errors.New("scaling recommendation current shard count must be positive")
	}
	if record.RecommendedShardCount <= 0 {
		return errors.New("scaling recommendation recommended shard count must be positive")
	}
	if record.TargetRowsPerShard <= 0 {
		return errors.New("scaling recommendation target rows per shard must be positive")
	}
	if math.IsNaN(record.RequestDensity) || math.IsInf(record.RequestDensity, 0) || record.RequestDensity < 0 {
		return errors.New("scaling recommendation request density must be finite and non-negative")
	}
	if record.Action != scalingActionGrow && record.Action != scalingActionShrink && record.Action != scalingActionKeep {
		return fmt.Errorf("unknown scaling recommendation action %q", record.Action)
	}
	if record.ProposedGlobalTableHeight != record.RecommendedShardCount*record.TargetRowsPerShard {
		return fmt.Errorf("scaling recommendation proposed global table height=%d, want %d", record.ProposedGlobalTableHeight, record.RecommendedShardCount*record.TargetRowsPerShard)
	}
	if record.ShardConfigVersion <= 0 {
		return errors.New("scaling recommendation shard config version must be positive")
	}
	return nil
}

func scalingRecommendationRecordsEqual(a ScalingRecommendationRecord, b ScalingRecommendationRecord) bool {
	a.Key = ""
	b.Key = ""
	return a == b
}

func scalingRecommendationFingerprint(record ScalingRecommendationRecord) string {
	normalized := record
	normalized.Key = ""
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
