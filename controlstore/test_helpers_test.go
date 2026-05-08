package controlstore

import (
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

type testEpochScalingMetrics struct {
	EpochID              int64
	CurrentShardCount    int
	AcceptedRequestCount int64
	DurationSeconds      int64
}

type testScalingRecommendation struct {
	RecommendedShardCount int
	TargetRowsPerShard    int
	RequestDensity        float64
	Action                string
	Reason                string
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

func shardConfigRecordFromShards(shards []ShardConfig, version int64) ShardConfigRecord {
	copied := append([]ShardConfig(nil), shards...)
	return ShardConfigRecord{
		Key:               "shard-config",
		Version:           version,
		ShardCount:        len(copied),
		RowsPerShard:      db.TABLE_HEIGHT,
		GlobalTableHeight: len(copied) * db.TABLE_HEIGHT,
		Shards:            copied,
	}
}

func epochShardConfigRecord(config ShardConfigRecord, epochID int64) ShardConfigRecord {
	snapshot := config
	snapshot.Key = epochShardConfigKey(epochID)
	snapshot.Shards = append([]ShardConfig(nil), config.Shards...)
	return snapshot
}

func scalingRecommendationRecord(metrics testEpochScalingMetrics, rec testScalingRecommendation, shardConfigVersion int64, createdAt time.Time) ScalingRecommendationRecord {
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
