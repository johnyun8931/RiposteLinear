package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"bitbucket.org/henrycg/riposte/db"
)

func globalTableHeightForShards(shards []ShardConfig) int {
	return len(shards) * db.TABLE_HEIGHT
}

func shardConfigRecordFromShards(shards []ShardConfig, version int64) ShardConfigRecord {
	copied := append([]ShardConfig(nil), shards...)
	return ShardConfigRecord{
		Key:               "shard-config",
		Version:           version,
		ShardCount:        len(copied),
		RowsPerShard:      db.TABLE_HEIGHT,
		GlobalTableHeight: globalTableHeightForShards(copied),
		Shards:            copied,
	}
}

func validateShardConfigRecord(config ShardConfigRecord) error {
	if config.Version <= 0 {
		return errors.New("shard config version must be positive")
	}
	if config.ShardCount != len(config.Shards) {
		return fmt.Errorf("shard config count mismatch: count=%d entries=%d", config.ShardCount, len(config.Shards))
	}
	if config.RowsPerShard != db.TABLE_HEIGHT {
		return fmt.Errorf("shard config rows_per_shard=%d, want %d", config.RowsPerShard, db.TABLE_HEIGHT)
	}
	if config.GlobalTableHeight != config.ShardCount*config.RowsPerShard {
		return fmt.Errorf("shard config global_table_height=%d, want %d", config.GlobalTableHeight, config.ShardCount*config.RowsPerShard)
	}
	_, err := validateShardMap(config.Shards)
	return err
}

func epochShardConfigKey(epochID int64) string {
	return fmt.Sprintf("shard-config#epoch#%d", epochID)
}

func shardConfigRecordsEqual(a ShardConfigRecord, b ShardConfigRecord) bool {
	if a.Key != b.Key ||
		a.Version != b.Version ||
		a.ShardCount != b.ShardCount ||
		a.RowsPerShard != b.RowsPerShard ||
		a.GlobalTableHeight != b.GlobalTableHeight ||
		len(a.Shards) != len(b.Shards) {
		return false
	}
	for i := range a.Shards {
		if a.Shards[i].ID != b.Shards[i].ID ||
			a.Shards[i].StartRow != b.Shards[i].StartRow ||
			a.Shards[i].EndRow != b.Shards[i].EndRow ||
			a.Shards[i].Active != b.Shards[i].Active {
			return false
		}
		if (a.Shards[i].Standby == nil) != (b.Shards[i].Standby == nil) {
			return false
		}
		if a.Shards[i].Standby != nil && *a.Shards[i].Standby != *b.Shards[i].Standby {
			return false
		}
	}
	return true
}

func epochShardConfigRecord(config ShardConfigRecord, epochID int64) ShardConfigRecord {
	snapshot := config
	snapshot.Key = epochShardConfigKey(epochID)
	snapshot.Shards = append([]ShardConfig(nil), config.Shards...)
	return snapshot
}

func shardConfigFingerprint(config ShardConfigRecord) string {
	normalized := config
	normalized.Key = ""
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
