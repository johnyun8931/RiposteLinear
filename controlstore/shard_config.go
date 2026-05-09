package controlstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"bitbucket.org/henrycg/riposte/db"
)

func epochShardConfigKey(epochID int64) string {
	return fmt.Sprintf("shard-config#epoch#%d", epochID)
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
	sorted := append([]ShardConfig(nil), config.Shards...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].StartRow == sorted[j].StartRow {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].StartRow < sorted[j].StartRow
	})
	expectedStart := 0
	seenIDs := make(map[int]bool)
	for _, shard := range sorted {
		if seenIDs[shard.ID] {
			return fmt.Errorf("duplicate shard id %d", shard.ID)
		}
		seenIDs[shard.ID] = true
		if shard.StartRow < 0 {
			return fmt.Errorf("shard %d has negative start row %d", shard.ID, shard.StartRow)
		}
		if shard.EndRow-shard.StartRow != db.TABLE_HEIGHT {
			return fmt.Errorf("shard %d range [%d,%d) must have height %d", shard.ID, shard.StartRow, shard.EndRow, db.TABLE_HEIGHT)
		}
		if shard.Active.LeaderAddr == "" || shard.Active.FollowerAddr == "" {
			return fmt.Errorf("shard %d missing active pair addresses", shard.ID)
		}
		if shard.StartRow != expectedStart {
			if shard.StartRow < expectedStart {
				return fmt.Errorf("shard %d overlaps previous range at row %d", shard.ID, shard.StartRow)
			}
			return fmt.Errorf("shard map has gap before row %d", shard.StartRow)
		}
		expectedStart = shard.EndRow
	}
	if expectedStart != len(sorted)*db.TABLE_HEIGHT {
		return fmt.Errorf("shard map ends at row %d, want %d", expectedStart, len(sorted)*db.TABLE_HEIGHT)
	}
	return nil
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
