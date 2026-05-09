package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

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

func ensureShardConfig(controlStore ControlStore, inventory []ShardConfig, seed []ShardConfig, canWrite bool) (ShardConfigRecord, error) {
	if len(seed) == 0 {
		seed = inventory
	}
	desired := shardConfigRecordFromShards(seed, 1)
	existing, ok, err := controlStore.GetShardConfig()
	if err != nil {
		return ShardConfigRecord{}, err
	}
	if !ok {
		if !canWrite {
			return ShardConfigRecord{}, errors.New("shard config is missing and coordinator is not active")
		}
		if err := controlStore.PutShardConfig(desired); err != nil {
			return ShardConfigRecord{}, err
		}
		return desired, nil
	}
	if err := validateShardInventoryContainsConfig(inventory, existing); err != nil {
		return ShardConfigRecord{}, err
	}
	return existing, nil
}

func initialShardConfigSeed(inventory []ShardConfig, initialActiveShards int) ([]ShardConfig, error) {
	if initialActiveShards == 0 {
		return append([]ShardConfig(nil), inventory...), nil
	}
	record, err := buildShardConfigFromInventory(inventory, initialActiveShards, 1)
	if err != nil {
		return nil, err
	}
	return record.Shards, nil
}

func currentShardConfigVersion(controlStore ControlStore) int64 {
	config, ok, err := controlStore.GetShardConfig()
	if err != nil || !ok || config.Version <= 0 {
		return 1
	}
	return config.Version
}

func shardConfigForStatus(controlStore ControlStore, fallback []ShardConfig) ShardConfigRecord {
	config, ok, err := controlStore.GetShardConfig()
	if err == nil && ok {
		return config
	}
	return shardConfigRecordFromShards(fallback, 1)
}

func activeShardConfig(controlStore ControlStore) (ShardConfigRecord, error) {
	config, ok, err := controlStore.GetShardConfig()
	if err != nil {
		return ShardConfigRecord{}, err
	}
	if !ok {
		return ShardConfigRecord{}, errors.New("shard config missing")
	}
	if err := validateShardConfigRecord(config); err != nil {
		return ShardConfigRecord{}, err
	}
	return config, nil
}

func validateShardInventoryContainsConfig(inventory []ShardConfig, active ShardConfigRecord) error {
	if err := validateShardConfigRecord(active); err != nil {
		return err
	}
	byID := make(map[int]ShardConfig, len(inventory))
	for _, shard := range inventory {
		byID[shard.ID] = shard
	}
	for _, shard := range active.Shards {
		available, ok := byID[shard.ID]
		if !ok {
			return fmt.Errorf("configured shard inventory missing active shard %d", shard.ID)
		}
		if !shardsEqual(available, shard) && !shardsEqualWithActiveStandbySwapped(available, shard) {
			return fmt.Errorf("configured shard inventory does not match active shard %d", shard.ID)
		}
	}
	return nil
}

func shardsEqual(a ShardConfig, b ShardConfig) bool {
	if a.ID != b.ID ||
		a.StartRow != b.StartRow ||
		a.EndRow != b.EndRow ||
		a.Active != b.Active {
		return false
	}
	if (a.Standby == nil) != (b.Standby == nil) {
		return false
	}
	if a.Standby != nil && *a.Standby != *b.Standby {
		return false
	}
	return true
}

func shardsEqualWithActiveStandbySwapped(inventory ShardConfig, active ShardConfig) bool {
	if inventory.ID != active.ID ||
		inventory.StartRow != active.StartRow ||
		inventory.EndRow != active.EndRow ||
		inventory.Standby == nil ||
		active.Standby == nil {
		return false
	}
	return inventory.Active == *active.Standby && *inventory.Standby == active.Active
}

func routeShard(shards []ShardConfig, row int) (ShardConfig, error) {
	for _, shard := range shards {
		if row >= shard.StartRow && row < shard.EndRow {
			return shard, nil
		}
	}
	return ShardConfig{}, fmt.Errorf("row %d outside shard map", row)
}

func buildShardConfigFromInventory(inventory []ShardConfig, shardCount int, version int64) (ShardConfigRecord, error) {
	if shardCount <= 0 {
		return ShardConfigRecord{}, errors.New("shard count must be positive")
	}
	if shardCount > len(inventory) {
		return ShardConfigRecord{}, fmt.Errorf("recommended shard count %d exceeds configured shard inventory %d", shardCount, len(inventory))
	}
	sorted := append([]ShardConfig(nil), inventory...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	shards := make([]ShardConfig, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shard := sorted[i]
		shard.StartRow = i * db.TABLE_HEIGHT
		shard.EndRow = (i + 1) * db.TABLE_HEIGHT
		shards = append(shards, shard)
	}
	validated, err := validateShardMap(shards)
	if err != nil {
		return ShardConfigRecord{}, err
	}
	return shardConfigRecordFromShards(validated, version), nil
}
