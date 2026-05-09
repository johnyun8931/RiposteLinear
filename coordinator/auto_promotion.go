package main

import (
	"fmt"
	"log"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	autoPromotionStatusDisabled                = "disabled"
	autoPromotionStatusActiveReachable         = "active_reachable"
	autoPromotionStatusWaitingFailureThreshold = "waiting_failure_threshold"
	autoPromotionStatusNoActiveBaseline        = "no_active_baseline"
	autoPromotionStatusCooldown                = "cooldown"
	autoPromotionStatusNotAuthoritative        = "not_authoritative"
	autoPromotionStatusEligible                = "eligible"
	autoPromotionStatusPromoted                = "promoted"
	autoPromotionStatusPromotionFailed         = "promotion_failed"
)

type autoPromotionConfig struct {
	Enabled          bool
	CheckInterval    time.Duration
	FailureThreshold int
	Cooldown         time.Duration
}

type autoPromotionShardState struct {
	FailureCount           int
	Eligible               bool
	Status                 string
	Reason                 string
	LastReachableActive    db.StatusReply
	HasLastReachableActive bool
	LastPromotionAt        time.Time
}

func defaultAutoPromotionConfig() autoPromotionConfig {
	return autoPromotionConfig{
		CheckInterval:    defaultShardHealthInterval,
		FailureThreshold: defaultAutoPromotionFailureThreshold,
		Cooldown:         defaultAutoPromotionCooldown,
	}
}

func validateAutoPromotionConfig(config autoPromotionConfig) error {
	if config.CheckInterval <= 0 {
		return fmt.Errorf("auto promotion check interval must be positive")
	}
	if config.FailureThreshold <= 0 {
		return fmt.Errorf("auto promotion failure threshold must be positive")
	}
	if config.Cooldown <= 0 {
		return fmt.Errorf("auto promotion cooldown must be positive")
	}
	return nil
}

func (c *Coordinator) configureAutoPromotion(config autoPromotionConfig) error {
	if err := validateAutoPromotionConfig(config); err != nil {
		return err
	}
	c.actorCall(func() {
		c.autoPromotion = config
	})
	return nil
}

func (c *Coordinator) startAutoPromotionLoop() {
	config := c.autoPromotionConfigSnapshot()
	if !config.Enabled {
		return
	}

	c.mu.Lock()
	if c.autoPromotionStopCh != nil {
		c.mu.Unlock()
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	c.autoPromotionStopCh = stopCh
	c.autoPromotionDoneCh = doneCh
	c.mu.Unlock()

	ticker := time.NewTicker(config.CheckInterval)
	go func() {
		defer close(doneCh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.runAutoPromotionCheck()
			case <-stopCh:
				return
			}
		}
	}()
}

func (c *Coordinator) autoPromotionConfigSnapshot() autoPromotionConfig {
	var config autoPromotionConfig
	c.actorCall(func() {
		config = c.autoPromotion
	})
	return config
}

func (c *Coordinator) runAutoPromotionCheck() {
	config := c.autoPromotionConfigSnapshot()
	if !config.Enabled {
		return
	}
	if err := c.requireCoordinatorLease(); err != nil {
		c.recordAutoPromotionNotAuthoritative(err)
		return
	}

	c.refreshShardHealth(c.healthTimeout)
	shardConfig, err := activeShardConfig(c.controlStore)
	if err != nil {
		log.Printf("Auto shard promotion skipped: %v", err)
		return
	}
	now := time.Now().UTC()
	for _, shard := range shardConfig.Shards {
		state := c.evaluateAutoPromotionShard(shard, config, now)
		if !state.Eligible {
			continue
		}
		evaluation, err := c.autoPromotionEvaluation(shard.ID)
		if err != nil {
			c.recordAutoPromotionResult(shard.ID, autoPromotionStatusPromotionFailed, err.Error(), now, false)
			log.Printf("Auto shard promotion evaluation for shard %d failed: %v", shard.ID, err)
			return
		}
		if err := c.applyShardStandbyPromotion(evaluation); err != nil {
			c.recordAutoPromotionResult(shard.ID, autoPromotionStatusPromotionFailed, err.Error(), now, false)
			log.Printf("Auto shard promotion for shard %d failed: %v", shard.ID, err)
			return
		}
		c.recordAutoPromotionResult(shard.ID, autoPromotionStatusPromoted, evaluation.reason, now, true)
		log.Printf("Auto promoted shard %d standby to active: version=%d->%d old_active=%s new_active=%s",
			shard.ID,
			evaluation.current.Version,
			evaluation.proposed.Version,
			evaluation.shard.Active.LeaderAddr,
			evaluation.promotedShard.Active.LeaderAddr,
		)
		return
	}
}

func (c *Coordinator) evaluateAutoPromotionShard(shard ShardConfig, config autoPromotionConfig, now time.Time) autoPromotionShardState {
	var state autoPromotionShardState
	c.actorCall(func() {
		health := c.health[shard.ID]
		state = c.autoPromotionState[shard.ID]
		if health.Active.Reachable {
			state.FailureCount = 0
			state.Eligible = false
			state.Status = autoPromotionStatusActiveReachable
			state.Reason = "active shard is reachable"
			state.LastReachableActive = health.Active.Status
			state.HasLastReachableActive = true
			c.autoPromotionState[shard.ID] = state
			return
		}

		state.FailureCount++
		entry := coordinatorShardStatusFromHealth(shard, health)
		status, reason, eligible := autoPromotionReadiness(entry, state)
		state.Status = status
		state.Reason = reason
		state.Eligible = eligible
		if state.FailureCount < config.FailureThreshold {
			state.Status = autoPromotionStatusWaitingFailureThreshold
			state.Reason = fmt.Sprintf("active shard failed %d/%d health checks", state.FailureCount, config.FailureThreshold)
			state.Eligible = false
		} else if !state.LastPromotionAt.IsZero() && now.Sub(state.LastPromotionAt) < config.Cooldown {
			state.Status = autoPromotionStatusCooldown
			state.Reason = fmt.Sprintf("last promotion was %s ago; cooldown is %s", now.Sub(state.LastPromotionAt).Round(time.Second), config.Cooldown)
			state.Eligible = false
		}
		c.autoPromotionState[shard.ID] = state
	})
	return state
}

func coordinatorShardStatusFromHealth(shard ShardConfig, health shardHealthSnapshot) db.CoordinatorShardStatus {
	entry := db.CoordinatorShardStatus{
		ID:                 shard.ID,
		StartRow:           shard.StartRow,
		EndRow:             shard.EndRow,
		ActiveLeaderAddr:   shard.Active.LeaderAddr,
		ActiveFollowerAddr: shard.Active.FollowerAddr,
		ActiveReachable:    health.Active.Reachable,
		ActiveStatus:       health.Active.Status,
		ActiveStatusError:  health.Active.Error,
		StandbyReachable:   health.Standby.Reachable,
		StandbyStatus:      health.Standby.Status,
		StandbyStatusError: health.Standby.Error,
	}
	if shard.Standby != nil {
		entry.HasStandby = true
		entry.StandbyLeaderAddr = shard.Standby.LeaderAddr
		entry.StandbyFollowerAddr = shard.Standby.FollowerAddr
	}
	return entry
}

func autoPromotionReadiness(entry db.CoordinatorShardStatus, state autoPromotionShardState) (string, string, bool) {
	if !state.HasLastReachableActive {
		return autoPromotionStatusNoActiveBaseline, "no last reachable active status is available for catch-up comparison", false
	}
	if !entry.HasStandby {
		return standbyPromotionStatusMissingStandby, "shard has no configured standby pair", false
	}
	if !entry.StandbyReachable {
		reason := "standby shard status is unavailable"
		if entry.StandbyStatusError != "" {
			reason = entry.StandbyStatusError
		}
		return standbyPromotionStatusStandbyUnreachable, reason, false
	}
	if entry.StandbyStatus.ReplicaID != db.CompletedUploadReplicaStandby {
		return standbyPromotionStatusStandbyNotReplica, fmt.Sprintf("standby reports replica_id=%q", entry.StandbyStatus.ReplicaID), false
	}
	if entry.StandbyStatus.IngestionQueueDepth != 0 || entry.StandbyStatus.IngestionInflightCount != 0 {
		return standbyPromotionStatusStandbyQueueNotDrained, fmt.Sprintf("standby queue depth=%d inflight=%d", entry.StandbyStatus.IngestionQueueDepth, entry.StandbyStatus.IngestionInflightCount), false
	}
	if standbyStatusHasIngestionErrors(entry.StandbyStatus) {
		return standbyPromotionStatusStandbyErrors, standbyPromotionErrorReason(entry.StandbyStatus), false
	}
	if entry.StandbyStatus.CompletedUploadCommittedCount < state.LastReachableActive.CompletedUploadCommittedCount {
		return standbyPromotionStatusStandbyBehind, fmt.Sprintf("standby committed %d completed uploads; last reachable active committed %d", entry.StandbyStatus.CompletedUploadCommittedCount, state.LastReachableActive.CompletedUploadCommittedCount), false
	}
	return autoPromotionStatusEligible, "active shard is down and standby is caught up to the last reachable active status", true
}

func (c *Coordinator) autoPromotionEvaluation(shardID int) (shardPromotionEvaluation, error) {
	current, err := activeShardConfig(c.controlStore)
	if err != nil {
		return shardPromotionEvaluation{}, err
	}
	for i, shard := range current.Shards {
		if shard.ID == shardID {
			return promotedShardEvaluation(current, i, autoPromotionStatusPromoted, "automatic promotion after consecutive active health failures")
		}
	}
	return shardPromotionEvaluation{}, fmt.Errorf("active shard config missing shard %d", shardID)
}

func (c *Coordinator) recordAutoPromotionNotAuthoritative(err error) {
	shardConfig := shardConfigForStatus(c.controlStore, c.availableShards)
	c.actorCall(func() {
		for _, shard := range shardConfig.Shards {
			state := c.autoPromotionState[shard.ID]
			state.Eligible = false
			state.Status = autoPromotionStatusNotAuthoritative
			state.Reason = fmt.Sprintf("coordinator lease unavailable: %v", err)
			c.autoPromotionState[shard.ID] = state
		}
	})
}

func (c *Coordinator) recordAutoPromotionResult(shardID int, status string, reason string, now time.Time, promoted bool) {
	c.actorCall(func() {
		state := c.autoPromotionState[shardID]
		state.Eligible = false
		state.Status = status
		state.Reason = reason
		if promoted {
			state.FailureCount = 0
			state.LastPromotionAt = now
		}
		c.autoPromotionState[shardID] = state
	})
}

func populateAutoPromotionStatus(entry *db.CoordinatorShardStatus, state autoPromotionShardState) {
	if state.Status == "" {
		entry.AutoPromotionStatus = autoPromotionStatusDisabled
		entry.AutoPromotionReason = "automatic shard standby promotion is disabled or has not evaluated this shard"
		return
	}
	entry.AutoPromotionFailureCount = state.FailureCount
	entry.AutoPromotionEligible = state.Eligible
	entry.AutoPromotionStatus = state.Status
	entry.AutoPromotionReason = state.Reason
}
