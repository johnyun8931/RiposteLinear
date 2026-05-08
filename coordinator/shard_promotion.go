package main

import (
	"errors"
	"fmt"
	"log"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	shardPromotionStatusPromoted      = "promoted"
	shardPromotionStatusActiveHealthy = "active_healthy"
)

type shardPromotionEvaluation struct {
	current       ShardConfigRecord
	proposed      ShardConfigRecord
	shard         ShardConfig
	promotedShard ShardConfig
	status        string
	reason        string
	allowed       bool
}

func (c *Coordinator) PromoteShardStandby(args *db.PromoteShardStandbyArgs, reply *db.PromoteShardStandbyReply) error {
	if args == nil {
		return errors.New("promote shard args are required")
	}
	if err := c.requireCoordinatorLease(); err != nil {
		return coordinatorWireError(errCoordinatorNotActive)
	}
	evaluation, err := c.evaluateShardStandbyPromotion(args.ShardID, args.Force)
	if err != nil {
		return err
	}
	if !evaluation.allowed {
		populatePromoteShardStandbyReply(reply, evaluation, false)
		return fmt.Errorf("%s: %s", evaluation.status, evaluation.reason)
	}

	newClient, err := c.shardLeaderDialer(evaluation.promotedShard.Active.LeaderAddr)
	if err != nil {
		return fmt.Errorf("connect promoted shard %d leader %s: %w", evaluation.promotedShard.ID, evaluation.promotedShard.Active.LeaderAddr, err)
	}
	if err := c.controlStore.PutShardConfig(evaluation.proposed); err != nil {
		if closer, ok := newClient.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return err
	}
	c.replaceActiveShardClientWith(evaluation.promotedShard, newClient)
	populatePromoteShardStandbyReply(reply, evaluation, true)
	return nil
}

func (c *Coordinator) evaluateShardStandbyPromotion(shardID int, force bool) (shardPromotionEvaluation, error) {
	if shardID < 0 {
		return shardPromotionEvaluation{}, errors.New("promote shard id must be non-negative")
	}
	current, err := activeShardConfig(c.controlStore)
	if err != nil {
		return shardPromotionEvaluation{}, err
	}
	index := -1
	for i, shard := range current.Shards {
		if shard.ID == shardID {
			index = i
			break
		}
	}
	if index < 0 {
		return shardPromotionEvaluation{}, fmt.Errorf("active shard config missing shard %d", shardID)
	}

	shard := current.Shards[index]
	evaluation := shardPromotionEvaluation{
		current: current,
		shard:   shard,
		status:  standbyPromotionStatusMissingStandby,
		reason:  "shard has no configured standby pair",
	}
	if shard.Standby == nil {
		return evaluation, nil
	}

	c.refreshShardHealth(c.healthTimeout)
	entry := c.currentShardStatusEntry(shard)
	status, reason, promotable := standbyPromotionReadiness(entry)
	evaluation.status = status
	evaluation.reason = reason
	if entry.ActiveReachable && !force {
		evaluation.status = shardPromotionStatusActiveHealthy
		evaluation.reason = "active shard is still reachable; use -force-promote-shard for planned promotion"
		return evaluation, nil
	}
	if !entry.ActiveReachable && !force {
		evaluation.status = standbyPromotionStatusUnknownActiveUnreachable
		evaluation.reason = "active shard status is unavailable; use -force-promote-shard to drop partial sessions and promote standby"
		return evaluation, nil
	}
	if !promotable {
		if force && !entry.ActiveReachable {
			status, reason, promotable = forcedStandbyPromotionReadiness(entry)
			evaluation.status = status
			evaluation.reason = reason
		}
		if !promotable {
			return evaluation, nil
		}
	}

	promoted := shard
	oldActive := shard.Active
	promoted.Active = *shard.Standby
	promoted.Standby = &oldActive
	proposed := current
	proposed.Version = current.Version + 1
	proposed.Shards = append([]ShardConfig(nil), current.Shards...)
	proposed.Shards[index] = promoted
	if err := validateShardConfigRecord(proposed); err != nil {
		return shardPromotionEvaluation{}, err
	}

	evaluation.proposed = proposed
	evaluation.promotedShard = promoted
	evaluation.status = shardPromotionStatusPromoted
	evaluation.reason = "standby promoted to active shard pair"
	evaluation.allowed = true
	return evaluation, nil
}

func forcedStandbyPromotionReadiness(entry db.CoordinatorShardStatus) (string, string, bool) {
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
	return standbyPromotionStatusPromotable, "standby is reachable, drained, and error-free; active status is unavailable and force was requested", true
}

func (c *Coordinator) currentShardStatusEntry(shard ShardConfig) db.CoordinatorShardStatus {
	var health shardHealthSnapshot
	c.actorCall(func() {
		health = c.health[shard.ID]
	})
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

func (c *Coordinator) replaceActiveShardClientWith(shard ShardConfig, client shardClient) {
	old := c.clients[shard.ID]
	c.clients[shard.ID] = client
	if closer, ok := old.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			log.Printf("Close old shard %d client after promotion: %v", shard.ID, err)
		}
	}
	c.actorCall(func() {
		delete(c.health, shard.ID)
	})
}

func populatePromoteShardStandbyReply(reply *db.PromoteShardStandbyReply, evaluation shardPromotionEvaluation, promoted bool) {
	reply.Promoted = promoted
	reply.ShardID = evaluation.shard.ID
	reply.PreviousVersion = evaluation.current.Version
	reply.NewVersion = evaluation.proposed.Version
	reply.OldActiveLeaderAddr = evaluation.shard.Active.LeaderAddr
	if evaluation.promotedShard.Active.LeaderAddr != "" {
		reply.NewActiveLeaderAddr = evaluation.promotedShard.Active.LeaderAddr
	} else if evaluation.shard.Standby != nil {
		reply.NewActiveLeaderAddr = evaluation.shard.Standby.LeaderAddr
	}
	reply.Status = evaluation.status
	reply.Reason = evaluation.reason
}
