package main

import (
	"fmt"
	"log"
	"time"

	"bitbucket.org/henrycg/riposte/controlstore"
)

func epochCycleState(controlStore ControlStore) string {
	cycle, ok, err := controlStore.GetEpochCycle()
	if err != nil || !ok || cycle.State == "" {
		return controlstore.EpochCycleStateIdle
	}
	return cycle.State
}

func requireEpochCycleAllowsStart(controlStore ControlStore) error {
	state := epochCycleState(controlStore)
	switch state {
	case controlstore.EpochCycleStateIdle,
		controlstore.EpochCycleStateScalingApplied,
		controlstore.EpochCycleStateScalingSkipped,
		controlstore.EpochCycleStateReadyForNextEpoch:
		return nil
	}
	return fmt.Errorf("epoch cycle state %s does not allow starting an epoch", state)
}

func putEpochCycleTransition(controlStore ControlStore, from string, to string, update EpochCycleRecord) error {
	if update.UpdatedAt.IsZero() {
		update.UpdatedAt = time.Now().UTC()
	}
	return controlStore.PutEpochCycleTransition(from, to, update)
}

func transitionEpochCycleStarted(controlStore ControlStore, epochID int64, shardConfigVersion int64) error {
	return putEpochCycleTransition(controlStore, epochCycleState(controlStore), controlstore.EpochCycleStateActive, EpochCycleRecord{
		EpochID:            epochID,
		ShardConfigVersion: shardConfigVersion,
		Reason:             "epoch started",
	})
}

func transitionEpochCycleRecommendationReady(controlStore ControlStore, epochID int64, shardConfigVersion int64) error {
	return putEpochCycleTransition(controlStore, controlstore.EpochCycleStateActive, controlstore.EpochCycleStateRecommendationReady, EpochCycleRecord{
		EpochID:                      epochID,
		ShardConfigVersion:           shardConfigVersion,
		ScalingRecommendationEpochID: epochID,
		Reason:                       "scaling recommendation ready",
	})
}

func transitionEpochCycleFailed(controlStore ControlStore, from string, epochID int64, shardConfigVersion int64, reason string) {
	if err := putEpochCycleTransition(controlStore, from, controlstore.EpochCycleStateFailed, EpochCycleRecord{
		EpochID:            epochID,
		ShardConfigVersion: shardConfigVersion,
		Reason:             reason,
	}); err != nil {
		logEpochCycleFailure("mark failed", err)
	}
}

func logEpochCycleFailure(action string, err error) {
	if err != nil {
		log.Printf("Epoch cycle %s failed: %v", action, err)
	}
}
