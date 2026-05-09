package controlstore

import (
	"fmt"
	"time"
)

const dynamoControlEpochCyclePK = "epoch-cycle"

func normalizeEpochCycleState(state string) string {
	if state == "" {
		return EpochCycleStateIdle
	}
	return state
}

func validEpochCycleState(state string) bool {
	switch state {
	case EpochCycleStateIdle,
		EpochCycleStateActive,
		EpochCycleStateCompleted,
		EpochCycleStateRecommendationReady,
		EpochCycleStateScalingInProgress,
		EpochCycleStateScalingApplied,
		EpochCycleStateScalingSkipped,
		EpochCycleStateReadyForNextEpoch,
		EpochCycleStateFailed:
		return true
	default:
		return false
	}
}

func validEpochCycleTransition(from string, to string) bool {
	from = normalizeEpochCycleState(from)
	if !validEpochCycleState(from) || !validEpochCycleState(to) {
		return false
	}
	if from == to {
		return true
	}
	switch from {
	case EpochCycleStateIdle, EpochCycleStateScalingApplied, EpochCycleStateScalingSkipped, EpochCycleStateReadyForNextEpoch:
		return to == EpochCycleStateActive
	case EpochCycleStateActive:
		return to == EpochCycleStateCompleted || to == EpochCycleStateRecommendationReady || to == EpochCycleStateFailed
	case EpochCycleStateCompleted:
		return to == EpochCycleStateRecommendationReady || to == EpochCycleStateFailed
	case EpochCycleStateRecommendationReady:
		return to == EpochCycleStateScalingInProgress || to == EpochCycleStateFailed
	case EpochCycleStateScalingInProgress:
		return to == EpochCycleStateScalingApplied || to == EpochCycleStateScalingSkipped || to == EpochCycleStateFailed
	case EpochCycleStateFailed:
		return to == EpochCycleStateReadyForNextEpoch
	default:
		return false
	}
}

func validateEpochCycleTransition(from string, to string) error {
	if !validEpochCycleTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", errEpochCycleTransition, normalizeEpochCycleState(from), to)
	}
	return nil
}

func prepareEpochCycleRecord(from string, to string, update EpochCycleRecord, existing EpochCycleRecord, hasExisting bool) (EpochCycleRecord, error) {
	if err := validateEpochCycleTransition(from, to); err != nil {
		return EpochCycleRecord{}, err
	}
	now := time.Now().UTC()
	record := update
	record.State = to
	if record.CreatedAt.IsZero() {
		if hasExisting && !existing.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		} else {
			record.CreatedAt = now
		}
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	return record, nil
}
