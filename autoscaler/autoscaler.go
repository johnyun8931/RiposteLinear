package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"bitbucket.org/henrycg/riposte/controlstore"
	"bitbucket.org/henrycg/riposte/db"
)

const (
	decisionMissing          = "missing"
	decisionStale            = "stale"
	decisionBlockedActive    = "blocked_active_epoch"
	decisionDryRunApplicable = "dry_run_applicable"
	decisionApplied          = "applied"
	decisionApplyFailed      = "apply_failed"
	decisionDryRunFailed     = "dry_run_failed"
	decisionNotApplicable    = "not_applicable"
	decisionReadFailed       = "read_failed"
	decisionBlockedCycle     = "blocked_cycle_state"
	scalingActionKeep        = "keep"
	scalingApplyStatusOK     = "applicable"
	defaultEvaluationTimeout = 10 * time.Second
)

type coordinatorAdmin interface {
	ApplyScalingRecommendation(ctx context.Context, dryRun bool) (db.ApplyScalingRecommendationReply, error)
}

type autoscaler struct {
	control                controlstore.ControlStore
	coordinator            coordinatorAdmin
	apply                  bool
	minRecommendationEpoch int64
	logger                 *log.Logger
}

func (a *autoscaler) evaluateOnce(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultEvaluationTimeout)
	defer cancel()

	cycle, ok, err := a.control.GetEpochCycle()
	if err != nil {
		a.logDecision(decisionReadFailed, "read epoch cycle failed: %v", err)
		return decisionReadFailed, err
	}
	if !ok || cycle.State != controlstore.EpochCycleStateRecommendationReady {
		state := controlstore.EpochCycleStateIdle
		if ok {
			state = cycle.State
		}
		a.logDecision(decisionBlockedCycle, "epoch_cycle_state=%s", state)
		return decisionBlockedCycle, nil
	}

	latest, ok, err := a.control.GetLatestScalingRecommendation()
	if err != nil {
		a.logDecision(decisionReadFailed, "read latest scaling recommendation failed: %v", err)
		if a.apply {
			a.transitionCycleFailed(cycle, err.Error())
		}
		return decisionReadFailed, err
	}
	if !ok {
		a.logDecision(decisionMissing, "latest scaling recommendation is missing")
		if a.apply {
			a.transitionCycleSkipped(cycle, "latest scaling recommendation is missing")
		}
		return decisionMissing, nil
	}
	if a.minRecommendationEpoch > 0 && latest.EpochID < a.minRecommendationEpoch {
		a.logDecision(decisionStale, "recommendation_epoch=%d min_recommendation_epoch=%d", latest.EpochID, a.minRecommendationEpoch)
		if a.apply {
			a.transitionCycleSkipped(cycle, "latest scaling recommendation is stale")
		}
		return decisionStale, nil
	}
	if latest.Action == scalingActionKeep {
		a.logDecision(decisionNotApplicable, "recommendation_epoch=%d action=keep", latest.EpochID)
		if a.apply {
			a.transitionCycleSkipped(cycle, "latest scaling recommendation action is keep")
		}
		return decisionNotApplicable, nil
	}

	current, ok, err := a.control.GetShardConfig()
	if err != nil {
		a.logDecision(decisionReadFailed, "read current shard config failed: %v", err)
		if a.apply {
			a.transitionCycleFailed(cycle, err.Error())
		}
		return decisionReadFailed, err
	}
	if !ok {
		a.logDecision(decisionMissing, "current shard config is missing recommendation_epoch=%d", latest.EpochID)
		if a.apply {
			a.transitionCycleSkipped(cycle, "current shard config is missing")
		}
		return decisionMissing, nil
	}
	if latest.ShardConfigVersion != current.Version {
		a.logDecision(decisionStale, "recommendation_epoch=%d recommendation_version=%d current_version=%d", latest.EpochID, latest.ShardConfigVersion, current.Version)
		if a.apply {
			a.transitionCycleSkipped(cycle, "latest scaling recommendation targets a stale shard config")
		}
		return decisionStale, nil
	}

	epoch, ok := a.control.CurrentEpoch()
	if ok {
		accepting, _ := a.control.Accepting(epoch.ID)
		if epoch.State == db.EpochStateActive || accepting {
			a.logDecision(decisionBlockedActive, "epoch=%d state=%s accepting=%t", epoch.ID, epoch.State.String(), accepting)
			if a.apply {
				a.transitionCycleFailed(cycle, "epoch became active before scaling apply")
			}
			return decisionBlockedActive, nil
		}
	}

	if a.apply {
		if err := a.control.PutEpochCycleTransition(controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleStateScalingInProgress, controlstore.EpochCycleRecord{
			EpochID:                      cycle.EpochID,
			ShardConfigVersion:           current.Version,
			ScalingRecommendationEpochID: latest.EpochID,
			Reason:                       "autoscaler started scaling evaluation",
		}); err != nil {
			a.logDecision(decisionApplyFailed, "recommendation_epoch=%d transition_error=%v", latest.EpochID, err)
			return decisionApplyFailed, nil
		}
	}

	dryRun, err := a.coordinator.ApplyScalingRecommendation(ctx, true)
	if err != nil {
		a.logDecision(decisionDryRunFailed, "recommendation_epoch=%d error=%v", latest.EpochID, err)
		if a.apply {
			a.transitionCycleFailedInProgress(cycle, err.Error())
		}
		return decisionDryRunFailed, err
	}
	if dryRun.Status != scalingApplyStatusOK {
		a.logDecision(decisionNotApplicable, "recommendation_epoch=%d status=%s reason=%q", latest.EpochID, dryRun.Status, dryRun.Reason)
		if a.apply {
			a.transitionCycleSkippedFromInProgress(cycle, dryRun.Reason)
		}
		return decisionNotApplicable, nil
	}
	a.logDecision(decisionDryRunApplicable, "recommendation_epoch=%d version=%d->%d shards=%d->%d", dryRun.RecommendationEpochID, dryRun.PreviousVersion, dryRun.NewVersion, dryRun.PreviousShardCount, dryRun.NewShardCount)

	if !a.apply {
		return decisionDryRunApplicable, nil
	}

	applyReply, err := a.coordinator.ApplyScalingRecommendation(ctx, false)
	if err != nil {
		a.logDecision(decisionApplyFailed, "recommendation_epoch=%d error=%v", latest.EpochID, err)
		a.transitionCycleFailedInProgress(cycle, err.Error())
		return decisionApplyFailed, nil
	}
	a.logDecision(decisionApplied, "recommendation_epoch=%d version=%d->%d shards=%d->%d", applyReply.RecommendationEpochID, applyReply.PreviousVersion, applyReply.NewVersion, applyReply.PreviousShardCount, applyReply.NewShardCount)
	return decisionApplied, nil
}

func (a *autoscaler) transitionCycleSkipped(cycle controlstore.EpochCycleRecord, reason string) {
	if err := a.control.PutEpochCycleTransition(controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleStateScalingInProgress, controlstore.EpochCycleRecord{
		EpochID:                      cycle.EpochID,
		ShardConfigVersion:           cycle.ShardConfigVersion,
		ScalingRecommendationEpochID: cycle.ScalingRecommendationEpochID,
		Reason:                       "autoscaler started scaling evaluation",
	}); err != nil {
		a.logDecision(decisionApplyFailed, "epoch_cycle_transition_error=%v", err)
		return
	}
	a.transitionCycleSkippedFromInProgress(cycle, reason)
}

func (a *autoscaler) transitionCycleSkippedFromInProgress(cycle controlstore.EpochCycleRecord, reason string) {
	if err := a.control.PutEpochCycleTransition(controlstore.EpochCycleStateScalingInProgress, controlstore.EpochCycleStateScalingSkipped, controlstore.EpochCycleRecord{
		EpochID:                      cycle.EpochID,
		ShardConfigVersion:           cycle.ShardConfigVersion,
		ScalingRecommendationEpochID: cycle.ScalingRecommendationEpochID,
		Reason:                       reason,
	}); err != nil {
		a.logDecision(decisionApplyFailed, "epoch_cycle_transition_error=%v", err)
	}
}

func (a *autoscaler) transitionCycleFailed(cycle controlstore.EpochCycleRecord, reason string) {
	if err := a.control.PutEpochCycleTransition(controlstore.EpochCycleStateRecommendationReady, controlstore.EpochCycleStateFailed, controlstore.EpochCycleRecord{
		EpochID:                      cycle.EpochID,
		ShardConfigVersion:           cycle.ShardConfigVersion,
		ScalingRecommendationEpochID: cycle.ScalingRecommendationEpochID,
		Reason:                       reason,
	}); err != nil {
		a.logDecision(decisionApplyFailed, "epoch_cycle_transition_error=%v", err)
	}
}

func (a *autoscaler) transitionCycleFailedInProgress(cycle controlstore.EpochCycleRecord, reason string) {
	if err := a.control.PutEpochCycleTransition(controlstore.EpochCycleStateScalingInProgress, controlstore.EpochCycleStateFailed, controlstore.EpochCycleRecord{
		EpochID:                      cycle.EpochID,
		ShardConfigVersion:           cycle.ShardConfigVersion,
		ScalingRecommendationEpochID: cycle.ScalingRecommendationEpochID,
		Reason:                       reason,
	}); err != nil {
		a.logDecision(decisionApplyFailed, "epoch_cycle_transition_error=%v", err)
	}
}

func (a *autoscaler) run(ctx context.Context, interval time.Duration, once bool) error {
	if interval <= 0 {
		return errors.New("interval must be positive")
	}
	for {
		_, err := a.evaluateOnce(ctx)
		if once {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (a *autoscaler) logDecision(decision string, format string, args ...any) {
	if a.logger == nil {
		return
	}
	a.logger.Printf("decision=%s %s", decision, fmt.Sprintf(format, args...))
}
