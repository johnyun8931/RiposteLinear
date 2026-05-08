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

	latest, ok, err := a.control.GetLatestScalingRecommendation()
	if err != nil {
		a.logDecision(decisionReadFailed, "read latest scaling recommendation failed: %v", err)
		return decisionReadFailed, err
	}
	if !ok {
		a.logDecision(decisionMissing, "latest scaling recommendation is missing")
		return decisionMissing, nil
	}
	if a.minRecommendationEpoch > 0 && latest.EpochID < a.minRecommendationEpoch {
		a.logDecision(decisionStale, "recommendation_epoch=%d min_recommendation_epoch=%d", latest.EpochID, a.minRecommendationEpoch)
		return decisionStale, nil
	}
	if latest.Action == scalingActionKeep {
		a.logDecision(decisionNotApplicable, "recommendation_epoch=%d action=keep", latest.EpochID)
		return decisionNotApplicable, nil
	}

	current, ok, err := a.control.GetShardConfig()
	if err != nil {
		a.logDecision(decisionReadFailed, "read current shard config failed: %v", err)
		return decisionReadFailed, err
	}
	if !ok {
		a.logDecision(decisionMissing, "current shard config is missing recommendation_epoch=%d", latest.EpochID)
		return decisionMissing, nil
	}
	if latest.ShardConfigVersion != current.Version {
		a.logDecision(decisionStale, "recommendation_epoch=%d recommendation_version=%d current_version=%d", latest.EpochID, latest.ShardConfigVersion, current.Version)
		return decisionStale, nil
	}

	epoch, ok := a.control.CurrentEpoch()
	if ok {
		accepting, _ := a.control.Accepting(epoch.ID)
		if epoch.State == db.EpochStateActive || accepting {
			a.logDecision(decisionBlockedActive, "epoch=%d state=%s accepting=%t", epoch.ID, epoch.State.String(), accepting)
			return decisionBlockedActive, nil
		}
	}

	dryRun, err := a.coordinator.ApplyScalingRecommendation(ctx, true)
	if err != nil {
		a.logDecision(decisionDryRunFailed, "recommendation_epoch=%d error=%v", latest.EpochID, err)
		return decisionDryRunFailed, err
	}
	if dryRun.Status != scalingApplyStatusOK {
		a.logDecision(decisionNotApplicable, "recommendation_epoch=%d status=%s reason=%q", latest.EpochID, dryRun.Status, dryRun.Reason)
		return decisionNotApplicable, nil
	}
	a.logDecision(decisionDryRunApplicable, "recommendation_epoch=%d version=%d->%d shards=%d->%d", dryRun.RecommendationEpochID, dryRun.PreviousVersion, dryRun.NewVersion, dryRun.PreviousShardCount, dryRun.NewShardCount)

	if !a.apply {
		return decisionDryRunApplicable, nil
	}

	applyReply, err := a.coordinator.ApplyScalingRecommendation(ctx, false)
	if err != nil {
		a.logDecision(decisionApplyFailed, "recommendation_epoch=%d error=%v", latest.EpochID, err)
		return decisionApplyFailed, nil
	}
	a.logDecision(decisionApplied, "recommendation_epoch=%d version=%d->%d shards=%d->%d", applyReply.RecommendationEpochID, applyReply.PreviousVersion, applyReply.NewVersion, applyReply.PreviousShardCount, applyReply.NewShardCount)
	return decisionApplied, nil
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
