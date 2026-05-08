package main

import (
	"errors"
	"fmt"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	scalingApplyStatusMissing              = "missing"
	scalingApplyStatusNotApplicable        = "not_applicable"
	scalingApplyStatusBlockedActiveEpoch   = "blocked_active_epoch"
	scalingApplyStatusBlockedMissingShards = "blocked_missing_shards"
	scalingApplyStatusApplicable           = "applicable"
)

type scalingApplyEvaluation struct {
	status         string
	reason         string
	recommendation ScalingRecommendationRecord
	current        ShardConfigRecord
	proposed       ShardConfigRecord
	applicable     bool
}

func evaluateScalingApply(controlStore ControlStore, inventory []ShardConfig) (scalingApplyEvaluation, error) {
	current, ok, err := controlStore.GetShardConfig()
	if err != nil {
		return scalingApplyEvaluation{}, err
	}
	if !ok {
		return scalingApplyEvaluation{
			status: scalingApplyStatusNotApplicable,
			reason: "current shard config is missing",
		}, nil
	}
	if err := validateShardConfigRecord(current); err != nil {
		return scalingApplyEvaluation{}, err
	}

	if epoch, ok := controlStore.CurrentEpoch(); ok {
		accepting, _ := controlStore.Accepting(epoch.ID)
		if epoch.State == db.EpochStateActive || accepting {
			return scalingApplyEvaluation{
				status:  scalingApplyStatusBlockedActiveEpoch,
				reason:  fmt.Sprintf("epoch %d is active or accepting", epoch.ID),
				current: current,
			}, nil
		}
	}

	latest, ok, err := controlStore.GetLatestScalingRecommendation()
	if err != nil {
		return scalingApplyEvaluation{}, err
	}
	if !ok {
		return scalingApplyEvaluation{
			status:  scalingApplyStatusMissing,
			reason:  "latest scaling recommendation is missing",
			current: current,
		}, nil
	}
	if latest.ShardConfigVersion != current.Version {
		return scalingApplyEvaluation{
			status:         scalingApplyStatusNotApplicable,
			reason:         fmt.Sprintf("latest recommendation targets shard config version %d, current version is %d", latest.ShardConfigVersion, current.Version),
			recommendation: latest,
			current:        current,
		}, nil
	}
	if latest.Action == scalingActionKeep {
		return scalingApplyEvaluation{
			status:         scalingApplyStatusNotApplicable,
			reason:         "latest recommendation action is keep",
			recommendation: latest,
			current:        current,
		}, nil
	}
	if latest.Action != scalingActionGrow && latest.Action != scalingActionShrink {
		return scalingApplyEvaluation{}, fmt.Errorf("unknown scaling recommendation action %q", latest.Action)
	}

	proposed, err := buildShardConfigFromInventory(inventory, latest.RecommendedShardCount, current.Version+1)
	if err != nil {
		return scalingApplyEvaluation{
			status:         scalingApplyStatusBlockedMissingShards,
			reason:         err.Error(),
			recommendation: latest,
			current:        current,
		}, nil
	}
	if proposed.ShardCount == current.ShardCount {
		return scalingApplyEvaluation{
			status:         scalingApplyStatusNotApplicable,
			reason:         "recommended shard count matches current shard count",
			recommendation: latest,
			current:        current,
			proposed:       proposed,
		}, nil
	}

	return scalingApplyEvaluation{
		status:         scalingApplyStatusApplicable,
		reason:         "latest recommendation can be applied",
		recommendation: latest,
		current:        current,
		proposed:       proposed,
		applicable:     true,
	}, nil
}

func requireScalingApplyApplicable(evaluation scalingApplyEvaluation) error {
	if evaluation.applicable {
		return nil
	}
	if evaluation.status == "" {
		return errors.New("scaling recommendation is not applicable")
	}
	return fmt.Errorf("%s: %s", evaluation.status, evaluation.reason)
}
