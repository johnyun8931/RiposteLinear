package main

import (
	"fmt"
	"math"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	defaultScaleUpDensityThreshold   = 4.0
	defaultScaleDownDensityThreshold = 1.0
	defaultMaxShardMultiplier        = 2

	scalingActionGrow   = "grow"
	scalingActionShrink = "shrink"
	scalingActionKeep   = "keep"
)

type EpochScalingMetrics struct {
	EpochID              int64
	CurrentShardCount    int
	AcceptedRequestCount int64
	DurationSeconds      int64
}

type ScalingPolicyConfig struct {
	MinShards                 int
	MaxShards                 int
	TargetRowsPerShard        int
	ScaleUpDensityThreshold   float64
	ScaleDownDensityThreshold float64
	MaxShardMultiplier        int
}

type ScalingRecommendation struct {
	CurrentShardCount     int
	RecommendedShardCount int
	TargetRowsPerShard    int
	RequestDensity        float64
	Action                string
	Reason                string
}

func defaultScalingPolicyConfig(currentShardCount int) ScalingPolicyConfig {
	return ScalingPolicyConfig{
		MinShards:                 currentShardCount,
		MaxShards:                 currentShardCount,
		TargetRowsPerShard:        db.TABLE_HEIGHT,
		ScaleUpDensityThreshold:   defaultScaleUpDensityThreshold,
		ScaleDownDensityThreshold: defaultScaleDownDensityThreshold,
		MaxShardMultiplier:        defaultMaxShardMultiplier,
	}
}

func ComputeNextDatasetScale(metrics EpochScalingMetrics, config ScalingPolicyConfig) ScalingRecommendation {
	rec := ScalingRecommendation{
		CurrentShardCount:     metrics.CurrentShardCount,
		RecommendedShardCount: metrics.CurrentShardCount,
		TargetRowsPerShard:    config.TargetRowsPerShard,
		Action:                scalingActionKeep,
	}

	if metrics.CurrentShardCount <= 0 {
		rec.Reason = "invalid current shard count"
		return rec
	}
	if metrics.AcceptedRequestCount < 0 {
		rec.Reason = "invalid accepted request count"
		return rec
	}
	if metrics.DurationSeconds <= 0 {
		rec.Reason = "invalid epoch duration"
		return rec
	}
	if config.MinShards <= 0 || config.MaxShards <= 0 || config.MinShards > config.MaxShards || metrics.CurrentShardCount > config.MaxShards {
		rec.Reason = "invalid shard bounds"
		return rec
	}
	if config.TargetRowsPerShard <= 0 {
		rec.Reason = "invalid target rows per shard"
		return rec
	}
	if config.ScaleDownDensityThreshold < 0 || config.ScaleUpDensityThreshold <= config.ScaleDownDensityThreshold {
		rec.Reason = "invalid density thresholds"
		return rec
	}
	if config.MaxShardMultiplier < 1 {
		rec.Reason = "invalid max shard multiplier"
		return rec
	}

	currentLogicalRows := metrics.CurrentShardCount * config.TargetRowsPerShard
	rec.RequestDensity = float64(metrics.AcceptedRequestCount) / float64(currentLogicalRows)

	if rec.RequestDensity >= config.ScaleUpDensityThreshold {
		target := metrics.CurrentShardCount * config.MaxShardMultiplier
		if target > config.MaxShards {
			target = config.MaxShards
		}
		if target == metrics.CurrentShardCount {
			rec.Reason = fmt.Sprintf("scale-up density %.2f reached, but max shard cap %d prevents growth", rec.RequestDensity, config.MaxShards)
			return rec
		}
		rec.RecommendedShardCount = target
		rec.Action = scalingActionGrow
		rec.Reason = fmt.Sprintf("request density %.2f reached scale-up threshold %.2f", rec.RequestDensity, config.ScaleUpDensityThreshold)
		return rec
	}

	if rec.RequestDensity <= config.ScaleDownDensityThreshold {
		target := int(math.Ceil(float64(metrics.CurrentShardCount) / float64(config.MaxShardMultiplier)))
		if target < config.MinShards {
			target = config.MinShards
		}
		if target == metrics.CurrentShardCount {
			rec.Reason = fmt.Sprintf("scale-down density %.2f reached, but min shard bound %d prevents shrink", rec.RequestDensity, config.MinShards)
			return rec
		}
		rec.RecommendedShardCount = target
		rec.Action = scalingActionShrink
		rec.Reason = fmt.Sprintf("request density %.2f reached scale-down threshold %.2f", rec.RequestDensity, config.ScaleDownDensityThreshold)
		return rec
	}

	rec.Reason = fmt.Sprintf("request density %.2f is within scaling thresholds", rec.RequestDensity)
	return rec
}

func defaultCoordinatorScalingRecommendation(currentShardCount int) ScalingRecommendation {
	config := defaultScalingPolicyConfig(currentShardCount)
	metrics := EpochScalingMetrics{
		CurrentShardCount:    currentShardCount,
		AcceptedRequestCount: int64(currentShardCount * config.TargetRowsPerShard * 2),
		DurationSeconds:      1,
	}
	return ComputeNextDatasetScale(metrics, config)
}
