package main

import (
	"strings"
	"testing"

	"bitbucket.org/henrycg/riposte/db"
)

func testScalingConfig() ScalingPolicyConfig {
	return ScalingPolicyConfig{
		MinShards:                 1,
		MaxShards:                 8,
		TargetRowsPerShard:        db.TABLE_HEIGHT,
		ScaleUpDensityThreshold:   defaultScaleUpDensityThreshold,
		ScaleDownDensityThreshold: defaultScaleDownDensityThreshold,
		MaxShardMultiplier:        defaultMaxShardMultiplier,
	}
}

func TestComputeNextDatasetScaleGrowsOnHighDensity(t *testing.T) {
	config := testScalingConfig()
	config.MaxShards = 4
	rec := ComputeNextDatasetScale(EpochScalingMetrics{
		EpochID:              1,
		CurrentShardCount:    2,
		AcceptedRequestCount: 2500,
		DurationSeconds:      60,
	}, config)

	if rec.Action != scalingActionGrow || rec.RecommendedShardCount != 4 {
		t.Fatalf("expected grow to 4 shards, got %+v", rec)
	}
	if rec.RequestDensity < 4.0 {
		t.Fatalf("expected high request density, got %.2f", rec.RequestDensity)
	}
}

func TestComputeNextDatasetScaleShrinksOnLowDensity(t *testing.T) {
	rec := ComputeNextDatasetScale(EpochScalingMetrics{
		EpochID:              1,
		CurrentShardCount:    4,
		AcceptedRequestCount: 700,
		DurationSeconds:      60,
	}, testScalingConfig())

	if rec.Action != scalingActionShrink || rec.RecommendedShardCount != 2 {
		t.Fatalf("expected shrink to 2 shards, got %+v", rec)
	}
	if rec.RequestDensity > 1.0 {
		t.Fatalf("expected low request density, got %.2f", rec.RequestDensity)
	}
}

func TestComputeNextDatasetScaleKeepsWithinThresholds(t *testing.T) {
	rec := ComputeNextDatasetScale(EpochScalingMetrics{
		EpochID:              1,
		CurrentShardCount:    2,
		AcceptedRequestCount: 1200,
		DurationSeconds:      60,
	}, testScalingConfig())

	if rec.Action != scalingActionKeep || rec.RecommendedShardCount != 2 {
		t.Fatalf("expected keep at 2 shards, got %+v", rec)
	}
	if rec.RequestDensity < 2.3 || rec.RequestDensity > 2.4 {
		t.Fatalf("unexpected density %.2f", rec.RequestDensity)
	}
}

func TestComputeNextDatasetScaleUsesShardRowsForDensity(t *testing.T) {
	rec := ComputeNextDatasetScale(EpochScalingMetrics{
		EpochID:              1,
		CurrentShardCount:    2,
		AcceptedRequestCount: 1024,
		DurationSeconds:      60,
	}, testScalingConfig())

	if rec.RequestDensity != 2 {
		t.Fatalf("expected density 1024 / (2 * 256) = 2, got %.2f", rec.RequestDensity)
	}
}

func TestComputeNextDatasetScaleMaxShardCapPreventsGrowth(t *testing.T) {
	config := testScalingConfig()
	config.MaxShards = 4
	rec := ComputeNextDatasetScale(EpochScalingMetrics{
		EpochID:              1,
		CurrentShardCount:    4,
		AcceptedRequestCount: 5000,
		DurationSeconds:      60,
	}, config)

	if rec.Action != scalingActionKeep || rec.RecommendedShardCount != 4 {
		t.Fatalf("expected keep at max shard cap, got %+v", rec)
	}
	if !strings.Contains(rec.Reason, "max shard cap") {
		t.Fatalf("expected cap reason, got %q", rec.Reason)
	}
}

func TestComputeNextDatasetScaleInvalidInputsKeep(t *testing.T) {
	tests := []struct {
		name    string
		metrics EpochScalingMetrics
		config  ScalingPolicyConfig
	}{
		{
			name:    "current shard count",
			metrics: EpochScalingMetrics{CurrentShardCount: 0, AcceptedRequestCount: 100, DurationSeconds: 60},
			config:  testScalingConfig(),
		},
		{
			name:    "duration",
			metrics: EpochScalingMetrics{CurrentShardCount: 1, AcceptedRequestCount: 100, DurationSeconds: 0},
			config:  testScalingConfig(),
		},
		{
			name:    "target rows",
			metrics: EpochScalingMetrics{CurrentShardCount: 1, AcceptedRequestCount: 100, DurationSeconds: 60},
			config:  ScalingPolicyConfig{MinShards: 1, MaxShards: 1, TargetRowsPerShard: 0, ScaleUpDensityThreshold: 4, ScaleDownDensityThreshold: 1, MaxShardMultiplier: 2},
		},
		{
			name:    "thresholds",
			metrics: EpochScalingMetrics{CurrentShardCount: 1, AcceptedRequestCount: 100, DurationSeconds: 60},
			config:  ScalingPolicyConfig{MinShards: 1, MaxShards: 1, TargetRowsPerShard: db.TABLE_HEIGHT, ScaleUpDensityThreshold: 1, ScaleDownDensityThreshold: 1, MaxShardMultiplier: 2},
		},
		{
			name:    "shard caps",
			metrics: EpochScalingMetrics{CurrentShardCount: 3, AcceptedRequestCount: 100, DurationSeconds: 60},
			config:  ScalingPolicyConfig{MinShards: 4, MaxShards: 2, TargetRowsPerShard: db.TABLE_HEIGHT, ScaleUpDensityThreshold: 4, ScaleDownDensityThreshold: 1, MaxShardMultiplier: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := ComputeNextDatasetScale(tt.metrics, tt.config)
			if rec.Action != scalingActionKeep || rec.RecommendedShardCount != tt.metrics.CurrentShardCount {
				t.Fatalf("expected invalid input to keep current shard count, got %+v", rec)
			}
			if rec.Reason == "" {
				t.Fatalf("expected invalid input reason")
			}
		})
	}
}
