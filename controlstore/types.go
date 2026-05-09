package controlstore

import (
	"context"
	"errors"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

var (
	ErrLeaseHeld            = errors.New("coordinator lease is held")
	ErrLeaseNotHeld         = errors.New("coordinator lease is not held")
	ErrStaleFence           = errors.New("stale coordinator fencing token")
	ErrEpochMismatch        = errors.New("epoch mismatch")
	ErrEpochCycleTransition = errors.New("invalid epoch cycle transition")
	ErrSessionExists        = errors.New("coordinator session already exists")
	ErrSessionMissing       = errors.New("coordinator session is missing")
)

var (
	errLeaseHeld            = ErrLeaseHeld
	errLeaseNotHeld         = ErrLeaseNotHeld
	errStaleFence           = ErrStaleFence
	errEpochMismatch        = ErrEpochMismatch
	errEpochCycleTransition = ErrEpochCycleTransition
	errSessionExists        = ErrSessionExists
	errSessionMissing       = ErrSessionMissing
)

const (
	EpochCycleStateIdle                = "idle"
	EpochCycleStateActive              = "active"
	EpochCycleStateCompleted           = "completed"
	EpochCycleStateRecommendationReady = "recommendation_ready"
	EpochCycleStateScalingInProgress   = "scaling_in_progress"
	EpochCycleStateScalingApplied      = "scaling_applied"
	EpochCycleStateScalingSkipped      = "scaling_skipped"
	EpochCycleStateReadyForNextEpoch   = "ready_for_next_epoch"
	EpochCycleStateFailed              = "failed"
)

type CoordinatorLease struct {
	Holder       string
	FencingToken int64
	ExpiresAt    time.Time
}

type ControlStore interface {
	AcquireLease(now time.Time, holder string, ttl time.Duration) (CoordinatorLease, error)
	RenewLease(now time.Time, holder string, fencingToken int64, ttl time.Duration) (CoordinatorLease, error)
	CurrentLease(now time.Time) (CoordinatorLease, bool)
	StartEpoch(epoch db.EpochMeta, shardConfigVersion int64) error
	CompleteEpoch(epochID int64) (db.EpochMeta, error)
	CurrentEpoch() (db.EpochMeta, bool)
	SetAccepting(epochID int64, accepting bool) error
	Accepting(epochID int64) (bool, error)
	GetShardConfig() (ShardConfigRecord, bool, error)
	PutShardConfig(config ShardConfigRecord) error
	GetEpochShardConfig(epochID int64) (ShardConfigRecord, bool, error)
	PutEpochShardConfig(epochID int64, config ShardConfigRecord) error
	PutScalingRecommendation(record ScalingRecommendationRecord) error
	GetLatestScalingRecommendation() (ScalingRecommendationRecord, bool, error)
	GetEpochScalingRecommendation(epochID int64) (ScalingRecommendationRecord, bool, error)
	GetEpochCycle() (EpochCycleRecord, bool, error)
	PutEpochCycleTransition(from string, to string, update EpochCycleRecord) error
}

type ShardConfigRecord struct {
	Key               string
	Version           int64
	ShardCount        int
	RowsPerShard      int
	GlobalTableHeight int
	Shards            []ShardConfig
}

type ShardConfig struct {
	ID       int
	StartRow int
	EndRow   int
	Active   PairConfig
	Standby  *PairConfig
}

type PairConfig struct {
	LeaderAddr   string
	FollowerAddr string
}

type ScalingRecommendationRecord struct {
	Key                       string
	EpochID                   int64
	AcceptedRequestCount      int64
	DurationSeconds           int64
	CurrentShardCount         int
	RecommendedShardCount     int
	TargetRowsPerShard        int
	RequestDensity            float64
	Action                    string
	Reason                    string
	ProposedGlobalTableHeight int
	ShardConfigVersion        int64
	CreatedAt                 time.Time
}

type EpochCycleRecord struct {
	State                        string
	EpochID                      int64
	ShardConfigVersion           int64
	ScalingRecommendationEpochID int64
	Reason                       string
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
}

type IngestionMessage struct {
	ID         string
	EpochID    int64
	ShardID    int
	GlobalUUID int64
	LocalUUID  int64
	HashKey    [32]byte
	Challenge  [16]byte
	GlobalRow  int
	LocalRow   int
	Args1      db.UploadArgs1
	Args2      db.UploadArgs2
	Args3      db.UploadArgs3
	EnqueuedAt time.Time
	Attempts   int
}

type QueuedIngestionMessage struct {
	Message       IngestionMessage
	ReceiptHandle string
}

type IngestionQueue interface {
	Enqueue(ctx context.Context, message IngestionMessage) (string, error)
	Receive(ctx context.Context, maxMessages int) ([]QueuedIngestionMessage, error)
	Ack(ctx context.Context, receiptHandle string) error
}

type SessionRecord struct {
	EpochID       int64
	ShardID       int
	GlobalUUID    int64
	LocalUUID     int64
	HashKey       [32]byte
	GlobalRow     int
	LocalRow      int
	ShardStartRow int
	CreatedAt     time.Time
}

type SessionStore interface {
	PutSession(ctx context.Context, session SessionRecord) error
	GetSession(ctx context.Context, globalUUID int64) (SessionRecord, error)
	DeleteSession(ctx context.Context, globalUUID int64) error
}
