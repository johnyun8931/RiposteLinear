package main

import (
	"context"
	"errors"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

var (
	errLeaseHeld      = errors.New("coordinator lease is held")
	errLeaseNotHeld   = errors.New("coordinator lease is not held")
	errStaleFence     = errors.New("stale coordinator fencing token")
	errEpochMismatch  = errors.New("epoch mismatch")
	errSessionExists  = errors.New("coordinator session already exists")
	errSessionMissing = errors.New("coordinator session is missing")
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
	ShardConfigVersion() int64
	SetShardConfigVersion(version int64) error
}

type IngestionMessage struct {
	ID         string
	EpochID    int64
	ShardID    int
	GlobalUUID int64
	LocalUUID  int64
	HashKey    [32]byte
	RouteRow   int
	EnqueuedAt time.Time
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
