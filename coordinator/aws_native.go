package main

import (
	"bitbucket.org/henrycg/riposte/controlstore"
)

var (
	errLeaseHeld            = controlstore.ErrLeaseHeld
	errLeaseNotHeld         = controlstore.ErrLeaseNotHeld
	errStaleFence           = controlstore.ErrStaleFence
	errEpochMismatch        = controlstore.ErrEpochMismatch
	errEpochCycleTransition = controlstore.ErrEpochCycleTransition
	errSessionExists        = controlstore.ErrSessionExists
	errSessionMissing       = controlstore.ErrSessionMissing
)

type CoordinatorLease = controlstore.CoordinatorLease
type ControlStore = controlstore.ControlStore
type ShardConfigRecord = controlstore.ShardConfigRecord
type ShardConfig = controlstore.ShardConfig
type PairConfig = controlstore.PairConfig
type ScalingRecommendationRecord = controlstore.ScalingRecommendationRecord
type EpochCycleRecord = controlstore.EpochCycleRecord
type IngestionMessage = controlstore.IngestionMessage
type QueuedIngestionMessage = controlstore.QueuedIngestionMessage
type IngestionQueue = controlstore.IngestionQueue
type SessionRecord = controlstore.SessionRecord
type SessionStore = controlstore.SessionStore

type memoryControlStore = controlstore.MemoryControlStore
type memorySessionStore = controlstore.MemorySessionStore
type dynamoDBControlStore = controlstore.DynamoDBStore
type dynamoDBControlClient = controlstore.DynamoDBClient

func newMemoryControlStore(shardConfigVersion int64) *memoryControlStore {
	return controlstore.NewMemoryControlStore(shardConfigVersion)
}

func newMemorySessionStore() *memorySessionStore {
	return controlstore.NewMemorySessionStore()
}

func newDynamoDBControlStore(client dynamoDBControlClient, table string) (*dynamoDBControlStore, error) {
	return controlstore.NewDynamoDBStore(client, table)
}

func dynamoDBControlStoreConfigError(name string) error {
	return controlstore.DynamoDBStoreConfigError(name)
}
