package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

var (
	errLeaseHeld     = errors.New("coordinator lease is held")
	errLeaseNotHeld  = errors.New("coordinator lease is not held")
	errStaleFence    = errors.New("stale coordinator fencing token")
	errEpochMismatch = errors.New("epoch mismatch")
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

type memoryControlStore struct {
	mu                 sync.Mutex
	lease              CoordinatorLease
	hasLease           bool
	lastFencingToken   int64
	epoch              db.EpochMeta
	hasEpoch           bool
	accepting          bool
	shardConfigVersion int64
}

func newMemoryControlStore(shardConfigVersion int64) *memoryControlStore {
	return &memoryControlStore{shardConfigVersion: shardConfigVersion}
}

func (s *memoryControlStore) AcquireLease(now time.Time, holder string, ttl time.Duration) (CoordinatorLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if holder == "" {
		return CoordinatorLease{}, errors.New("lease holder is required")
	}
	if ttl <= 0 {
		return CoordinatorLease{}, errors.New("lease ttl must be positive")
	}
	if s.hasLease && now.Before(s.lease.ExpiresAt) {
		if s.lease.Holder != holder {
			return CoordinatorLease{}, errLeaseHeld
		}
		s.lease.ExpiresAt = now.Add(ttl)
		return s.lease, nil
	}
	s.lastFencingToken++
	s.lease = CoordinatorLease{
		Holder:       holder,
		FencingToken: s.lastFencingToken,
		ExpiresAt:    now.Add(ttl),
	}
	s.hasLease = true
	return s.lease, nil
}

func (s *memoryControlStore) RenewLease(now time.Time, holder string, fencingToken int64, ttl time.Duration) (CoordinatorLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ttl <= 0 {
		return CoordinatorLease{}, errors.New("lease ttl must be positive")
	}
	if !s.hasLease || !now.Before(s.lease.ExpiresAt) {
		return CoordinatorLease{}, errLeaseNotHeld
	}
	if s.lease.Holder != holder || s.lease.FencingToken != fencingToken {
		return CoordinatorLease{}, errStaleFence
	}
	s.lease.ExpiresAt = now.Add(ttl)
	return s.lease, nil
}

func (s *memoryControlStore) CurrentLease(now time.Time) (CoordinatorLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasLease || !now.Before(s.lease.ExpiresAt) {
		return CoordinatorLease{}, false
	}
	return s.lease, true
}

func (s *memoryControlStore) StartEpoch(epoch db.EpochMeta, shardConfigVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch.ID <= 0 {
		return errors.New("epoch id must be positive")
	}
	s.epoch = epoch
	s.hasEpoch = true
	s.accepting = epoch.State == db.EpochStateActive
	s.shardConfigVersion = shardConfigVersion
	return nil
}

func (s *memoryControlStore) CompleteEpoch(epochID int64) (db.EpochMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return db.EpochMeta{}, errEpochMismatch
	}
	s.epoch.State = db.EpochStateCompleted
	s.accepting = false
	return s.epoch, nil
}

func (s *memoryControlStore) CurrentEpoch() (db.EpochMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch {
		return db.EpochMeta{}, false
	}
	return s.epoch, true
}

func (s *memoryControlStore) SetAccepting(epochID int64, accepting bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return errEpochMismatch
	}
	s.accepting = accepting
	return nil
}

func (s *memoryControlStore) Accepting(epochID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return false, errEpochMismatch
	}
	return s.accepting, nil
}

func (s *memoryControlStore) ShardConfigVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shardConfigVersion
}

func (s *memoryControlStore) SetShardConfigVersion(version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version <= 0 {
		return errors.New("shard config version must be positive")
	}
	s.shardConfigVersion = version
	return nil
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

type memoryIngestionQueue struct {
	mu            sync.Mutex
	nextMessageID int64
	nextReceiptID int64
	available     []string
	messages      map[string]IngestionMessage
	inflight      map[string]string
}

func newMemoryIngestionQueue() *memoryIngestionQueue {
	return &memoryIngestionQueue{
		messages: make(map[string]IngestionMessage),
		inflight: make(map[string]string),
	}
}

func (q *memoryIngestionQueue) Enqueue(ctx context.Context, message IngestionMessage) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if message.EpochID <= 0 {
		return "", errors.New("epoch id must be positive")
	}
	if message.ID == "" {
		q.nextMessageID++
		message.ID = fmt.Sprintf("msg-%d", q.nextMessageID)
	}
	if message.EnqueuedAt.IsZero() {
		message.EnqueuedAt = time.Now().UTC()
	}
	if _, exists := q.messages[message.ID]; exists {
		return "", fmt.Errorf("duplicate ingestion message id %q", message.ID)
	}
	q.messages[message.ID] = message
	q.available = append(q.available, message.ID)
	return message.ID, nil
}

func (q *memoryIngestionQueue) Receive(ctx context.Context, maxMessages int) ([]QueuedIngestionMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxMessages <= 0 {
		return nil, errors.New("receive max messages must be positive")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if maxMessages > len(q.available) {
		maxMessages = len(q.available)
	}
	out := make([]QueuedIngestionMessage, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		messageID := q.available[0]
		q.available = q.available[1:]
		q.nextReceiptID++
		receipt := fmt.Sprintf("receipt-%d", q.nextReceiptID)
		q.inflight[receipt] = messageID
		out = append(out, QueuedIngestionMessage{
			Message:       q.messages[messageID],
			ReceiptHandle: receipt,
		})
	}
	return out, nil
}

func (q *memoryIngestionQueue) Ack(ctx context.Context, receiptHandle string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	messageID, ok := q.inflight[receiptHandle]
	if !ok {
		return fmt.Errorf("unknown receipt handle %q", receiptHandle)
	}
	delete(q.inflight, receiptHandle)
	delete(q.messages, messageID)
	return nil
}
