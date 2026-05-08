package controlstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

type MemoryControlStore struct {
	mu               sync.Mutex
	lease            CoordinatorLease
	hasLease         bool
	lastFencingToken int64
	epoch            db.EpochMeta
	hasEpoch         bool
	accepting        bool
	shardConfig      ShardConfigRecord
	hasShardConfig   bool
	epochShardConfig map[int64]ShardConfigRecord
	scalingByEpoch   map[int64]ScalingRecommendationRecord
	latestScaling    ScalingRecommendationRecord
	hasLatestScaling bool
}

func NewMemoryControlStore(shardConfigVersion int64) *MemoryControlStore {
	return &MemoryControlStore{
		epochShardConfig: make(map[int64]ShardConfigRecord),
		scalingByEpoch:   make(map[int64]ScalingRecommendationRecord),
	}
}

func (s *MemoryControlStore) AcquireLease(now time.Time, holder string, ttl time.Duration) (CoordinatorLease, error) {
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

func (s *MemoryControlStore) RenewLease(now time.Time, holder string, fencingToken int64, ttl time.Duration) (CoordinatorLease, error) {
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

func (s *MemoryControlStore) CurrentLease(now time.Time) (CoordinatorLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasLease || !now.Before(s.lease.ExpiresAt) {
		return CoordinatorLease{}, false
	}
	return s.lease, true
}

func (s *MemoryControlStore) StartEpoch(epoch db.EpochMeta, shardConfigVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch.ID <= 0 {
		return errors.New("epoch id must be positive")
	}
	s.epoch = epoch
	s.hasEpoch = true
	s.accepting = epoch.State == db.EpochStateActive
	if s.hasShardConfig {
		s.shardConfig.Version = shardConfigVersion
	}
	return nil
}

func (s *MemoryControlStore) CompleteEpoch(epochID int64) (db.EpochMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return db.EpochMeta{}, errEpochMismatch
	}
	s.epoch.State = db.EpochStateCompleted
	s.accepting = false
	return s.epoch, nil
}

func (s *MemoryControlStore) CurrentEpoch() (db.EpochMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch {
		return db.EpochMeta{}, false
	}
	return s.epoch, true
}

func (s *MemoryControlStore) SetAccepting(epochID int64, accepting bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return errEpochMismatch
	}
	s.accepting = accepting
	return nil
}

func (s *MemoryControlStore) Accepting(epochID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasEpoch || s.epoch.ID != epochID {
		return false, errEpochMismatch
	}
	return s.accepting, nil
}

func (s *MemoryControlStore) GetShardConfig() (ShardConfigRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasShardConfig {
		return ShardConfigRecord{}, false, nil
	}
	config := s.shardConfig
	config.Shards = append([]ShardConfig(nil), s.shardConfig.Shards...)
	return config, true, nil
}

func (s *MemoryControlStore) PutShardConfig(config ShardConfigRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateShardConfigRecord(config); err != nil {
		return err
	}
	if s.hasShardConfig && config.Version < s.shardConfig.Version {
		return errors.New("stale shard config version")
	}
	config.Shards = append([]ShardConfig(nil), config.Shards...)
	s.shardConfig = config
	s.hasShardConfig = true
	return nil
}

func (s *MemoryControlStore) GetEpochShardConfig(epochID int64) (ShardConfigRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	config, ok := s.epochShardConfig[epochID]
	if !ok {
		return ShardConfigRecord{}, false, nil
	}
	config.Shards = append([]ShardConfig(nil), config.Shards...)
	return config, true, nil
}

func (s *MemoryControlStore) PutEpochShardConfig(epochID int64, config ShardConfigRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epochID <= 0 {
		return errors.New("epoch id must be positive")
	}
	config.Key = epochShardConfigKey(epochID)
	if err := validateShardConfigRecord(config); err != nil {
		return err
	}
	if existing, ok := s.epochShardConfig[epochID]; ok {
		if !shardConfigRecordsEqual(existing, config) {
			return errors.New("epoch shard config already exists with different config")
		}
		return nil
	}
	config.Shards = append([]ShardConfig(nil), config.Shards...)
	s.epochShardConfig[epochID] = config
	return nil
}

func (s *MemoryControlStore) PutScalingRecommendation(record ScalingRecommendationRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateScalingRecommendationRecord(record); err != nil {
		return err
	}
	record.Key = epochScalingRecommendationKey(record.EpochID)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if existing, ok := s.scalingByEpoch[record.EpochID]; ok {
		if !scalingRecommendationRecordsEqual(existing, record) {
			return errors.New("scaling recommendation already exists with different record")
		}
	} else {
		s.scalingByEpoch[record.EpochID] = record
	}
	latest := record
	latest.Key = latestScalingRecommendationKey
	if !s.hasLatestScaling || record.EpochID >= s.latestScaling.EpochID {
		s.latestScaling = latest
		s.hasLatestScaling = true
	}
	return nil
}

func (s *MemoryControlStore) GetLatestScalingRecommendation() (ScalingRecommendationRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasLatestScaling {
		return ScalingRecommendationRecord{}, false, nil
	}
	return s.latestScaling, true, nil
}

func (s *MemoryControlStore) GetEpochScalingRecommendation(epochID int64) (ScalingRecommendationRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.scalingByEpoch[epochID]
	if !ok {
		return ScalingRecommendationRecord{}, false, nil
	}
	return record, true, nil
}

type memoryIngestionQueue struct {
	mu            sync.Mutex
	nextMessageID int64
	nextReceiptID int64
	available     []string
	messages      map[string]IngestionMessage
	inflight      map[string]string
}

type MemorySessionStore struct {
	mu       sync.Mutex
	sessions map[int64]SessionRecord
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[int64]SessionRecord)}
}

func (s *MemorySessionStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func (s *MemorySessionStore) PutSession(ctx context.Context, session SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if session.EpochID <= 0 {
		return errors.New("session epoch id must be positive")
	}
	if session.GlobalUUID <= 0 {
		return errors.New("session global uuid must be positive")
	}
	if session.LocalUUID <= 0 {
		return errors.New("session local uuid must be positive")
	}
	if session.CreatedAt.IsZero() {
		session.CreatedAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[session.GlobalUUID]; exists {
		return errSessionExists
	}
	s.sessions[session.GlobalUUID] = session
	return nil
}

func (s *MemorySessionStore) GetSession(ctx context.Context, globalUUID int64) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[globalUUID]
	if !ok {
		return SessionRecord{}, errSessionMissing
	}
	return session, nil
}

func (s *MemorySessionStore) DeleteSession(ctx context.Context, globalUUID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[globalUUID]; !ok {
		return errSessionMissing
	}
	delete(s.sessions, globalUUID)
	return nil
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
