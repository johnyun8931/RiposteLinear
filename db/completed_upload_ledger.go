package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	memoryCompletedUploadLedgerBackend   = "memory"
	CompletedUploadLedgerStateProcessing = "processing"
	CompletedUploadLedgerStateCommitted  = "committed"
	defaultCompletedUploadProcessingTTL  = 15 * time.Minute
)

var errCompletedUploadProcessingBusy = errors.New("completed upload processing is busy")

type CompletedUploadProcessingLease struct {
	ShardID   int
	EpochID   int64
	Uuid      int64
	AttemptID string
}

type CompletedUploadLedgerBeginResult struct {
	AlreadyCommitted bool
	Lease            CompletedUploadProcessingLease
}

type CompletedUploadLedger interface {
	Backend() string
	BeginProcessing(ctx context.Context, message CompletedUploadMessage, now time.Time, ttl time.Duration) (CompletedUploadLedgerBeginResult, error)
	CompleteProcessing(ctx context.Context, lease CompletedUploadProcessingLease, now time.Time) error
}

type completedUploadLedgerRecord struct {
	State               string
	AttemptID           string
	ProcessingExpiresAt time.Time
	CommittedAt         time.Time
	UpdatedAt           time.Time
}

type memoryCompletedUploadLedger struct {
	mu      sync.Mutex
	nextID  int64
	records map[string]completedUploadLedgerRecord
}

func newMemoryCompletedUploadLedger() *memoryCompletedUploadLedger {
	return &memoryCompletedUploadLedger{records: make(map[string]completedUploadLedgerRecord)}
}

func (l *memoryCompletedUploadLedger) Backend() string {
	return memoryCompletedUploadLedgerBackend
}

func completedUploadLedgerKey(shardID int, epochID int64, uuid int64) string {
	return fmt.Sprintf("completed-upload#shard#%d#epoch#%d#uuid#%d", shardID, epochID, uuid)
}

func validateCompletedUploadLedgerIdentity(shardID int, epochID int64, uuid int64) error {
	if shardID < 0 {
		return errors.New("completed upload ledger shard id must be non-negative")
	}
	if epochID <= 0 {
		return errors.New("completed upload ledger epoch id must be positive")
	}
	if uuid <= 0 {
		return errors.New("completed upload ledger uuid must be positive")
	}
	return nil
}

func (l *memoryCompletedUploadLedger) BeginProcessing(ctx context.Context, message CompletedUploadMessage, now time.Time, ttl time.Duration) (CompletedUploadLedgerBeginResult, error) {
	if err := ctx.Err(); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	}
	if ttl <= 0 {
		return CompletedUploadLedgerBeginResult{}, errors.New("completed upload processing ttl must be positive")
	}
	if err := validateCompletedUploadLedgerIdentity(message.ShardID, message.EpochID, message.Uuid); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	key := completedUploadLedgerKey(message.ShardID, message.EpochID, message.Uuid)
	l.mu.Lock()
	defer l.mu.Unlock()
	if record, ok := l.records[key]; ok {
		switch record.State {
		case CompletedUploadLedgerStateCommitted:
			return CompletedUploadLedgerBeginResult{AlreadyCommitted: true}, nil
		case CompletedUploadLedgerStateProcessing:
			if now.Before(record.ProcessingExpiresAt) {
				return CompletedUploadLedgerBeginResult{}, errCompletedUploadProcessingBusy
			}
		}
	}

	l.nextID++
	lease := CompletedUploadProcessingLease{
		ShardID:   message.ShardID,
		EpochID:   message.EpochID,
		Uuid:      message.Uuid,
		AttemptID: fmt.Sprintf("memory-attempt-%d", l.nextID),
	}
	l.records[key] = completedUploadLedgerRecord{
		State:               CompletedUploadLedgerStateProcessing,
		AttemptID:           lease.AttemptID,
		ProcessingExpiresAt: now.Add(ttl),
		UpdatedAt:           now,
	}
	return CompletedUploadLedgerBeginResult{Lease: lease}, nil
}

func (l *memoryCompletedUploadLedger) CompleteProcessing(ctx context.Context, lease CompletedUploadProcessingLease, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCompletedUploadLedgerIdentity(lease.ShardID, lease.EpochID, lease.Uuid); err != nil {
		return err
	}
	if lease.AttemptID == "" {
		return errors.New("completed upload processing attempt id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	key := completedUploadLedgerKey(lease.ShardID, lease.EpochID, lease.Uuid)
	l.mu.Lock()
	defer l.mu.Unlock()
	record, ok := l.records[key]
	if ok && record.State == CompletedUploadLedgerStateCommitted {
		return nil
	}
	if !ok || record.State != CompletedUploadLedgerStateProcessing || record.AttemptID != lease.AttemptID {
		return errCompletedUploadProcessingBusy
	}
	l.records[key] = completedUploadLedgerRecord{
		State:       CompletedUploadLedgerStateCommitted,
		AttemptID:   lease.AttemptID,
		CommittedAt: now,
		UpdatedAt:   now,
	}
	return nil
}
