package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const memoryIngestionQueueBackend = "memory"

type CompletedUploadMessage struct {
	ID         string
	EpochID    int64
	ShardID    int
	Uuid       int64
	HashKey    [32]byte
	Challenge  [16]byte
	GlobalRow  int
	LocalRow   int
	EnqueuedAt time.Time
	Attempts   int
	Args1      UploadArgs1
	Args2      UploadArgs2
	Args3      UploadArgs3
}

type QueuedCompletedUploadMessage struct {
	Message       CompletedUploadMessage
	ReceiptHandle string
}

type IngestionQueueStats struct {
	Depth    int
	Inflight int
}

type completedUploadQueue interface {
	Backend() string
	Enqueue(context.Context, CompletedUploadMessage) (string, error)
	Receive(context.Context, int) ([]QueuedCompletedUploadMessage, error)
	Ack(context.Context, string) error
	Stats() IngestionQueueStats
}

type CompletedUploadQueue = completedUploadQueue

type memoryCompletedUploadQueue struct {
	mu            sync.Mutex
	nextMessageID int64
	nextReceiptID int64
	available     []string
	messages      map[string]CompletedUploadMessage
	inflight      map[string]string
	notify        chan struct{}
}

func newMemoryCompletedUploadQueue() *memoryCompletedUploadQueue {
	return &memoryCompletedUploadQueue{
		messages: make(map[string]CompletedUploadMessage),
		inflight: make(map[string]string),
		notify:   make(chan struct{}),
	}
}

func (q *memoryCompletedUploadQueue) Backend() string {
	return memoryIngestionQueueBackend
}

func (q *memoryCompletedUploadQueue) Enqueue(ctx context.Context, message CompletedUploadMessage) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if message.EpochID <= 0 {
		return "", errors.New("epoch id must be positive")
	}
	if message.Uuid <= 0 {
		return "", errors.New("upload uuid must be positive")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if message.ID == "" {
		q.nextMessageID++
		message.ID = fmt.Sprintf("completed-upload-%d", q.nextMessageID)
	}
	if message.EnqueuedAt.IsZero() {
		message.EnqueuedAt = time.Now().UTC()
	}
	if _, exists := q.messages[message.ID]; exists {
		return "", fmt.Errorf("duplicate ingestion message id %q", message.ID)
	}
	q.messages[message.ID] = message
	q.available = append(q.available, message.ID)
	close(q.notify)
	q.notify = make(chan struct{})
	return message.ID, nil
}

func (q *memoryCompletedUploadQueue) Receive(ctx context.Context, maxMessages int) ([]QueuedCompletedUploadMessage, error) {
	if maxMessages <= 0 {
		return nil, errors.New("receive max messages must be positive")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		q.mu.Lock()
		if len(q.available) > 0 {
			if maxMessages > len(q.available) {
				maxMessages = len(q.available)
			}
			out := make([]QueuedCompletedUploadMessage, 0, maxMessages)
			for i := 0; i < maxMessages; i++ {
				messageID := q.available[0]
				q.available = q.available[1:]
				q.nextReceiptID++
				receipt := fmt.Sprintf("completed-upload-receipt-%d", q.nextReceiptID)
				q.inflight[receipt] = messageID
				msg := q.messages[messageID]
				msg.Attempts++
				q.messages[messageID] = msg
				out = append(out, QueuedCompletedUploadMessage{
					Message:       msg,
					ReceiptHandle: receipt,
				})
			}
			q.mu.Unlock()
			return out, nil
		}
		notify := q.notify
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (q *memoryCompletedUploadQueue) Ack(ctx context.Context, receiptHandle string) error {
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

func (q *memoryCompletedUploadQueue) Stats() IngestionQueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return IngestionQueueStats{
		Depth:    len(q.available),
		Inflight: len(q.inflight),
	}
}
