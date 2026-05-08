package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const sqsIngestionQueueBackend = "sqs"
const completedUploadPointerSchemaVersion = 1
const defaultSQSCompletedUploadWaitSeconds int32 = 10
const defaultSQSCompletedUploadVisibilityTimeoutSeconds int32 = 300

type SQSCompletedUploadClient interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type completedUploadPointerMessage struct {
	SchemaVersion int       `json:"schema_version"`
	EpochID       int64     `json:"epoch_id"`
	ShardID       int       `json:"shard_id"`
	Uuid          int64     `json:"uuid"`
	Bucket        string    `json:"payload_s3_bucket"`
	Key           string    `json:"payload_s3_key"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
}

type sqsCompletedUploadQueue struct {
	client                   SQSCompletedUploadClient
	payloadStore             completedUploadPayloadStore
	queueURL                 string
	standbyQueueURL          string
	waitTimeSeconds          int32
	visibilityTimeoutSeconds int32
}

type SQSCompletedUploadQueueOptions struct {
	WaitTimeSeconds          int32
	VisibilityTimeoutSeconds int32
	StandbyQueueURL          string
}

func NewSQSCompletedUploadQueue(sqsClient SQSCompletedUploadClient, s3Client S3CompletedUploadPayloadClient, queueURL string, payloadBucket string) (completedUploadQueue, error) {
	return NewSQSCompletedUploadQueueWithOptions(sqsClient, s3Client, queueURL, payloadBucket, SQSCompletedUploadQueueOptions{
		WaitTimeSeconds:          defaultSQSCompletedUploadWaitSeconds,
		VisibilityTimeoutSeconds: defaultSQSCompletedUploadVisibilityTimeoutSeconds,
	})
}

func NewSQSCompletedUploadQueueWithOptions(sqsClient SQSCompletedUploadClient, s3Client S3CompletedUploadPayloadClient, queueURL string, payloadBucket string, options SQSCompletedUploadQueueOptions) (completedUploadQueue, error) {
	payloadStore, err := newS3CompletedUploadPayloadStore(s3Client, payloadBucket)
	if err != nil {
		return nil, err
	}
	return newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, queueURL, options)
}

func newSQSCompletedUploadQueueWithPayloadStore(sqsClient SQSCompletedUploadClient, payloadStore completedUploadPayloadStore, queueURL string, options SQSCompletedUploadQueueOptions) (completedUploadQueue, error) {
	if sqsClient == nil {
		return nil, errors.New("sqs completed upload client is required")
	}
	if payloadStore == nil {
		return nil, errors.New("completed upload payload store is required")
	}
	if queueURL == "" {
		return nil, errors.New("sqs completed upload queue url is required")
	}
	if options.VisibilityTimeoutSeconds == 0 {
		options.VisibilityTimeoutSeconds = defaultSQSCompletedUploadVisibilityTimeoutSeconds
	}
	if options.WaitTimeSeconds < 0 || options.WaitTimeSeconds > 20 {
		return nil, errors.New("sqs completed upload wait seconds must be between 0 and 20")
	}
	if options.VisibilityTimeoutSeconds <= 0 {
		return nil, errors.New("sqs completed upload visibility timeout seconds must be positive")
	}
	return &sqsCompletedUploadQueue{
		client:                   sqsClient,
		payloadStore:             payloadStore,
		queueURL:                 queueURL,
		standbyQueueURL:          options.StandbyQueueURL,
		waitTimeSeconds:          options.WaitTimeSeconds,
		visibilityTimeoutSeconds: options.VisibilityTimeoutSeconds,
	}, nil
}

func (q *sqsCompletedUploadQueue) Backend() string {
	return sqsIngestionQueueBackend
}

func encodeCompletedUploadPointer(pointer completedUploadPointerMessage) (string, error) {
	if pointer.SchemaVersion == 0 {
		pointer.SchemaVersion = completedUploadPointerSchemaVersion
	}
	if pointer.SchemaVersion != completedUploadPointerSchemaVersion {
		return "", fmt.Errorf("unsupported completed upload pointer schema version %d", pointer.SchemaVersion)
	}
	if pointer.EpochID <= 0 {
		return "", errors.New("completed upload pointer epoch id must be positive")
	}
	if pointer.ShardID < 0 {
		return "", errors.New("completed upload pointer shard id must be non-negative")
	}
	if pointer.Uuid <= 0 {
		return "", errors.New("completed upload pointer uuid must be positive")
	}
	if pointer.Bucket == "" || pointer.Key == "" {
		return "", errors.New("completed upload pointer requires payload bucket and key")
	}
	if pointer.EnqueuedAt.IsZero() {
		pointer.EnqueuedAt = time.Now().UTC()
	}
	data, err := json.Marshal(pointer)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeCompletedUploadPointer(body string) (completedUploadPointerMessage, error) {
	var pointer completedUploadPointerMessage
	if err := json.Unmarshal([]byte(body), &pointer); err != nil {
		return completedUploadPointerMessage{}, err
	}
	if pointer.SchemaVersion != completedUploadPointerSchemaVersion {
		return completedUploadPointerMessage{}, fmt.Errorf("unsupported completed upload pointer schema version %d", pointer.SchemaVersion)
	}
	if pointer.EpochID <= 0 {
		return completedUploadPointerMessage{}, errors.New("completed upload pointer epoch id must be positive")
	}
	if pointer.ShardID < 0 {
		return completedUploadPointerMessage{}, errors.New("completed upload pointer shard id must be non-negative")
	}
	if pointer.Uuid <= 0 {
		return completedUploadPointerMessage{}, errors.New("completed upload pointer uuid must be positive")
	}
	if pointer.Bucket == "" || pointer.Key == "" {
		return completedUploadPointerMessage{}, errors.New("completed upload pointer requires payload bucket and key")
	}
	return pointer, nil
}

func (q *sqsCompletedUploadQueue) Enqueue(ctx context.Context, message CompletedUploadMessage) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if message.EnqueuedAt.IsZero() {
		message.EnqueuedAt = time.Now().UTC()
	}
	ref, err := q.payloadStore.Put(ctx, message)
	if err != nil {
		return "", err
	}
	body, err := encodeCompletedUploadPointer(completedUploadPointerMessage{
		EpochID:    message.EpochID,
		ShardID:    message.ShardID,
		Uuid:       message.Uuid,
		Bucket:     ref.Bucket,
		Key:        ref.Key,
		EnqueuedAt: message.EnqueuedAt,
	})
	if err != nil {
		return "", err
	}
	if q.standbyQueueURL != "" {
		if _, err := q.sendPointer(ctx, q.standbyQueueURL, body); err != nil {
			return "", err
		}
	}
	out, err := q.sendPointer(ctx, q.queueURL, body)
	if err != nil {
		return "", err
	}
	if out.MessageId == nil {
		return "", nil
	}
	return *out.MessageId, nil
}

func (q *sqsCompletedUploadQueue) sendPointer(ctx context.Context, queueURL string, body string) (*sqs.SendMessageOutput, error) {
	return q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
}

func (q *sqsCompletedUploadQueue) Receive(ctx context.Context, maxMessages int) ([]QueuedCompletedUploadMessage, error) {
	if maxMessages <= 0 {
		return nil, errors.New("receive max messages must be positive")
	}
	if maxMessages > 10 {
		maxMessages = 10
	}
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueURL),
		MaxNumberOfMessages: int32(maxMessages),
		WaitTimeSeconds:     q.waitTimeSeconds,
		VisibilityTimeout:   q.visibilityTimeoutSeconds,
	})
	if err != nil {
		return nil, err
	}
	items := make([]QueuedCompletedUploadMessage, 0, len(out.Messages))
	for _, raw := range out.Messages {
		if raw.Body == nil {
			return nil, errors.New("sqs completed upload message missing body")
		}
		if raw.ReceiptHandle == nil {
			return nil, errors.New("sqs completed upload message missing receipt handle")
		}
		pointer, err := decodeCompletedUploadPointer(*raw.Body)
		if err != nil {
			return nil, err
		}
		msg, err := q.payloadStore.Get(ctx, completedUploadPayloadReference{
			Bucket: pointer.Bucket,
			Key:    pointer.Key,
		})
		if err != nil {
			return nil, err
		}
		items = append(items, QueuedCompletedUploadMessage{
			Message:       msg,
			ReceiptHandle: *raw.ReceiptHandle,
		})
	}
	return items, nil
}

func (q *sqsCompletedUploadQueue) Ack(ctx context.Context, receiptHandle string) error {
	if receiptHandle == "" {
		return errors.New("sqs completed upload receipt handle is required")
	}
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (q *sqsCompletedUploadQueue) Stats() IngestionQueueStats {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		return IngestionQueueStats{}
	}
	return IngestionQueueStats{
		Depth:    parseSQSApproximateCount(out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)]),
		Inflight: parseSQSApproximateCount(out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]),
	}
}

func parseSQSApproximateCount(value string) int {
	if value == "" {
		return 0
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 0 {
		return 0
	}
	return count
}
