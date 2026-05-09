package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const completedUploadPayloadSchemaVersion = 1

type completedUploadPayloadReference struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type completedUploadPayloadEnvelope struct {
	SchemaVersion int                    `json:"schema_version"`
	EncodedAt     time.Time              `json:"encoded_at"`
	Message       CompletedUploadMessage `json:"message"`
}

type completedUploadPayloadStore interface {
	Put(context.Context, CompletedUploadMessage) (completedUploadPayloadReference, error)
	Get(context.Context, completedUploadPayloadReference) (CompletedUploadMessage, error)
}

type S3CompletedUploadPayloadClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type s3CompletedUploadPayloadStore struct {
	client S3CompletedUploadPayloadClient
	bucket string
}

func encodeCompletedUploadPayload(message CompletedUploadMessage) ([]byte, error) {
	envelope := completedUploadPayloadEnvelope{
		SchemaVersion: completedUploadPayloadSchemaVersion,
		EncodedAt:     time.Now().UTC(),
		Message:       message,
	}
	return json.Marshal(envelope)
}

func decodeCompletedUploadPayload(data []byte) (CompletedUploadMessage, error) {
	var envelope completedUploadPayloadEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return CompletedUploadMessage{}, err
	}
	if envelope.SchemaVersion != completedUploadPayloadSchemaVersion {
		return CompletedUploadMessage{}, fmt.Errorf("unsupported completed upload payload schema version %d", envelope.SchemaVersion)
	}
	if envelope.Message.EpochID <= 0 {
		return CompletedUploadMessage{}, errors.New("completed upload payload epoch id must be positive")
	}
	if envelope.Message.Uuid <= 0 {
		return CompletedUploadMessage{}, errors.New("completed upload payload uuid must be positive")
	}
	return envelope.Message, nil
}

func completedUploadPayloadKey(message CompletedUploadMessage) (string, error) {
	if message.ShardID < 0 {
		return "", errors.New("completed upload payload shard id must be non-negative")
	}
	if message.EpochID <= 0 {
		return "", errors.New("completed upload payload epoch id must be positive")
	}
	if message.Uuid <= 0 {
		return "", errors.New("completed upload payload uuid must be positive")
	}
	return fmt.Sprintf("completed-uploads/shard-%d/epoch-%d/uuid-%d.json", message.ShardID, message.EpochID, message.Uuid), nil
}

func newS3CompletedUploadPayloadStore(client S3CompletedUploadPayloadClient, bucket string) (*s3CompletedUploadPayloadStore, error) {
	if client == nil {
		return nil, errors.New("s3 payload client is required")
	}
	if bucket == "" {
		return nil, errors.New("s3 payload bucket is required")
	}
	return &s3CompletedUploadPayloadStore{client: client, bucket: bucket}, nil
}

func (s *s3CompletedUploadPayloadStore) Put(ctx context.Context, message CompletedUploadMessage) (completedUploadPayloadReference, error) {
	key, err := completedUploadPayloadKey(message)
	if err != nil {
		return completedUploadPayloadReference{}, err
	}
	payload, err := encodeCompletedUploadPayload(message)
	if err != nil {
		return completedUploadPayloadReference{}, err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return completedUploadPayloadReference{}, err
	}
	return completedUploadPayloadReference{Bucket: s.bucket, Key: key}, nil
}

func (s *s3CompletedUploadPayloadStore) Get(ctx context.Context, ref completedUploadPayloadReference) (CompletedUploadMessage, error) {
	if ref.Bucket == "" || ref.Key == "" {
		return CompletedUploadMessage{}, errors.New("completed upload payload reference requires bucket and key")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ref.Bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return CompletedUploadMessage{}, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return CompletedUploadMessage{}, err
	}
	return decodeCompletedUploadPayload(data)
}
