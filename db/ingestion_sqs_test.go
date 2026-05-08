package db

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type fakePayloadStore struct {
	mu    sync.Mutex
	puts  []CompletedUploadMessage
	items map[completedUploadPayloadReference]CompletedUploadMessage
}

func newFakePayloadStore() *fakePayloadStore {
	return &fakePayloadStore{items: make(map[completedUploadPayloadReference]CompletedUploadMessage)}
}

func (s *fakePayloadStore) Put(_ context.Context, message CompletedUploadMessage) (completedUploadPayloadReference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := completedUploadPayloadReference{
		Bucket: "payload-bucket",
		Key:    "payload-key",
	}
	if len(s.puts) > 0 {
		ref.Key = "payload-key-next"
	}
	s.puts = append(s.puts, message)
	s.items[ref] = message
	return ref, nil
}

func (s *fakePayloadStore) Get(_ context.Context, ref completedUploadPayloadReference) (CompletedUploadMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.items[ref]
	if !ok {
		return CompletedUploadMessage{}, errors.New("missing payload")
	}
	return msg, nil
}

type fakeSQSMessage struct {
	id      string
	body    string
	receipt string
	visible bool
}

type fakeSQSClient struct {
	mu           sync.Mutex
	nextID       int
	messages     []fakeSQSMessage
	deleted      []string
	receiveInput *sqs.ReceiveMessageInput
}

func (c *fakeSQSClient) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	id := "msg-1"
	receipt := "receipt-1"
	if c.nextID > 1 {
		id = "msg-next"
		receipt = "receipt-next"
	}
	c.messages = append(c.messages, fakeSQSMessage{
		id:      id,
		body:    aws.ToString(input.MessageBody),
		receipt: receipt,
		visible: true,
	})
	return &sqs.SendMessageOutput{MessageId: aws.String(id)}, nil
}

func (c *fakeSQSClient) ReceiveMessage(_ context.Context, input *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.receiveInput = input
	for i := range c.messages {
		if !c.messages[i].visible {
			continue
		}
		c.messages[i].visible = false
		return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{{
			Body:          aws.String(c.messages[i].body),
			ReceiptHandle: aws.String(c.messages[i].receipt),
		}}}, nil
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (c *fakeSQSClient) DeleteMessage(_ context.Context, input *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	receipt := aws.ToString(input.ReceiptHandle)
	c.deleted = append(c.deleted, receipt)
	for i := range c.messages {
		if c.messages[i].receipt == receipt {
			c.messages = append(c.messages[:i], c.messages[i+1:]...)
			break
		}
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (c *fakeSQSClient) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	visible := 0
	inflight := 0
	for _, msg := range c.messages {
		if msg.visible {
			visible++
		} else {
			inflight++
		}
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
		string(sqstypes.QueueAttributeNameApproximateNumberOfMessages):           strconv.Itoa(visible),
		string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible): strconv.Itoa(inflight),
	}}, nil
}

type fakeS3PayloadClient struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeS3PayloadClient() *fakeS3PayloadClient {
	return &fakeS3PayloadClient{objects: make(map[string][]byte)}
}

func (c *fakeS3PayloadClient) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	c.objects[aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key)] = data
	return &s3.PutObjectOutput{}, nil
}

func (c *fakeS3PayloadClient) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.objects[aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key)]
	if !ok {
		return nil, errors.New("missing s3 object")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func TestCompletedUploadPayloadEnvelopeRoundTrip(t *testing.T) {
	msg := CompletedUploadMessage{
		EpochID:   3,
		ShardID:   1,
		Uuid:      99,
		GlobalRow: 256,
		LocalRow:  0,
	}
	data, err := encodeCompletedUploadPayload(msg)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	got, err := decodeCompletedUploadPayload(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.EpochID != msg.EpochID || got.ShardID != msg.ShardID || got.Uuid != msg.Uuid || got.GlobalRow != msg.GlobalRow {
		t.Fatalf("unexpected decoded message: %+v", got)
	}
}

func TestS3CompletedUploadPayloadStoreUsesDeterministicKey(t *testing.T) {
	client := newFakeS3PayloadClient()
	store, err := newS3CompletedUploadPayloadStore(client, "bucket")
	if err != nil {
		t.Fatalf("store create failed: %v", err)
	}
	ref, err := store.Put(context.Background(), CompletedUploadMessage{EpochID: 4, ShardID: 2, Uuid: 77})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if ref.Bucket != "bucket" || ref.Key != "completed-uploads/shard-2/epoch-4/uuid-77.json" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
	got, err := store.Get(context.Background(), ref)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.EpochID != 4 || got.ShardID != 2 || got.Uuid != 77 {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestCompletedUploadPointerRoundTripAndValidation(t *testing.T) {
	body, err := encodeCompletedUploadPointer(completedUploadPointerMessage{
		EpochID: 1,
		ShardID: 0,
		Uuid:    7,
		Bucket:  "bucket",
		Key:     "key",
	})
	if err != nil {
		t.Fatalf("encode pointer failed: %v", err)
	}
	pointer, err := decodeCompletedUploadPointer(body)
	if err != nil {
		t.Fatalf("decode pointer failed: %v", err)
	}
	if pointer.EpochID != 1 || pointer.Uuid != 7 || pointer.Bucket != "bucket" || pointer.Key != "key" {
		t.Fatalf("unexpected pointer: %+v", pointer)
	}
	if _, err := encodeCompletedUploadPointer(completedUploadPointerMessage{EpochID: 1, ShardID: 0, Uuid: 7}); err == nil {
		t.Fatal("expected missing bucket/key to fail")
	}
}

func TestSQSCompletedUploadQueueEnqueueWritesPayloadBeforePointer(t *testing.T) {
	payloadStore := newFakePayloadStore()
	sqsClient := &fakeSQSClient{}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	id, err := queue.Enqueue(context.Background(), CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 7})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if id != "msg-1" {
		t.Fatalf("unexpected message id %q", id)
	}
	if len(payloadStore.puts) != 1 || payloadStore.puts[0].Uuid != 7 {
		t.Fatalf("expected payload put before pointer send, got %+v", payloadStore.puts)
	}
	if len(sqsClient.messages) != 1 {
		t.Fatalf("expected one sqs message, got %d", len(sqsClient.messages))
	}
}

func TestSQSCompletedUploadQueueReceiveLoadsPayloadAndAckDeletesMessage(t *testing.T) {
	payloadStore := newFakePayloadStore()
	sqsClient := &fakeSQSClient{}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{
		WaitTimeSeconds:          7,
		VisibilityTimeoutSeconds: 42,
	})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	if _, err := queue.Enqueue(context.Background(), CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 8}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	items, err := queue.Receive(context.Background(), 1)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}
	if len(items) != 1 || items[0].Message.Uuid != 8 || items[0].ReceiptHandle == "" {
		t.Fatalf("unexpected items: %+v", items)
	}
	if sqsClient.receiveInput == nil || sqsClient.receiveInput.MaxNumberOfMessages != 1 || sqsClient.receiveInput.WaitTimeSeconds != 7 || sqsClient.receiveInput.VisibilityTimeout != 42 {
		t.Fatalf("unexpected receive input: %+v", sqsClient.receiveInput)
	}
	if err := queue.Ack(context.Background(), items[0].ReceiptHandle); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if len(sqsClient.deleted) != 1 || sqsClient.deleted[0] != items[0].ReceiptHandle {
		t.Fatalf("expected delete for receipt, got %+v", sqsClient.deleted)
	}
}

func TestSQSCompletedUploadQueueRejectsInvalidOptions(t *testing.T) {
	payloadStore := newFakePayloadStore()
	tests := []SQSCompletedUploadQueueOptions{
		{WaitTimeSeconds: -1},
		{WaitTimeSeconds: 21},
		{VisibilityTimeoutSeconds: -1},
	}
	for _, options := range tests {
		if _, err := newSQSCompletedUploadQueueWithPayloadStore(&fakeSQSClient{}, payloadStore, "queue-url", options); err == nil {
			t.Fatalf("expected invalid options to fail: %+v", options)
		}
	}
}

func TestSQSCompletedUploadQueueMalformedPointerRemainsUnacked(t *testing.T) {
	payloadStore := newFakePayloadStore()
	sqsClient := &fakeSQSClient{
		messages: []fakeSQSMessage{{
			id:      "msg-1",
			body:    "{bad-json",
			receipt: "receipt-1",
			visible: true,
		}},
	}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	if _, err := queue.Receive(context.Background(), 1); err == nil {
		t.Fatal("expected malformed pointer to fail")
	}
	if len(sqsClient.deleted) != 0 {
		t.Fatalf("expected malformed pointer to remain unacked, deleted=%+v", sqsClient.deleted)
	}
	stats := queue.Stats()
	if stats.Inflight != 1 {
		t.Fatalf("expected malformed pointer to stay in flight, got %+v", stats)
	}
}

func TestSQSCompletedUploadQueueMissingPayloadRemainsUnacked(t *testing.T) {
	payloadStore := newFakePayloadStore()
	body, err := encodeCompletedUploadPointer(completedUploadPointerMessage{
		EpochID: 1,
		ShardID: 0,
		Uuid:    7,
		Bucket:  "payload-bucket",
		Key:     "missing-key",
	})
	if err != nil {
		t.Fatalf("encode pointer failed: %v", err)
	}
	sqsClient := &fakeSQSClient{
		messages: []fakeSQSMessage{{
			id:      "msg-1",
			body:    body,
			receipt: "receipt-1",
			visible: true,
		}},
	}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	if _, err := queue.Receive(context.Background(), 1); err == nil {
		t.Fatal("expected missing payload to fail")
	}
	if len(sqsClient.deleted) != 0 {
		t.Fatalf("expected missing payload to remain unacked, deleted=%+v", sqsClient.deleted)
	}
	stats := queue.Stats()
	if stats.Inflight != 1 {
		t.Fatalf("expected missing payload pointer to stay in flight, got %+v", stats)
	}
}

func TestIngestionWorkerLeavesSQSMessageUnackedOnFailure(t *testing.T) {
	payloadStore := newFakePayloadStore()
	sqsClient := &fakeSQSClient{}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	s := newTestLeaderServer()
	if err := s.SetCompletedUploadQueue(queue); err != nil {
		t.Fatalf("set queue failed: %v", err)
	}
	s.processUploadFn = func(msg CompletedUploadMessage) (bool, error) {
		return false, errors.New("prepare failed")
	}
	s.commitUploadFn = func(uuid int64, shouldCommit bool) error {
		t.Fatal("commit should not run after prepare failure")
		return nil
	}
	if _, err := queue.Enqueue(context.Background(), CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 9}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	items, err := queue.Receive(context.Background(), 1)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}
	s.processIngestionJob(items[0])
	if len(sqsClient.deleted) != 0 {
		t.Fatalf("expected failed processing to leave message unacked, deleted=%+v", sqsClient.deleted)
	}
	stats := queue.Stats()
	if stats.Inflight != 1 {
		t.Fatalf("expected in-flight message after failure, got %+v", stats)
	}
}

func TestUpload3WithSQSBackendEnqueuesCompletedPayload(t *testing.T) {
	payloadStore := newFakePayloadStore()
	sqsClient := &fakeSQSClient{}
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(sqsClient, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	s := newTestLeaderServer()
	if err := s.SetCompletedUploadQueue(queue); err != nil {
		t.Fatalf("set queue failed: %v", err)
	}
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &StartEpochReply{}); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	defer s.stopEpochTimer()

	var up1 UploadReply1
	if err := s.Upload1(&UploadArgs1{RouteRow: 3}, &up1); err != nil {
		t.Fatalf("upload1 failed: %v", err)
	}
	if err := s.Upload2(&UploadArgs2{Uuid: up1.Uuid, HashKey: up1.HashKey}, &UploadReply2{}); err != nil {
		t.Fatalf("upload2 failed: %v", err)
	}
	if err := s.Upload3(&UploadArgs3{Uuid: up1.Uuid, HashKey: up1.HashKey}, &UploadReply3{}); err != nil {
		t.Fatalf("upload3 failed: %v", err)
	}
	if len(payloadStore.puts) != 1 {
		t.Fatalf("expected one payload write, got %d", len(payloadStore.puts))
	}
	if payloadStore.puts[0].LocalRow != 3 || payloadStore.puts[0].Uuid != up1.Uuid {
		t.Fatalf("unexpected payload: %+v", payloadStore.puts[0])
	}
	if len(sqsClient.messages) != 1 {
		t.Fatalf("expected one sqs pointer, got %d", len(sqsClient.messages))
	}
}

func TestSQSStatsParseLargeCounts(t *testing.T) {
	if got := parseSQSApproximateCount("12"); got != 12 {
		t.Fatalf("expected 12, got %d", got)
	}
	if got := parseSQSApproximateCount("-1"); got != 0 {
		t.Fatalf("expected invalid count to parse as zero, got %d", got)
	}
}

func TestSQSReceiveRejectsEmptyQueueImmediatelyFromFake(t *testing.T) {
	payloadStore := newFakePayloadStore()
	queue, err := newSQSCompletedUploadQueueWithPayloadStore(&fakeSQSClient{}, payloadStore, "queue-url", SQSCompletedUploadQueueOptions{})
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	items, err := queue.Receive(ctx, 1)
	if err != nil {
		t.Fatalf("receive empty queue failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no items, got %+v", items)
	}
}
