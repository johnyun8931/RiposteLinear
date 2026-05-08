package db

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeDynamoDBCompletedUploadLedgerClient struct {
	getItem    func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	updateItem func(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	updates    []*dynamodb.UpdateItemInput
}

func (f *fakeDynamoDBCompletedUploadLedgerClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.getItem == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	return f.getItem(input)
}

func (f *fakeDynamoDBCompletedUploadLedgerClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.updates = append(f.updates, input)
	if f.updateItem == nil {
		return &dynamodb.UpdateItemOutput{}, nil
	}
	return f.updateItem(input)
}

func completedUploadLedgerItem(state string, attemptID string, processingExpires time.Time) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		"state": &ddbtypes.AttributeValueMemberS{Value: state},
	}
	if attemptID != "" {
		item["attempt_id"] = &ddbtypes.AttributeValueMemberS{Value: attemptID}
	}
	if !processingExpires.IsZero() {
		item["processing_expires_unix_ms"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(processingExpires.UnixMilli(), 10)}
	}
	return item
}

func TestMemoryCompletedUploadLedgerBeginCompleteAndDuplicate(t *testing.T) {
	ledger := newMemoryCompletedUploadLedger()
	now := time.Unix(100, 0).UTC()
	msg := CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 7}

	begin, err := ledger.BeginProcessing(context.Background(), msg, now, time.Minute)
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if begin.AlreadyCommitted || begin.Lease.AttemptID == "" {
		t.Fatalf("unexpected begin result: %+v", begin)
	}
	if _, err := ledger.BeginProcessing(context.Background(), msg, now.Add(time.Second), time.Minute); !errors.Is(err, errCompletedUploadProcessingBusy) {
		t.Fatalf("expected busy begin, got %v", err)
	}
	if err := ledger.CompleteProcessing(context.Background(), begin.Lease, now.Add(2*time.Second)); err != nil {
		t.Fatalf("complete failed: %v", err)
	}
	duplicate, err := ledger.BeginProcessing(context.Background(), msg, now.Add(3*time.Second), time.Minute)
	if err != nil {
		t.Fatalf("duplicate begin failed: %v", err)
	}
	if !duplicate.AlreadyCommitted {
		t.Fatalf("expected duplicate to be already committed: %+v", duplicate)
	}
}

func TestMemoryCompletedUploadLedgerExpiredProcessingCanBeReclaimed(t *testing.T) {
	ledger := newMemoryCompletedUploadLedger()
	now := time.Unix(200, 0).UTC()
	msg := CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 8}

	first, err := ledger.BeginProcessing(context.Background(), msg, now, time.Second)
	if err != nil {
		t.Fatalf("first begin failed: %v", err)
	}
	second, err := ledger.BeginProcessing(context.Background(), msg, now.Add(2*time.Second), time.Minute)
	if err != nil {
		t.Fatalf("second begin failed after expiry: %v", err)
	}
	if first.Lease.AttemptID == second.Lease.AttemptID {
		t.Fatalf("expected a new attempt id after expiry")
	}
	if err := ledger.CompleteProcessing(context.Background(), first.Lease, now.Add(3*time.Second)); !errors.Is(err, errCompletedUploadProcessingBusy) {
		t.Fatalf("expected old lease completion to fail, got %v", err)
	}
}

func TestDynamoDBCompletedUploadLedgerBeginUsesConditionalWrite(t *testing.T) {
	fake := &fakeDynamoDBCompletedUploadLedgerClient{}
	ledger, err := NewDynamoDBCompletedUploadLedger(fake, "control")
	if err != nil {
		t.Fatalf("ledger create failed: %v", err)
	}
	msg := CompletedUploadMessage{EpochID: 1, ShardID: 2, Uuid: 9}
	begin, err := ledger.BeginProcessing(context.Background(), msg, time.Unix(300, 0).UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if begin.Lease.ShardID != 2 || begin.Lease.EpochID != 1 || begin.Lease.Uuid != 9 || begin.Lease.AttemptID == "" {
		t.Fatalf("unexpected lease: %+v", begin.Lease)
	}
	if len(fake.updates) != 1 {
		t.Fatalf("expected one update, got %d", len(fake.updates))
	}
	if fake.updates[0].ConditionExpression == nil {
		t.Fatal("expected conditional update")
	}
}

func TestDynamoDBCompletedUploadLedgerBeginClassifiesCommitted(t *testing.T) {
	fake := &fakeDynamoDBCompletedUploadLedgerClient{
		getItem: func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: completedUploadLedgerItem(CompletedUploadLedgerStateCommitted, "attempt", time.Time{})}, nil
		},
	}
	ledger, err := NewDynamoDBCompletedUploadLedger(fake, "control")
	if err != nil {
		t.Fatalf("ledger create failed: %v", err)
	}
	result, err := ledger.BeginProcessing(context.Background(), CompletedUploadMessage{EpochID: 1, ShardID: 0, Uuid: 10}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if !result.AlreadyCommitted {
		t.Fatalf("expected committed result: %+v", result)
	}
	if len(fake.updates) != 0 {
		t.Fatalf("expected no update for committed item, got %d", len(fake.updates))
	}
}

func TestDynamoDBCompletedUploadLedgerCompleteMapsCommittedConditionalFailure(t *testing.T) {
	fake := &fakeDynamoDBCompletedUploadLedgerClient{
		updateItem: func(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return nil, &ddbtypes.ConditionalCheckFailedException{}
		},
		getItem: func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: completedUploadLedgerItem(CompletedUploadLedgerStateCommitted, "attempt", time.Time{})}, nil
		},
	}
	ledger, err := NewDynamoDBCompletedUploadLedger(fake, "control")
	if err != nil {
		t.Fatalf("ledger create failed: %v", err)
	}
	err = ledger.CompleteProcessing(context.Background(), CompletedUploadProcessingLease{ShardID: 0, EpochID: 1, Uuid: 11, AttemptID: "attempt"}, time.Now().UTC())
	if err != nil {
		t.Fatalf("expected committed conditional failure to be idempotent, got %v", err)
	}
}
