package main

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"bitbucket.org/henrycg/riposte/db"
)

type fakeDynamoDBControlClient struct {
	getItem    func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	updateItem func(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	deleteItem func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)

	updateInputs []*dynamodb.UpdateItemInput
	deleteInputs []*dynamodb.DeleteItemInput
}

func (f *fakeDynamoDBControlClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.getItem == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	return f.getItem(input)
}

func (f *fakeDynamoDBControlClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.updateInputs = append(f.updateInputs, input)
	if f.updateItem == nil {
		return &dynamodb.UpdateItemOutput{}, nil
	}
	return f.updateItem(input)
}

func (f *fakeDynamoDBControlClient) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.deleteInputs = append(f.deleteInputs, input)
	if f.deleteItem == nil {
		return &dynamodb.DeleteItemOutput{}, nil
	}
	return f.deleteItem(input)
}

func leaseItem(holder string, token int64, expires time.Time) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName: &ddbtypes.AttributeValueMemberS{Value: dynamoControlLeasePK},
		"holder":            &ddbtypes.AttributeValueMemberS{Value: holder},
		"fencing_token":     numberAttr(token),
		"expires_unix_ms":   numberAttr(expires.UnixMilli()),
	}
}

func epochItem(epoch db.EpochMeta, accepting bool) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName: &ddbtypes.AttributeValueMemberS{Value: dynamoControlEpochPK},
		"epoch_id":          numberAttr(epoch.ID),
		"state":             &ddbtypes.AttributeValueMemberS{Value: epoch.State.String()},
		"start_unix":        numberAttr(epoch.StartTime.Unix()),
		"end_unix":          numberAttr(epoch.EndTime.Unix()),
		"duration_secs":     numberAttr(epoch.DurationSeconds),
		"accepting":         &ddbtypes.AttributeValueMemberBOOL{Value: accepting},
	}
}

func sessionItem(session SessionRecord) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName: &ddbtypes.AttributeValueMemberS{Value: dynamoSessionPK(session.GlobalUUID)},
		"epoch_id":          numberAttr(session.EpochID),
		"shard_id":          numberAttr(int64(session.ShardID)),
		"global_uuid":       numberAttr(session.GlobalUUID),
		"local_uuid":        numberAttr(session.LocalUUID),
		"hash_key":          &ddbtypes.AttributeValueMemberS{Value: hex.EncodeToString(session.HashKey[:])},
		"global_row":        numberAttr(int64(session.GlobalRow)),
		"local_row":         numberAttr(int64(session.LocalRow)),
		"shard_start_row":   numberAttr(int64(session.ShardStartRow)),
		"created_unix_ms":   numberAttr(session.CreatedAt.UnixMilli()),
	}
}

func TestDynamoDBControlStoreAcquireLeaseUsesConditionalUpdate(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			if input.ConditionExpression == nil || !strings.Contains(*input.ConditionExpression, "expires_unix_ms <= :now") {
				t.Fatalf("expected conditional expired-lease update, got %v", aws.ToString(input.ConditionExpression))
			}
			if input.UpdateExpression == nil || !strings.Contains(*input.UpdateExpression, "if_not_exists") {
				t.Fatalf("expected fencing token increment expression, got %v", aws.ToString(input.UpdateExpression))
			}
			return &dynamodb.UpdateItemOutput{Attributes: leaseItem("coord-a", 1, now.Add(time.Minute))}, nil
		},
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	lease, err := store.AcquireLease(now, "coord-a", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if lease.Holder != "coord-a" || lease.FencingToken != 1 || !lease.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected lease: %+v", lease)
	}
}

func TestDynamoDBControlStoreAcquireLeaseReturnsHeldForActiveOtherHolder(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	fake := &fakeDynamoDBControlClient{
		getItem: func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: leaseItem("coord-b", 1, now.Add(time.Minute))}, nil
		},
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	_, err = store.AcquireLease(now, "coord-a", time.Minute)
	if !errors.Is(err, errLeaseHeld) {
		t.Fatalf("expected held lease error, got %v", err)
	}
}

func TestDynamoDBControlStoreRenewLeaseMapsConditionalFailure(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	conditionalErr := &ddbtypes.ConditionalCheckFailedException{}
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			if input.ConditionExpression == nil || !strings.Contains(*input.ConditionExpression, "fencing_token = :token") {
				t.Fatalf("expected fenced renew condition, got %v", aws.ToString(input.ConditionExpression))
			}
			return nil, conditionalErr
		},
		getItem: func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: leaseItem("coord-b", 2, now.Add(time.Minute))}, nil
		},
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	_, err = store.RenewLease(now, "coord-a", 1, time.Minute)
	if !errors.Is(err, errStaleFence) {
		t.Fatalf("expected stale fence error, got %v", err)
	}
}

func TestDynamoDBControlStoreEpochOperations(t *testing.T) {
	start := time.Unix(2000, 0).UTC()
	epoch := db.EpochMeta{
		ID:              7,
		State:           db.EpochStateActive,
		StartTime:       start,
		EndTime:         start.Add(time.Hour),
		DurationSeconds: int64(time.Hour / time.Second),
	}
	fake := &fakeDynamoDBControlClient{}
	fake.updateItem = func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if input.ConditionExpression == nil && strings.Contains(aws.ToString(input.UpdateExpression), "epoch_id") {
			t.Fatalf("expected epoch update to be conditional")
		}
		if strings.Contains(aws.ToString(input.UpdateExpression), "#state = :completed") {
			completed := epoch
			completed.State = db.EpochStateCompleted
			return &dynamodb.UpdateItemOutput{Attributes: epochItem(completed, false)}, nil
		}
		return &dynamodb.UpdateItemOutput{}, nil
	}
	fake.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		pk := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value
		if pk == dynamoControlEpochPK {
			return &dynamodb.GetItemOutput{Item: epochItem(epoch, true)}, nil
		}
		if pk == dynamoControlShardConfigPK {
			return &dynamodb.GetItemOutput{Item: map[string]ddbtypes.AttributeValue{
				dynamoControlPKName: &ddbtypes.AttributeValueMemberS{Value: dynamoControlShardConfigPK},
				"version":           numberAttr(3),
			}}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	if err := store.StartEpoch(epoch, 3); err != nil {
		t.Fatalf("StartEpoch failed: %v", err)
	}
	current, ok := store.CurrentEpoch()
	if !ok || current.ID != epoch.ID || current.State != db.EpochStateActive {
		t.Fatalf("unexpected current epoch ok=%t epoch=%+v", ok, current)
	}
	accepting, err := store.Accepting(epoch.ID)
	if err != nil || !accepting {
		t.Fatalf("expected accepting active epoch, accepting=%t err=%v", accepting, err)
	}
	completed, err := store.CompleteEpoch(epoch.ID)
	if err != nil {
		t.Fatalf("CompleteEpoch failed: %v", err)
	}
	if completed.State != db.EpochStateCompleted {
		t.Fatalf("expected completed epoch, got %+v", completed)
	}
	if version := store.ShardConfigVersion(); version != 3 {
		t.Fatalf("expected shard config version 3, got %d", version)
	}
	if err := store.SetAccepting(epoch.ID, false); err != nil {
		t.Fatalf("SetAccepting failed: %v", err)
	}
	if err := store.SetShardConfigVersion(4); err != nil {
		t.Fatalf("SetShardConfigVersion failed: %v", err)
	}
}

func TestDynamoDBControlStoreSessionOperations(t *testing.T) {
	createdAt := time.Unix(3000, 0).UTC()
	var hashKey [32]byte
	hashKey[0] = 8
	session := SessionRecord{
		EpochID:       5,
		ShardID:       1,
		GlobalUUID:    10,
		LocalUUID:     20,
		HashKey:       hashKey,
		GlobalRow:     300,
		LocalRow:      44,
		ShardStartRow: 256,
		CreatedAt:     createdAt,
	}
	fake := &fakeDynamoDBControlClient{}
	fake.updateItem = func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if input.ConditionExpression == nil || aws.ToString(input.ConditionExpression) != "attribute_not_exists(pk)" {
			t.Fatalf("expected session put to be conditional, got %v", aws.ToString(input.ConditionExpression))
		}
		if got := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value; got != dynamoSessionPK(session.GlobalUUID) {
			t.Fatalf("unexpected session key %q", got)
		}
		return &dynamodb.UpdateItemOutput{}, nil
	}
	fake.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if got := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value; got != dynamoSessionPK(session.GlobalUUID) {
			t.Fatalf("unexpected get key %q", got)
		}
		return &dynamodb.GetItemOutput{Item: sessionItem(session)}, nil
	}
	fake.deleteItem = func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		if input.ConditionExpression == nil || aws.ToString(input.ConditionExpression) != "attribute_exists(pk)" {
			t.Fatalf("expected session delete to be conditional, got %v", aws.ToString(input.ConditionExpression))
		}
		if got := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value; got != dynamoSessionPK(session.GlobalUUID) {
			t.Fatalf("unexpected delete key %q", got)
		}
		return &dynamodb.DeleteItemOutput{}, nil
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	if err := store.PutSession(context.Background(), session); err != nil {
		t.Fatalf("PutSession failed: %v", err)
	}
	got, err := store.GetSession(context.Background(), session.GlobalUUID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if got != session {
		t.Fatalf("unexpected session: got %+v want %+v", got, session)
	}
	if err := store.DeleteSession(context.Background(), session.GlobalUUID); err != nil {
		t.Fatalf("DeleteSession failed: %v", err)
	}
}

func TestDynamoDBControlStoreSessionConditionalFailures(t *testing.T) {
	conditionalErr := &ddbtypes.ConditionalCheckFailedException{}
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return nil, conditionalErr
		},
		deleteItem: func(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			return nil, conditionalErr
		},
	}
	store, err := newDynamoDBControlStore(fake, "control")
	if err != nil {
		t.Fatalf("newDynamoDBControlStore failed: %v", err)
	}

	err = store.PutSession(context.Background(), SessionRecord{EpochID: 1, GlobalUUID: 1, LocalUUID: 1})
	if !errors.Is(err, errSessionExists) {
		t.Fatalf("expected session exists error, got %v", err)
	}
	if _, err := store.GetSession(context.Background(), 1); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected missing session error, got %v", err)
	}
	if err := store.DeleteSession(context.Background(), 1); !errors.Is(err, errSessionMissing) {
		t.Fatalf("expected missing session delete error, got %v", err)
	}
}
