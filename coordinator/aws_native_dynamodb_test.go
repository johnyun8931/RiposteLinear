package main

import (
	"context"
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

	updateInputs []*dynamodb.UpdateItemInput
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
