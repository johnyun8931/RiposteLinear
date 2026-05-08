package controlstore

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

func shardConfigItem(config ShardConfigRecord) map[string]ddbtypes.AttributeValue {
	values := shardConfigToAttributes(config)
	key := config.Key
	if key == "" {
		key = dynamoControlShardConfigPK
	}
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName:   &ddbtypes.AttributeValueMemberS{Value: key},
		"version":             values[":version"],
		"shard_count":         values[":shard_count"],
		"rows_per_shard":      values[":rows_per_shard"],
		"global_table_height": values[":global_table_height"],
		"shards":              values[":shards"],
	}
}

func scalingRecommendationItem(record ScalingRecommendationRecord) map[string]ddbtypes.AttributeValue {
	values := scalingRecommendationToAttributes(record)
	key := record.Key
	if key == "" {
		key = epochScalingRecommendationKey(record.EpochID)
	}
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName:            &ddbtypes.AttributeValueMemberS{Value: key},
		"epoch_id":                     values[":epoch_id"],
		"accepted_request_count":       values[":accepted_request_count"],
		"duration_secs":                values[":duration_secs"],
		"current_shard_count":          values[":current_shard_count"],
		"recommended_shard_count":      values[":recommended_shard_count"],
		"target_rows_per_shard":        values[":target_rows_per_shard"],
		"request_density":              values[":request_density"],
		"action":                       values[":action"],
		"reason":                       values[":reason"],
		"proposed_global_table_height": values[":proposed_global_table_height"],
		"shard_config_version":         values[":shard_config_version"],
		"created_unix_ms":              values[":created_unix_ms"],
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
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
		if strings.Contains(aws.ToString(input.UpdateExpression), "epoch_id") {
			if input.ExpressionAttributeValues[":shard_config_version"] == nil {
				t.Fatal("expected epoch record to include shard config version")
			}
			keyAttr, ok := input.ExpressionAttributeValues[":shard_config_key"].(*ddbtypes.AttributeValueMemberS)
			if !ok || keyAttr.Value != epochShardConfigKey(epoch.ID) {
				t.Fatalf("expected epoch shard config key %q, got %#v", epochShardConfigKey(epoch.ID), input.ExpressionAttributeValues[":shard_config_key"])
			}
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
			return &dynamodb.GetItemOutput{Item: shardConfigItem(shardConfigRecordFromShards([]ShardConfig{
				activeOnlyShard(0, 0, db.TABLE_HEIGHT),
			}, 3))}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
	config, ok, err := store.GetShardConfig()
	if err != nil || !ok || config.Version != 3 || config.ShardCount != 1 {
		t.Fatalf("expected shard config version 3 count 1, ok=%t err=%v config=%+v", ok, err, config)
	}
	if err := store.SetAccepting(epoch.ID, false); err != nil {
		t.Fatalf("SetAccepting failed: %v", err)
	}
	if err := store.PutShardConfig(shardConfigRecordFromShards([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, 4)); err != nil {
		t.Fatalf("PutShardConfig failed: %v", err)
	}
}

func TestDynamoDBControlStoreEpochShardConfigSnapshot(t *testing.T) {
	config := shardConfigRecordFromShards([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, 3)
	fake := &fakeDynamoDBControlClient{}
	fake.updateItem = func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if got := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value; got != epochShardConfigKey(7) {
			t.Fatalf("unexpected snapshot key %q", got)
		}
		if input.ConditionExpression == nil || !strings.Contains(aws.ToString(input.ConditionExpression), "config_hash = :config_hash") {
			t.Fatalf("expected immutable snapshot condition, got %v", aws.ToString(input.ConditionExpression))
		}
		if input.ExpressionAttributeValues[":config_hash"] == nil {
			t.Fatal("expected config hash attribute")
		}
		return &dynamodb.UpdateItemOutput{}, nil
	}
	fake.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if got := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value; got != epochShardConfigKey(7) {
			t.Fatalf("unexpected get key %q", got)
		}
		return &dynamodb.GetItemOutput{Item: shardConfigItem(epochShardConfigRecord(config, 7))}, nil
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
	}

	if err := store.PutEpochShardConfig(7, epochShardConfigRecord(config, 7)); err != nil {
		t.Fatalf("PutEpochShardConfig failed: %v", err)
	}
	got, ok, err := store.GetEpochShardConfig(7)
	if err != nil || !ok || !shardConfigRecordsEqual(got, epochShardConfigRecord(config, 7)) {
		t.Fatalf("unexpected epoch shard config ok=%t err=%v config=%+v", ok, err, got)
	}
}

func TestDynamoDBControlStoreEpochShardConfigRejectsDifferentRewrite(t *testing.T) {
	conditionalErr := &ddbtypes.ConditionalCheckFailedException{}
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return nil, conditionalErr
		},
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
	}

	err = store.PutEpochShardConfig(7, epochShardConfigRecord(shardConfigRecordFromShards([]ShardConfig{
		activeOnlyShard(0, 0, db.TABLE_HEIGHT),
	}, 3), 7))
	if err == nil || !strings.Contains(err.Error(), "different config") {
		t.Fatalf("expected different config error, got %v", err)
	}
}

func TestDynamoDBControlStoreScalingRecommendation(t *testing.T) {
	created := time.Unix(4000, 0).UTC()
	record := scalingRecommendationRecord(
		testEpochScalingMetrics{EpochID: 7, CurrentShardCount: 2, AcceptedRequestCount: 2048, DurationSeconds: 60},
		testScalingRecommendation{RecommendedShardCount: 4, TargetRowsPerShard: db.TABLE_HEIGHT, RequestDensity: 4, Action: scalingActionGrow, Reason: "grow"},
		5,
		created,
	)
	fake := &fakeDynamoDBControlClient{}
	fake.updateItem = func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		pk := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value
		switch pk {
		case epochScalingRecommendationKey(7):
			if input.ConditionExpression == nil || !strings.Contains(aws.ToString(input.ConditionExpression), "recommendation_hash = :recommendation_hash") {
				t.Fatalf("expected immutable recommendation condition, got %v", aws.ToString(input.ConditionExpression))
			}
		case latestScalingRecommendationKey:
			if input.ConditionExpression == nil || !strings.Contains(aws.ToString(input.ConditionExpression), "epoch_id <= :epoch_id") {
				t.Fatalf("expected latest recommendation epoch condition, got %v", aws.ToString(input.ConditionExpression))
			}
		default:
			t.Fatalf("unexpected scaling recommendation key %q", pk)
		}
		if input.ExpressionAttributeValues[":recommendation_hash"] == nil {
			t.Fatal("expected recommendation hash attribute")
		}
		return &dynamodb.UpdateItemOutput{}, nil
	}
	fake.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		pk := input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value
		item := scalingRecommendationItem(record)
		item[dynamoControlPKName] = &ddbtypes.AttributeValueMemberS{Value: pk}
		return &dynamodb.GetItemOutput{Item: item}, nil
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
	}

	if err := store.PutScalingRecommendation(record); err != nil {
		t.Fatalf("PutScalingRecommendation failed: %v", err)
	}
	if len(fake.updateInputs) != 2 {
		t.Fatalf("expected epoch and latest updates, got %d", len(fake.updateInputs))
	}
	got, ok, err := store.GetEpochScalingRecommendation(7)
	if err != nil || !ok || !scalingRecommendationRecordsEqual(got, record) {
		t.Fatalf("unexpected epoch scaling recommendation ok=%t err=%v record=%+v", ok, err, got)
	}
	latest, ok, err := store.GetLatestScalingRecommendation()
	wantLatest := record
	wantLatest.Key = latestScalingRecommendationKey
	if err != nil || !ok || !scalingRecommendationRecordsEqual(latest, wantLatest) {
		t.Fatalf("unexpected latest scaling recommendation ok=%t err=%v record=%+v", ok, err, latest)
	}
}

func TestDynamoDBControlStoreScalingRecommendationRejectsDifferentRewrite(t *testing.T) {
	conditionalErr := &ddbtypes.ConditionalCheckFailedException{}
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			if input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value == epochScalingRecommendationKey(7) {
				return nil, conditionalErr
			}
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
	}
	record := scalingRecommendationRecord(
		testEpochScalingMetrics{EpochID: 7, CurrentShardCount: 1, AcceptedRequestCount: 8, DurationSeconds: 60},
		testScalingRecommendation{RecommendedShardCount: 2, TargetRowsPerShard: 2, RequestDensity: 4, Action: scalingActionGrow, Reason: "grow"},
		1,
		time.Unix(4000, 0).UTC(),
	)

	err = store.PutScalingRecommendation(record)
	if err == nil || !strings.Contains(err.Error(), "different record") {
		t.Fatalf("expected different record error, got %v", err)
	}
}

func TestDynamoDBControlStoreScalingRecommendationIgnoresOlderLatest(t *testing.T) {
	conditionalErr := &ddbtypes.ConditionalCheckFailedException{}
	fake := &fakeDynamoDBControlClient{
		updateItem: func(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			if input.Key[dynamoControlPKName].(*ddbtypes.AttributeValueMemberS).Value == latestScalingRecommendationKey {
				return nil, conditionalErr
			}
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
	}
	record := scalingRecommendationRecord(
		testEpochScalingMetrics{EpochID: 7, CurrentShardCount: 1, AcceptedRequestCount: 8, DurationSeconds: 60},
		testScalingRecommendation{RecommendedShardCount: 2, TargetRowsPerShard: 2, RequestDensity: 4, Action: scalingActionGrow, Reason: "grow"},
		1,
		time.Unix(4000, 0).UTC(),
	)

	if err := store.PutScalingRecommendation(record); err != nil {
		t.Fatalf("expected stale latest update to be ignored, got %v", err)
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
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
	store, err := NewDynamoDBStore(fake, "control")
	if err != nil {
		t.Fatalf("NewDynamoDBStore failed: %v", err)
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
