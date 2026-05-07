package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	dynamoControlPKName           = "pk"
	dynamoControlLeasePK          = "lease"
	dynamoControlEpochPK          = "epoch"
	dynamoControlShardConfigPK    = "shard-config"
	defaultDynamoOperationTimeout = 5 * time.Second
)

type dynamoDBControlClient interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type dynamoDBControlStore struct {
	client  dynamoDBControlClient
	table   string
	timeout time.Duration
}

func newDynamoDBControlStore(client dynamoDBControlClient, table string) (*dynamoDBControlStore, error) {
	if client == nil {
		return nil, errors.New("dynamodb client is required")
	}
	if table == "" {
		return nil, errors.New("dynamodb control table is required")
	}
	return &dynamoDBControlStore{
		client:  client,
		table:   table,
		timeout: defaultDynamoOperationTimeout,
	}, nil
}

func (s *dynamoDBControlStore) operationContext() (context.Context, context.CancelFunc) {
	return s.operationContextFrom(context.Background())
}

func (s *dynamoDBControlStore) operationContextFrom(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, s.timeout)
}

func dynamoKey(pk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		dynamoControlPKName: &ddbtypes.AttributeValueMemberS{Value: pk},
	}
}

func stringAttr(item map[string]ddbtypes.AttributeValue, name string) string {
	if attr, ok := item[name].(*ddbtypes.AttributeValueMemberS); ok {
		return attr.Value
	}
	return ""
}

func boolAttr(item map[string]ddbtypes.AttributeValue, name string) bool {
	if attr, ok := item[name].(*ddbtypes.AttributeValueMemberBOOL); ok {
		return attr.Value
	}
	return false
}

func int64Attr(item map[string]ddbtypes.AttributeValue, name string) int64 {
	attr, ok := item[name].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return 0
	}
	value, err := strconv.ParseInt(attr.Value, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func numberAttr(value int64) ddbtypes.AttributeValue {
	return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func isConditionalCheckFailed(err error) bool {
	var conditional *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}

func leaseFromItem(item map[string]ddbtypes.AttributeValue) CoordinatorLease {
	return CoordinatorLease{
		Holder:       stringAttr(item, "holder"),
		FencingToken: int64Attr(item, "fencing_token"),
		ExpiresAt:    time.UnixMilli(int64Attr(item, "expires_unix_ms")).UTC(),
	}
}

func (s *dynamoDBControlStore) getItem(pk string) (map[string]ddbtypes.AttributeValue, bool, error) {
	return s.getItemWithContext(context.Background(), pk)
}

func (s *dynamoDBControlStore) getItemWithContext(parent context.Context, pk string) (map[string]ddbtypes.AttributeValue, bool, error) {
	ctx, cancel := s.operationContextFrom(parent)
	defer cancel()
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key:       dynamoKey(pk),
	})
	if err != nil {
		return nil, false, err
	}
	if len(out.Item) == 0 {
		return nil, false, nil
	}
	return out.Item, true, nil
}

func (s *dynamoDBControlStore) AcquireLease(now time.Time, holder string, ttl time.Duration) (CoordinatorLease, error) {
	if holder == "" {
		return CoordinatorLease{}, errors.New("lease holder is required")
	}
	if ttl <= 0 {
		return CoordinatorLease{}, errors.New("lease ttl must be positive")
	}
	if item, ok, err := s.getItem(dynamoControlLeasePK); err != nil {
		return CoordinatorLease{}, err
	} else if ok {
		current := leaseFromItem(item)
		if now.Before(current.ExpiresAt) {
			if current.Holder != holder {
				return CoordinatorLease{}, errLeaseHeld
			}
			return s.RenewLease(now, holder, current.FencingToken, ttl)
		}
	}

	ctx, cancel := s.operationContext()
	defer cancel()
	expiryMs := now.Add(ttl).UnixMilli()
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlLeasePK),
		UpdateExpression:    aws.String("SET holder = :holder, expires_unix_ms = :expires, fencing_token = if_not_exists(fencing_token, :zero) + :one"),
		ConditionExpression: aws.String("attribute_not_exists(pk) OR expires_unix_ms <= :now"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":holder":  &ddbtypes.AttributeValueMemberS{Value: holder},
			":expires": numberAttr(expiryMs),
			":now":     numberAttr(now.UnixMilli()),
			":zero":    numberAttr(0),
			":one":     numberAttr(1),
		},
		ReturnValues: ddbtypes.ReturnValueAllNew,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return CoordinatorLease{}, errLeaseHeld
		}
		return CoordinatorLease{}, err
	}
	return leaseFromItem(out.Attributes), nil
}

func (s *dynamoDBControlStore) RenewLease(now time.Time, holder string, fencingToken int64, ttl time.Duration) (CoordinatorLease, error) {
	if ttl <= 0 {
		return CoordinatorLease{}, errors.New("lease ttl must be positive")
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlLeasePK),
		UpdateExpression:    aws.String("SET expires_unix_ms = :expires"),
		ConditionExpression: aws.String("holder = :holder AND fencing_token = :token AND expires_unix_ms > :now"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":holder":  &ddbtypes.AttributeValueMemberS{Value: holder},
			":token":   numberAttr(fencingToken),
			":expires": numberAttr(now.Add(ttl).UnixMilli()),
			":now":     numberAttr(now.UnixMilli()),
		},
		ReturnValues: ddbtypes.ReturnValueAllNew,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			if _, ok := s.CurrentLease(now); !ok {
				return CoordinatorLease{}, errLeaseNotHeld
			}
			return CoordinatorLease{}, errStaleFence
		}
		return CoordinatorLease{}, err
	}
	return leaseFromItem(out.Attributes), nil
}

func (s *dynamoDBControlStore) CurrentLease(now time.Time) (CoordinatorLease, bool) {
	item, ok, err := s.getItem(dynamoControlLeasePK)
	if err != nil || !ok {
		return CoordinatorLease{}, false
	}
	lease := leaseFromItem(item)
	if !now.Before(lease.ExpiresAt) {
		return CoordinatorLease{}, false
	}
	return lease, true
}

func epochToAttributes(epoch db.EpochMeta, shardConfigVersion int64) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		":epoch_id":             numberAttr(epoch.ID),
		":state":                &ddbtypes.AttributeValueMemberS{Value: epoch.State.String()},
		":start_unix":           numberAttr(epoch.StartTime.Unix()),
		":end_unix":             numberAttr(epoch.EndTime.Unix()),
		":duration_secs":        numberAttr(epoch.DurationSeconds),
		":accepting":            &ddbtypes.AttributeValueMemberBOOL{Value: epoch.State == db.EpochStateActive},
		":shard_config_version": numberAttr(shardConfigVersion),
		":active":               &ddbtypes.AttributeValueMemberS{Value: db.EpochStateActive.String()},
	}
}

func epochFromItem(item map[string]ddbtypes.AttributeValue) db.EpochMeta {
	return db.EpochMeta{
		ID:              int64Attr(item, "epoch_id"),
		State:           parseEpochStateName(stringAttr(item, "state")),
		StartTime:       time.Unix(int64Attr(item, "start_unix"), 0).UTC(),
		EndTime:         time.Unix(int64Attr(item, "end_unix"), 0).UTC(),
		DurationSeconds: int64Attr(item, "duration_secs"),
	}
}

func parseEpochStateName(state string) db.DbState {
	switch state {
	case db.EpochStateActive.String():
		return db.EpochStateActive
	case db.EpochStateClosing.String():
		return db.EpochStateClosing
	case db.EpochStateMerging.String():
		return db.EpochStateMerging
	case db.EpochStateCompleted.String():
		return db.EpochStateCompleted
	default:
		return db.EpochStateNoActive
	}
}

func (s *dynamoDBControlStore) StartEpoch(epoch db.EpochMeta, shardConfigVersion int64) error {
	if epoch.ID <= 0 {
		return errors.New("epoch id must be positive")
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlEpochPK),
		UpdateExpression:    aws.String("SET epoch_id = :epoch_id, #state = :state, start_unix = :start_unix, end_unix = :end_unix, duration_secs = :duration_secs, accepting = :accepting, shard_config_version = :shard_config_version"),
		ConditionExpression: aws.String("attribute_not_exists(pk) OR epoch_id = :epoch_id OR #state <> :active"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state",
		},
		ExpressionAttributeValues: epochToAttributes(epoch, shardConfigVersion),
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return errEpochMismatch
		}
		return err
	}
	return nil
}

func (s *dynamoDBControlStore) CompleteEpoch(epochID int64) (db.EpochMeta, error) {
	ctx, cancel := s.operationContext()
	defer cancel()
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlEpochPK),
		UpdateExpression:    aws.String("SET #state = :completed, accepting = :accepting"),
		ConditionExpression: aws.String("epoch_id = :epoch_id"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":epoch_id":  numberAttr(epochID),
			":completed": &ddbtypes.AttributeValueMemberS{Value: db.EpochStateCompleted.String()},
			":accepting": &ddbtypes.AttributeValueMemberBOOL{Value: false},
		},
		ReturnValues: ddbtypes.ReturnValueAllNew,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return db.EpochMeta{}, errEpochMismatch
		}
		return db.EpochMeta{}, err
	}
	return epochFromItem(out.Attributes), nil
}

func (s *dynamoDBControlStore) CurrentEpoch() (db.EpochMeta, bool) {
	item, ok, err := s.getItem(dynamoControlEpochPK)
	if err != nil || !ok {
		return db.EpochMeta{}, false
	}
	return epochFromItem(item), true
}

func (s *dynamoDBControlStore) SetAccepting(epochID int64, accepting bool) error {
	ctx, cancel := s.operationContext()
	defer cancel()
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlEpochPK),
		UpdateExpression:    aws.String("SET accepting = :accepting"),
		ConditionExpression: aws.String("epoch_id = :epoch_id"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":epoch_id":  numberAttr(epochID),
			":accepting": &ddbtypes.AttributeValueMemberBOOL{Value: accepting},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return errEpochMismatch
		}
		return err
	}
	return nil
}

func (s *dynamoDBControlStore) Accepting(epochID int64) (bool, error) {
	item, ok, err := s.getItem(dynamoControlEpochPK)
	if err != nil {
		return false, err
	}
	if !ok || int64Attr(item, "epoch_id") != epochID {
		return false, errEpochMismatch
	}
	return boolAttr(item, "accepting"), nil
}

func (s *dynamoDBControlStore) ShardConfigVersion() int64 {
	item, ok, err := s.getItem(dynamoControlShardConfigPK)
	if err != nil || !ok {
		return 1
	}
	version := int64Attr(item, "version")
	if version <= 0 {
		return 1
	}
	return version
}

func (s *dynamoDBControlStore) SetShardConfigVersion(version int64) error {
	if version <= 0 {
		return errors.New("shard config version must be positive")
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoControlShardConfigPK),
		UpdateExpression:    aws.String("SET #version = :version"),
		ConditionExpression: aws.String("attribute_not_exists(pk) OR #version <= :version"),
		ExpressionAttributeNames: map[string]string{
			"#version": "version",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":version": numberAttr(version),
		},
	})
	return err
}

func dynamoDBControlStoreConfigError(name string) error {
	return fmt.Errorf("%s is required for dynamodb store", name)
}

func dynamoSessionPK(globalUUID int64) string {
	return fmt.Sprintf("session#%d", globalUUID)
}

func sessionToAttributes(session SessionRecord) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		":epoch_id":        numberAttr(session.EpochID),
		":shard_id":        numberAttr(int64(session.ShardID)),
		":global_uuid":     numberAttr(session.GlobalUUID),
		":local_uuid":      numberAttr(session.LocalUUID),
		":hash_key":        &ddbtypes.AttributeValueMemberS{Value: hex.EncodeToString(session.HashKey[:])},
		":global_row":      numberAttr(int64(session.GlobalRow)),
		":local_row":       numberAttr(int64(session.LocalRow)),
		":shard_start_row": numberAttr(int64(session.ShardStartRow)),
		":created_unix_ms": numberAttr(session.CreatedAt.UnixMilli()),
	}
}

func sessionFromItem(item map[string]ddbtypes.AttributeValue) SessionRecord {
	var hashKey [32]byte
	decoded, err := hex.DecodeString(stringAttr(item, "hash_key"))
	if err == nil {
		copy(hashKey[:], decoded)
	}
	return SessionRecord{
		EpochID:       int64Attr(item, "epoch_id"),
		ShardID:       int(int64Attr(item, "shard_id")),
		GlobalUUID:    int64Attr(item, "global_uuid"),
		LocalUUID:     int64Attr(item, "local_uuid"),
		HashKey:       hashKey,
		GlobalRow:     int(int64Attr(item, "global_row")),
		LocalRow:      int(int64Attr(item, "local_row")),
		ShardStartRow: int(int64Attr(item, "shard_start_row")),
		CreatedAt:     time.UnixMilli(int64Attr(item, "created_unix_ms")).UTC(),
	}
}

func (s *dynamoDBControlStore) PutSession(ctx context.Context, session SessionRecord) error {
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
	opCtx, cancel := s.operationContextFrom(ctx)
	defer cancel()
	_, err := s.client.UpdateItem(opCtx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       dynamoKey(dynamoSessionPK(session.GlobalUUID)),
		UpdateExpression:          aws.String("SET epoch_id = :epoch_id, shard_id = :shard_id, global_uuid = :global_uuid, local_uuid = :local_uuid, hash_key = :hash_key, global_row = :global_row, local_row = :local_row, shard_start_row = :shard_start_row, created_unix_ms = :created_unix_ms"),
		ConditionExpression:       aws.String("attribute_not_exists(pk)"),
		ExpressionAttributeValues: sessionToAttributes(session),
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return errSessionExists
		}
		return err
	}
	return nil
}

func (s *dynamoDBControlStore) GetSession(ctx context.Context, globalUUID int64) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	item, ok, err := s.getItemWithContext(ctx, dynamoSessionPK(globalUUID))
	if err != nil {
		return SessionRecord{}, err
	}
	if !ok {
		return SessionRecord{}, errSessionMissing
	}
	return sessionFromItem(item), nil
}

func (s *dynamoDBControlStore) DeleteSession(ctx context.Context, globalUUID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	opCtx, cancel := s.operationContextFrom(ctx)
	defer cancel()
	_, err := s.client.DeleteItem(opCtx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(dynamoSessionPK(globalUUID)),
		ConditionExpression: aws.String("attribute_exists(pk)"),
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return errSessionMissing
		}
		return err
	}
	return nil
}
