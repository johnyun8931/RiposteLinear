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

func intAttr(item map[string]ddbtypes.AttributeValue, name string) int {
	return int(int64Attr(item, name))
}

func float64Attr(item map[string]ddbtypes.AttributeValue, name string) float64 {
	attr, ok := item[name].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return 0
	}
	value, err := strconv.ParseFloat(attr.Value, 64)
	if err != nil {
		return 0
	}
	return value
}

func numberAttr(value int64) ddbtypes.AttributeValue {
	return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func floatNumberAttr(value float64) ddbtypes.AttributeValue {
	return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatFloat(value, 'g', -1, 64)}
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
		":shard_config_key":     &ddbtypes.AttributeValueMemberS{Value: epochShardConfigKey(epoch.ID)},
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
		UpdateExpression:    aws.String("SET epoch_id = :epoch_id, #state = :state, start_unix = :start_unix, end_unix = :end_unix, duration_secs = :duration_secs, accepting = :accepting, shard_config_version = :shard_config_version, shard_config_key = :shard_config_key"),
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

func shardConfigToAttributes(config ShardConfigRecord) map[string]ddbtypes.AttributeValue {
	shards := make([]ddbtypes.AttributeValue, 0, len(config.Shards))
	for _, shard := range config.Shards {
		entry := map[string]ddbtypes.AttributeValue{
			"id":                    numberAttr(int64(shard.ID)),
			"start_row":             numberAttr(int64(shard.StartRow)),
			"end_row":               numberAttr(int64(shard.EndRow)),
			"active_leader_addr":    &ddbtypes.AttributeValueMemberS{Value: shard.Active.LeaderAddr},
			"active_follower_addr":  &ddbtypes.AttributeValueMemberS{Value: shard.Active.FollowerAddr},
			"has_standby":           &ddbtypes.AttributeValueMemberBOOL{Value: shard.Standby != nil},
			"standby_leader_addr":   &ddbtypes.AttributeValueMemberS{Value: ""},
			"standby_follower_addr": &ddbtypes.AttributeValueMemberS{Value: ""},
		}
		if shard.Standby != nil {
			entry["standby_leader_addr"] = &ddbtypes.AttributeValueMemberS{Value: shard.Standby.LeaderAddr}
			entry["standby_follower_addr"] = &ddbtypes.AttributeValueMemberS{Value: shard.Standby.FollowerAddr}
		}
		shards = append(shards, &ddbtypes.AttributeValueMemberM{Value: entry})
	}
	return map[string]ddbtypes.AttributeValue{
		":version":             numberAttr(config.Version),
		":shard_count":         numberAttr(int64(config.ShardCount)),
		":rows_per_shard":      numberAttr(int64(config.RowsPerShard)),
		":global_table_height": numberAttr(int64(config.GlobalTableHeight)),
		":shards":              &ddbtypes.AttributeValueMemberL{Value: shards},
	}
}

func shardConfigFromItem(item map[string]ddbtypes.AttributeValue) (ShardConfigRecord, bool) {
	shardList, ok := item["shards"].(*ddbtypes.AttributeValueMemberL)
	if !ok {
		return ShardConfigRecord{}, false
	}
	config := ShardConfigRecord{
		Key:               stringAttr(item, dynamoControlPKName),
		Version:           int64Attr(item, "version"),
		ShardCount:        intAttr(item, "shard_count"),
		RowsPerShard:      intAttr(item, "rows_per_shard"),
		GlobalTableHeight: intAttr(item, "global_table_height"),
		Shards:            make([]ShardConfig, 0, len(shardList.Value)),
	}
	for _, attr := range shardList.Value {
		entryAttr, ok := attr.(*ddbtypes.AttributeValueMemberM)
		if !ok {
			return ShardConfigRecord{}, false
		}
		entry := entryAttr.Value
		shard := ShardConfig{
			ID:       intAttr(entry, "id"),
			StartRow: intAttr(entry, "start_row"),
			EndRow:   intAttr(entry, "end_row"),
			Active: PairConfig{
				LeaderAddr:   stringAttr(entry, "active_leader_addr"),
				FollowerAddr: stringAttr(entry, "active_follower_addr"),
			},
		}
		if boolAttr(entry, "has_standby") {
			shard.Standby = &PairConfig{
				LeaderAddr:   stringAttr(entry, "standby_leader_addr"),
				FollowerAddr: stringAttr(entry, "standby_follower_addr"),
			}
		}
		config.Shards = append(config.Shards, shard)
	}
	return config, true
}

func (s *dynamoDBControlStore) GetShardConfig() (ShardConfigRecord, bool, error) {
	item, ok, err := s.getItem(dynamoControlShardConfigPK)
	if err != nil || !ok {
		return ShardConfigRecord{}, false, err
	}
	config, ok := shardConfigFromItem(item)
	if !ok {
		return ShardConfigRecord{}, false, nil
	}
	if err := validateShardConfigRecord(config); err != nil {
		return ShardConfigRecord{}, false, err
	}
	return config, true, nil
}

func (s *dynamoDBControlStore) PutShardConfig(config ShardConfigRecord) error {
	config.Key = dynamoControlShardConfigPK
	return s.putShardConfigAtKey(dynamoControlShardConfigPK, config, false)
}

func (s *dynamoDBControlStore) GetEpochShardConfig(epochID int64) (ShardConfigRecord, bool, error) {
	item, ok, err := s.getItem(epochShardConfigKey(epochID))
	if err != nil || !ok {
		return ShardConfigRecord{}, false, err
	}
	config, ok := shardConfigFromItem(item)
	if !ok {
		return ShardConfigRecord{}, false, nil
	}
	if err := validateShardConfigRecord(config); err != nil {
		return ShardConfigRecord{}, false, err
	}
	return config, true, nil
}

func (s *dynamoDBControlStore) PutEpochShardConfig(epochID int64, config ShardConfigRecord) error {
	if epochID <= 0 {
		return errors.New("epoch id must be positive")
	}
	config.Key = epochShardConfigKey(epochID)
	return s.putShardConfigAtKey(config.Key, config, true)
}

func (s *dynamoDBControlStore) putShardConfigAtKey(pk string, config ShardConfigRecord, immutable bool) error {
	if err := validateShardConfigRecord(config); err != nil {
		return err
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	condition := "attribute_not_exists(pk) OR #version <= :version"
	if immutable {
		condition = "attribute_not_exists(pk) OR (config_hash = :config_hash)"
	}
	values := shardConfigToAttributes(config)
	values[":config_hash"] = &ddbtypes.AttributeValueMemberS{Value: shardConfigFingerprint(config)}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(pk),
		UpdateExpression:    aws.String("SET #version = :version, shard_count = :shard_count, rows_per_shard = :rows_per_shard, global_table_height = :global_table_height, shards = :shards, config_hash = :config_hash"),
		ConditionExpression: aws.String(condition),
		ExpressionAttributeNames: map[string]string{
			"#version": "version",
		},
		ExpressionAttributeValues: values,
	})
	if err != nil && isConditionalCheckFailed(err) {
		if immutable {
			return errors.New("epoch shard config already exists with different config")
		}
		return errors.New("stale shard config version")
	}
	return err
}

func scalingRecommendationToAttributes(record ScalingRecommendationRecord) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		":epoch_id":                     numberAttr(record.EpochID),
		":accepted_request_count":       numberAttr(record.AcceptedRequestCount),
		":duration_secs":                numberAttr(record.DurationSeconds),
		":current_shard_count":          numberAttr(int64(record.CurrentShardCount)),
		":recommended_shard_count":      numberAttr(int64(record.RecommendedShardCount)),
		":target_rows_per_shard":        numberAttr(int64(record.TargetRowsPerShard)),
		":request_density":              floatNumberAttr(record.RequestDensity),
		":action":                       &ddbtypes.AttributeValueMemberS{Value: record.Action},
		":reason":                       &ddbtypes.AttributeValueMemberS{Value: record.Reason},
		":proposed_global_table_height": numberAttr(int64(record.ProposedGlobalTableHeight)),
		":shard_config_version":         numberAttr(record.ShardConfigVersion),
		":created_unix_ms":              numberAttr(record.CreatedAt.UnixMilli()),
	}
}

func scalingRecommendationFromItem(item map[string]ddbtypes.AttributeValue) ScalingRecommendationRecord {
	return ScalingRecommendationRecord{
		Key:                       stringAttr(item, dynamoControlPKName),
		EpochID:                   int64Attr(item, "epoch_id"),
		AcceptedRequestCount:      int64Attr(item, "accepted_request_count"),
		DurationSeconds:           int64Attr(item, "duration_secs"),
		CurrentShardCount:         intAttr(item, "current_shard_count"),
		RecommendedShardCount:     intAttr(item, "recommended_shard_count"),
		TargetRowsPerShard:        intAttr(item, "target_rows_per_shard"),
		RequestDensity:            float64Attr(item, "request_density"),
		Action:                    stringAttr(item, "action"),
		Reason:                    stringAttr(item, "reason"),
		ProposedGlobalTableHeight: intAttr(item, "proposed_global_table_height"),
		ShardConfigVersion:        int64Attr(item, "shard_config_version"),
		CreatedAt:                 time.UnixMilli(int64Attr(item, "created_unix_ms")).UTC(),
	}
}

func (s *dynamoDBControlStore) PutScalingRecommendation(record ScalingRecommendationRecord) error {
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.Key = epochScalingRecommendationKey(record.EpochID)
	if err := validateScalingRecommendationRecord(record); err != nil {
		return err
	}
	if err := s.putScalingRecommendationAtKey(record.Key, record, true); err != nil {
		return err
	}
	latest := record
	latest.Key = latestScalingRecommendationKey
	return s.putScalingRecommendationAtKey(latestScalingRecommendationKey, latest, false)
}

func (s *dynamoDBControlStore) GetLatestScalingRecommendation() (ScalingRecommendationRecord, bool, error) {
	return s.getScalingRecommendation(latestScalingRecommendationKey)
}

func (s *dynamoDBControlStore) GetEpochScalingRecommendation(epochID int64) (ScalingRecommendationRecord, bool, error) {
	return s.getScalingRecommendation(epochScalingRecommendationKey(epochID))
}

func (s *dynamoDBControlStore) getScalingRecommendation(pk string) (ScalingRecommendationRecord, bool, error) {
	item, ok, err := s.getItem(pk)
	if err != nil || !ok {
		return ScalingRecommendationRecord{}, false, err
	}
	record := scalingRecommendationFromItem(item)
	if err := validateScalingRecommendationRecord(record); err != nil {
		return ScalingRecommendationRecord{}, false, err
	}
	return record, true, nil
}

func (s *dynamoDBControlStore) putScalingRecommendationAtKey(pk string, record ScalingRecommendationRecord, immutable bool) error {
	ctx, cancel := s.operationContext()
	defer cancel()
	condition := "attribute_not_exists(pk) OR epoch_id <= :epoch_id"
	if immutable {
		condition = "attribute_not_exists(pk) OR recommendation_hash = :recommendation_hash"
	}
	values := scalingRecommendationToAttributes(record)
	values[":recommendation_hash"] = &ddbtypes.AttributeValueMemberS{Value: scalingRecommendationFingerprint(record)}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 dynamoKey(pk),
		UpdateExpression:    aws.String("SET epoch_id = :epoch_id, accepted_request_count = :accepted_request_count, duration_secs = :duration_secs, current_shard_count = :current_shard_count, recommended_shard_count = :recommended_shard_count, target_rows_per_shard = :target_rows_per_shard, request_density = :request_density, #action = :action, reason = :reason, proposed_global_table_height = :proposed_global_table_height, shard_config_version = :shard_config_version, created_unix_ms = :created_unix_ms, recommendation_hash = :recommendation_hash"),
		ConditionExpression: aws.String(condition),
		ExpressionAttributeNames: map[string]string{
			"#action": "action",
		},
		ExpressionAttributeValues: values,
	})
	if err != nil && isConditionalCheckFailed(err) {
		if immutable {
			return errors.New("scaling recommendation already exists with different record")
		}
		return nil
	}
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
