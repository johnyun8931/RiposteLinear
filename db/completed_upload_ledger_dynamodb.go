package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const dynamoCompletedUploadLedgerBackend = "dynamodb"
const dynamoCompletedUploadLedgerPKName = "pk"

type DynamoDBCompletedUploadLedgerClient interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

type dynamoDBCompletedUploadLedger struct {
	client DynamoDBCompletedUploadLedgerClient
	table  string
}

func NewDynamoDBCompletedUploadLedger(client DynamoDBCompletedUploadLedgerClient, table string) (CompletedUploadLedger, error) {
	if client == nil {
		return nil, errors.New("dynamodb completed upload ledger client is required")
	}
	if table == "" {
		return nil, errors.New("dynamodb completed upload ledger table is required")
	}
	return &dynamoDBCompletedUploadLedger{client: client, table: table}, nil
}

func (l *dynamoDBCompletedUploadLedger) Backend() string {
	return dynamoCompletedUploadLedgerBackend
}

func dynamoCompletedUploadLedgerKey(pk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		dynamoCompletedUploadLedgerPKName: &ddbtypes.AttributeValueMemberS{Value: pk},
	}
}

func dynamoCompletedUploadLedgerStringAttr(item map[string]ddbtypes.AttributeValue, name string) string {
	if attr, ok := item[name].(*ddbtypes.AttributeValueMemberS); ok {
		return attr.Value
	}
	return ""
}

func dynamoCompletedUploadLedgerInt64Attr(item map[string]ddbtypes.AttributeValue, name string) int64 {
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

func dynamoCompletedUploadLedgerNumberAttr(value int64) ddbtypes.AttributeValue {
	return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func dynamoCompletedUploadLedgerConditionalFailed(err error) bool {
	var conditional *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}

func (l *dynamoDBCompletedUploadLedger) getRecord(ctx context.Context, key string) (completedUploadLedgerRecord, bool, error) {
	out, err := l.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(l.table),
		Key:       dynamoCompletedUploadLedgerKey(key),
	})
	if err != nil {
		return completedUploadLedgerRecord{}, false, err
	}
	if len(out.Item) == 0 {
		return completedUploadLedgerRecord{}, false, nil
	}
	return completedUploadLedgerRecord{
		State:               dynamoCompletedUploadLedgerStringAttr(out.Item, "state"),
		AttemptID:           dynamoCompletedUploadLedgerStringAttr(out.Item, "attempt_id"),
		ProcessingExpiresAt: time.UnixMilli(dynamoCompletedUploadLedgerInt64Attr(out.Item, "processing_expires_unix_ms")).UTC(),
		CommittedAt:         time.UnixMilli(dynamoCompletedUploadLedgerInt64Attr(out.Item, "committed_unix_ms")).UTC(),
		UpdatedAt:           time.UnixMilli(dynamoCompletedUploadLedgerInt64Attr(out.Item, "updated_unix_ms")).UTC(),
	}, true, nil
}

func classifyCompletedUploadLedgerRecord(record completedUploadLedgerRecord, now time.Time) (CompletedUploadLedgerBeginResult, error) {
	switch record.State {
	case CompletedUploadLedgerStateCommitted:
		return CompletedUploadLedgerBeginResult{AlreadyCommitted: true}, nil
	case CompletedUploadLedgerStateProcessing:
		if now.Before(record.ProcessingExpiresAt) {
			return CompletedUploadLedgerBeginResult{}, errCompletedUploadProcessingBusy
		}
	}
	return CompletedUploadLedgerBeginResult{}, nil
}

func (l *dynamoDBCompletedUploadLedger) BeginProcessing(ctx context.Context, message CompletedUploadMessage, now time.Time, ttl time.Duration) (CompletedUploadLedgerBeginResult, error) {
	if err := ctx.Err(); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	}
	if ttl <= 0 {
		return CompletedUploadLedgerBeginResult{}, errors.New("completed upload processing ttl must be positive")
	}
	if err := validateCompletedUploadLedgerIdentity(message.ShardID, message.EpochID, message.Uuid); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	}
	if err := validateCompletedUploadReplicaID(message.ReplicaID); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	replicaID := message.ReplicaID
	if replicaID == "" {
		replicaID = CompletedUploadReplicaActive
	}
	key := completedUploadLedgerKeyForReplica(replicaID, message.ShardID, message.EpochID, message.Uuid)

	if record, ok, err := l.getRecord(ctx, key); err != nil {
		return CompletedUploadLedgerBeginResult{}, err
	} else if ok {
		if result, err := classifyCompletedUploadLedgerRecord(record, now); err != nil || result.AlreadyCommitted {
			return result, err
		}
	}

	lease := CompletedUploadProcessingLease{
		ReplicaID: replicaID,
		ShardID:   message.ShardID,
		EpochID:   message.EpochID,
		Uuid:      message.Uuid,
		AttemptID: fmt.Sprintf("%d-%d", now.UnixNano(), message.Uuid),
	}
	_, err := l.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(l.table),
		Key:                 dynamoCompletedUploadLedgerKey(key),
		UpdateExpression:    aws.String("SET replica_id = :replica_id, shard_id = :shard_id, epoch_id = :epoch_id, #uuid = :uuid, #state = :processing, attempt_id = :attempt_id, processing_expires_unix_ms = :expires, updated_unix_ms = :updated"),
		ConditionExpression: aws.String("attribute_not_exists(pk) OR (#state <> :committed AND (attribute_not_exists(processing_expires_unix_ms) OR processing_expires_unix_ms <= :now))"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state",
			"#uuid":  "uuid",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":replica_id": &ddbtypes.AttributeValueMemberS{Value: replicaID},
			":shard_id":   dynamoCompletedUploadLedgerNumberAttr(int64(message.ShardID)),
			":epoch_id":   dynamoCompletedUploadLedgerNumberAttr(message.EpochID),
			":uuid":       dynamoCompletedUploadLedgerNumberAttr(message.Uuid),
			":processing": &ddbtypes.AttributeValueMemberS{Value: CompletedUploadLedgerStateProcessing},
			":committed":  &ddbtypes.AttributeValueMemberS{Value: CompletedUploadLedgerStateCommitted},
			":attempt_id": &ddbtypes.AttributeValueMemberS{Value: lease.AttemptID},
			":expires":    dynamoCompletedUploadLedgerNumberAttr(now.Add(ttl).UnixMilli()),
			":updated":    dynamoCompletedUploadLedgerNumberAttr(now.UnixMilli()),
			":now":        dynamoCompletedUploadLedgerNumberAttr(now.UnixMilli()),
		},
	})
	if err != nil {
		if dynamoCompletedUploadLedgerConditionalFailed(err) {
			record, ok, getErr := l.getRecord(ctx, key)
			if getErr != nil {
				return CompletedUploadLedgerBeginResult{}, getErr
			}
			if ok {
				return classifyCompletedUploadLedgerRecord(record, now)
			}
			return CompletedUploadLedgerBeginResult{}, errCompletedUploadProcessingBusy
		}
		return CompletedUploadLedgerBeginResult{}, err
	}
	return CompletedUploadLedgerBeginResult{Lease: lease}, nil
}

func (l *dynamoDBCompletedUploadLedger) CompleteProcessing(ctx context.Context, lease CompletedUploadProcessingLease, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCompletedUploadLedgerIdentity(lease.ShardID, lease.EpochID, lease.Uuid); err != nil {
		return err
	}
	if err := validateCompletedUploadReplicaID(lease.ReplicaID); err != nil {
		return err
	}
	if lease.AttemptID == "" {
		return errors.New("completed upload processing attempt id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	replicaID := lease.ReplicaID
	if replicaID == "" {
		replicaID = CompletedUploadReplicaActive
	}
	key := completedUploadLedgerKeyForReplica(replicaID, lease.ShardID, lease.EpochID, lease.Uuid)
	_, err := l.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(l.table),
		Key:                 dynamoCompletedUploadLedgerKey(key),
		UpdateExpression:    aws.String("SET #state = :committed, committed_unix_ms = :committed_at, updated_unix_ms = :updated REMOVE processing_expires_unix_ms"),
		ConditionExpression: aws.String("#state = :processing AND attempt_id = :attempt_id"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":committed":    &ddbtypes.AttributeValueMemberS{Value: CompletedUploadLedgerStateCommitted},
			":processing":   &ddbtypes.AttributeValueMemberS{Value: CompletedUploadLedgerStateProcessing},
			":attempt_id":   &ddbtypes.AttributeValueMemberS{Value: lease.AttemptID},
			":committed_at": dynamoCompletedUploadLedgerNumberAttr(now.UnixMilli()),
			":updated":      dynamoCompletedUploadLedgerNumberAttr(now.UnixMilli()),
		},
	})
	if err != nil {
		if dynamoCompletedUploadLedgerConditionalFailed(err) {
			record, ok, getErr := l.getRecord(ctx, key)
			if getErr != nil {
				return getErr
			}
			if ok && record.State == CompletedUploadLedgerStateCommitted {
				return nil
			}
			return errCompletedUploadProcessingBusy
		}
		return err
	}
	return nil
}
