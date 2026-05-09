package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	TableBinaryFileName   = "table.bin"
	TableManifestFileName = "manifest.json"
)

type ResultTableS3Client interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type TableManifest struct {
	EpochID        int64     `json:"epoch_id"`
	ShardID        int       `json:"shard_id"`
	GlobalStartRow int       `json:"global_start_row"`
	GlobalEndRow   int       `json:"global_end_row"`
	TableHeight    int       `json:"table_height"`
	TableWidth     int       `json:"table_width"`
	SlotLength     int       `json:"slot_length"`
	ByteLength     int       `json:"byte_length"`
	SHA256Hex      string    `json:"sha256_hex"`
	TableKey       string    `json:"table_key"`
	ManifestKey    string    `json:"manifest_key"`
	PublishedAt    time.Time `json:"published_at"`
}

type TablePublication struct {
	Manifest    TableManifest
	TableURI    string
	ManifestURI string
}

type TablePublisher interface {
	PublishTable(context.Context, *BitMatrix, EpochMeta, time.Time) (TablePublication, error)
}

type s3TablePublisher struct {
	client         ResultTableS3Client
	bucket         string
	prefix         string
	shardID        int
	globalRowStart int
}

func SerializeBitMatrix(matrix *BitMatrix) []byte {
	if matrix == nil {
		return nil
	}
	out := make([]byte, 0, TABLE_HEIGHT*TABLE_WIDTH*SLOT_LENGTH)
	for row := 0; row < TABLE_HEIGHT; row++ {
		out = append(out, matrix[row][:]...)
	}
	return out
}

func ResultTableKey(prefix string, shardID int) string {
	return cleanS3Key(path.Join(prefix, "shards", fmt.Sprintf("%d", shardID), "current", TableBinaryFileName))
}

func ResultManifestKey(prefix string, shardID int) string {
	return cleanS3Key(path.Join(prefix, "shards", fmt.Sprintf("%d", shardID), "current", TableManifestFileName))
}

func NewS3TablePublisher(client ResultTableS3Client, bucket string, prefix string, shardID int, globalRowStart int) (TablePublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("result table s3 client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("result table s3 bucket is required")
	}
	if shardID < 0 {
		return nil, fmt.Errorf("result table shard id must be non-negative")
	}
	if globalRowStart < 0 {
		return nil, fmt.Errorf("result table global row start must be non-negative")
	}
	return &s3TablePublisher{
		client:         client,
		bucket:         strings.TrimSpace(bucket),
		prefix:         cleanS3Key(prefix),
		shardID:        shardID,
		globalRowStart: globalRowStart,
	}, nil
}

func BuildTableManifest(matrix *BitMatrix, epoch EpochMeta, publishedAt time.Time, shardID int, globalRowStart int, tableKey string, manifestKey string) TableManifest {
	table := SerializeBitMatrix(matrix)
	sum := sha256.Sum256(table)
	return TableManifest{
		EpochID:        epoch.ID,
		ShardID:        shardID,
		GlobalStartRow: globalRowStart,
		GlobalEndRow:   globalRowStart + TABLE_HEIGHT,
		TableHeight:    TABLE_HEIGHT,
		TableWidth:     TABLE_WIDTH,
		SlotLength:     SLOT_LENGTH,
		ByteLength:     len(table),
		SHA256Hex:      hex.EncodeToString(sum[:]),
		TableKey:       tableKey,
		ManifestKey:    manifestKey,
		PublishedAt:    publishedAt.UTC(),
	}
}

func (p *s3TablePublisher) PublishTable(ctx context.Context, matrix *BitMatrix, epoch EpochMeta, publishedAt time.Time) (TablePublication, error) {
	tableKey := ResultTableKey(p.prefix, p.shardID)
	manifestKey := ResultManifestKey(p.prefix, p.shardID)
	table := SerializeBitMatrix(matrix)
	sum := sha256.Sum256(table)
	manifest := TableManifest{
		EpochID:        epoch.ID,
		ShardID:        p.shardID,
		GlobalStartRow: p.globalRowStart,
		GlobalEndRow:   p.globalRowStart + TABLE_HEIGHT,
		TableHeight:    TABLE_HEIGHT,
		TableWidth:     TABLE_WIDTH,
		SlotLength:     SLOT_LENGTH,
		ByteLength:     len(table),
		SHA256Hex:      hex.EncodeToString(sum[:]),
		TableKey:       tableKey,
		ManifestKey:    manifestKey,
		PublishedAt:    publishedAt.UTC(),
	}

	if _, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(p.bucket),
		Key:         aws.String(tableKey),
		Body:        bytes.NewReader(table),
		ContentType: aws.String("application/octet-stream"),
	}); err != nil {
		return TablePublication{}, err
	}

	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return TablePublication{}, err
	}
	manifestData = append(manifestData, '\n')
	if _, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(p.bucket),
		Key:         aws.String(manifestKey),
		Body:        bytes.NewReader(manifestData),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return TablePublication{}, err
	}

	return TablePublication{
		Manifest:    manifest,
		TableURI:    s3URI(p.bucket, tableKey),
		ManifestURI: s3URI(p.bucket, manifestKey),
	}, nil
}

func cleanS3Key(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	if value == "." {
		return ""
	}
	return value
}

func s3URI(bucket string, key string) string {
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}
