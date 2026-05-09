package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeResultS3Client struct {
	puts []s3.PutObjectInput
	body [][]byte
}

func (c *fakeResultS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.puts = append(c.puts, *input)
	var buf bytes.Buffer
	if input.Body != nil {
		if _, err := buf.ReadFrom(input.Body); err != nil {
			return nil, err
		}
	}
	c.body = append(c.body, buf.Bytes())
	return &s3.PutObjectOutput{}, nil
}

func TestSerializeBitMatrixRowMajor(t *testing.T) {
	var matrix BitMatrix
	matrix[0][0] = 1
	matrix[0][1] = 2
	matrix[1][0] = 3

	got := SerializeBitMatrix(&matrix)
	wantLen := TABLE_HEIGHT * TABLE_WIDTH * SLOT_LENGTH
	if len(got) != wantLen {
		t.Fatalf("expected serialized length %d, got %d", wantLen, len(got))
	}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("expected first row bytes to be first in output, got %d %d", got[0], got[1])
	}
	if got[len(matrix[0])] != 3 {
		t.Fatalf("expected second row byte at offset %d, got %d", len(matrix[0]), got[len(matrix[0])])
	}
}

func TestS3TablePublisherUploadsTableThenManifest(t *testing.T) {
	client := &fakeResultS3Client{}
	publisher, err := NewS3TablePublisher(client, "bucket", "prefix", 3, 512)
	if err != nil {
		t.Fatalf("NewS3TablePublisher failed: %v", err)
	}

	var matrix BitMatrix
	copy(matrix[2][3*SLOT_LENGTH:(3+1)*SLOT_LENGTH], []byte("hello"))
	epoch := EpochMeta{ID: 7}
	publishedAt := time.Unix(160, 0).UTC()
	publication, err := publisher.PublishTable(context.Background(), &matrix, epoch, publishedAt)
	if err != nil {
		t.Fatalf("PublishTable failed: %v", err)
	}

	if len(client.puts) != 2 {
		t.Fatalf("expected two S3 puts, got %d", len(client.puts))
	}
	if got := aws.ToString(client.puts[0].Key); got != "prefix/shards/3/current/table.bin" {
		t.Fatalf("unexpected table key %q", got)
	}
	if got := aws.ToString(client.puts[1].Key); got != "prefix/shards/3/current/manifest.json" {
		t.Fatalf("unexpected manifest key %q", got)
	}
	if publication.TableURI != "s3://bucket/prefix/shards/3/current/table.bin" {
		t.Fatalf("unexpected table uri %q", publication.TableURI)
	}

	var manifest TableManifest
	if err := json.Unmarshal(client.body[1], &manifest); err != nil {
		t.Fatalf("unmarshal manifest failed: %v", err)
	}
	if manifest.EpochID != 7 || manifest.ShardID != 3 || manifest.GlobalStartRow != 512 || manifest.GlobalEndRow != 512+TABLE_HEIGHT {
		t.Fatalf("unexpected manifest identity: %+v", manifest)
	}
	if manifest.ByteLength != TABLE_HEIGHT*TABLE_WIDTH*SLOT_LENGTH {
		t.Fatalf("unexpected manifest byte length %d", manifest.ByteLength)
	}
	sum := sha256.Sum256(client.body[0])
	if manifest.SHA256Hex != hex.EncodeToString(sum[:]) {
		t.Fatalf("manifest sha256 %s does not match table body", manifest.SHA256Hex)
	}
}
