package db

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3ObjectStore struct {
	client *s3.S3
}

func (s *s3ObjectStore) PutObject(bucket, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return err
}

func (t *Server) SetS3Results(bucket, prefix, region string) error {
	if bucket == "" {
		t.setResultObjectStore("", "", nil)
		return nil
	}

	cfg := aws.NewConfig()
	if region != "" {
		cfg = cfg.WithRegion(region)
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		return err
	}
	t.setResultObjectStore(bucket, prefix, &s3ObjectStore{client: s3.New(sess)})
	return nil
}
