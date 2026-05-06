package main

import (
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3ObjectReader struct {
	client *s3.S3
}

func (s *s3ObjectReader) GetObject(bucket, key string) ([]byte, error) {
	out, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return ioutil.ReadAll(out.Body)
}

func (c *Coordinator) SetS3Results(bucket, prefix, region string) error {
	if bucket == "" {
		c.setResultObjectReader("", "", nil)
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
	c.setResultObjectReader(bucket, prefix, &s3ObjectReader{client: s3.New(sess)})
	return nil
}
