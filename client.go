package main

import (
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/sha1sum/aws_signing_client"
	"gopkg.in/olivere/elastic.v2"
)

func newElasticClient(creds *credentials.Credentials, endpoint *string, region *string) (*elastic.Client, error) {
	signer := v4.NewSigner(creds)
	awsClient, err := aws_signing_client.New(signer, nil, "es", *region)
	if err != nil {
		return nil, err
	}
	return elastic.NewClient(
		elastic.SetURL(*endpoint),
		elastic.SetScheme("https"),
		elastic.SetHttpClient(awsClient),
		elastic.SetSniff(false), //needs to be disabled due to EAS behavior. Healthcheck still operates as normal.
	)
}
