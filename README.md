# Concept Read Writer for Elasticsearch 

Small application that writes concepts into amazon elasticsearch in batches.

:warning: The AWS SDK for Go [does not currently include support for ES data plane api](https://github.com/aws/aws-sdk-go/issues/710), but the Signer is exposed since v1.2.0.

The taken approach to access ES:
- Create Transport based on [https://github.com/smartystreets/go-aws-auth](https://github.com/smartystreets/go-aws-auth), using v4 signer.
- Use https://github.com/olivere/elastic library to any ES request, after passing in the above created client

## How to run

```
go get -u github.com/Financial-Times/concept-rw-elasticsearch
go build
./concept-rw-elasticsearch --aws-access-key="{access key}" --aws-secret-access-key="{secret key}"
```
It is also possible to provide the elasticsearch endpoint, region and the port you expect the app to run on.

## Available DATA endpoints:

### -XPUT localhost:8080/{type}/{uuid}

### -XGET localhost:8080/{type}/{uuid}

### -XDELETE localhost:8080/{type}/{uuid}


## Available HEALTH endpoints:

### localhost:8080/__health

Provides the standard FT output indicating the connectivity and the cluster's health.

### localhost:8080/__health-details

Provides a detailed health status of the ES cluster. 
It matches the response from [elasticsearch-endpoint/_cluster/health](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html)