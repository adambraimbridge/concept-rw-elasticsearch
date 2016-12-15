# Concept Read Writer for Elasticsearch

[![Circle CI](https://circleci.com/gh/Financial-Times/concept-rw-elasticsearch/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/concept-rw-elasticsearch/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concept-rw-elasticsearch)](https://goreportcard.com/report/github.com/Financial-Times/concept-rw-elasticsearch) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concept-rw-elasticsearch/badge.svg)](https://coveralls.io/github/Financial-Times/concept-rw-elasticsearch)


Writes concepts into Amazon Elasticsearch cluster in batches.

:warning: The AWS SDK for Go [does not currently include support for ES data plane api](https://github.com/aws/aws-sdk-go/issues/710), but the Signer is exposed since v1.2.0.

The taken approach to access AES (Amazon Elasticsearch Service):
- Create Transport based on [https://github.com/smartystreets/go-aws-auth](https://github.com/smartystreets/go-aws-auth), using v4 signer [317](
).
- Use https://github.com/olivere/elastic library to any ES request, after passing in the above created client

If you need to set-up your elasticsearch first, please see some instructions [here](https://github.com/Financial-Times/concept-rw-elasticsearch/blob/master/mapping_readme.md).

## How to run

```
go get -u github.com/Financial-Times/concept-rw-elasticsearch
go build
./concept-rw-elasticsearch --aws-access-key="{access key}" --aws-secret-access-key="{secret key}"
```
It is also possible to provide the elasticsearch endpoint, region and the port you expect the app to run on.

Other parameters:
- elasticsearch-endpoint
- elasticsearch-region (if `local`: the application creates a simple client, without the amazon signing mechanism)
- port
- index-name (defaults to concept)
- bulk-workers
- bulk-requests
- bulk-size
- flush-interval
- whitelisted-concepts - comma separated values with concept types that are supported by this writer. This is important if we don't want to end-up with automatically defined mapping types in our index.

The currently supported concept types are: "genres,topics,sections,subjects,locations,brands,organisations,people".

## Available DATA endpoints:

localhost:8080/{type}/{uuid}

Available types:
`organisations, brands, genres, locations, people, sections, subjects, topics`

### -XPUT localhost:8080/{type}/{uuid}

A successful PUT results in 200. If a request fails it will return a 500 server error response.
Invalid json body input, or uuids that don't match between the path and the body will result in a 400 bad request response.

`curl -XPUT -H "Content-Type: application/json" -H "X-Request-Id: 123" localhost:8080/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8 --data '{"uuid":"2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","type":"PublicCompany","properName":"Apple, Inc.","prefLabel":"Apple, Inc.","legalName":"Apple Inc.","shortName":"Apple","hiddenLabel":"APPLE INC","formerNames":["Apple Computer, Inc."],"aliases":["Apple Inc","Apple Computers","Apple","Apple Canada","Apple Computer","Apple Computer, Inc.","APPLE INC","Apple Incorporated","Apple Computer Inc","Apple Inc.","Apple, Inc."],"industryClassification":"7a01c847-a9bd-33be-b991-c6fbd8871a46","alternativeIdentifiers":{"TME":["TnN0ZWluX09OX0ZvcnR1bmVDb21wYW55X0FBUEw=-T04="],"uuids":["2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","2abff0bd-544d-31c3-899b-fba2f60d53dd"],"factsetIdentifier":"000C7F-E","leiCode":"HWUPKR0MPOU8FGXBT394"}}'`

The only fields which will be saved at this point are: uuid (transformed into id), prefLabel, aliases, type and types(generated from type), the others are ignored.

### -XPUT localhost:8080/bulk/{type}/{uuid}

Requests will be executed in batched, according to the bulk processor's configuration.
If the request was correctly "taken" by the application, it will always return 200.
If the request fails to correctly get written into elasticsearch, the requests will be logged. (Please verify application logs.)

`curl -XPUT -H "Content-Type: application/json" -H "X-Request-Id: 123" localhost:8080/bulk/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8 --data '{"uuid":"2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","type":"PublicCompany","properName":"Apple, Inc.","prefLabel":"Apple, Inc.","legalName":"Apple Inc.","shortName":"Apple","hiddenLabel":"APPLE INC","formerNames":["Apple Computer, Inc."],"aliases":["Apple Inc","Apple Computers","Apple","Apple Canada","Apple Computer","Apple Computer, Inc.","APPLE INC","Apple Incorporated","Apple Computer Inc","Apple Inc.","Apple, Inc."],"industryClassification":"7a01c847-a9bd-33be-b991-c6fbd8871a46","alternativeIdentifiers":{"TME":["TnN0ZWluX09OX0ZvcnR1bmVDb21wYW55X0FBUEw=-T04="],"uuids":["2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","2abff0bd-544d-31c3-899b-fba2f60d53dd"],"factsetIdentifier":"000C7F-E","leiCode":"HWUPKR0MPOU8FGXBT394"}}'`


### -XGET localhost:8080/{type}/{uuid}

The internal read should return what got written. If not found, you'll get a 404 response.

`curl -H "X-Request-Id: 123" localhost:8080/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8`

The following fields should be returned: Id, ApiUrl, PrefLabel, Types, DirectType, Aliases(if exists).

### -XDELETE localhost:8080/{type}/{uuid}
It is not exposed for clients, available only for internal testing.
Will return 204 if successful, 404 if not found.

`curl -XDELETE -H "X-Request-Id: 123" localhost:8080/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8`

## Available HEALTH endpoints:

### localhost:8080/__health

Provides the standard FT output indicating the connectivity and the cluster's health.

### localhost:8080/__health-details

Provides a detailed health status of the ES cluster. 
It matches the response from [elasticsearch-endpoint/_cluster/health](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html)
It returns 503 is the service is currently unavailable, and cannot connect to elasticsearch.

### localhost:8080/__gtg

Return 200 if the application is healthy, 503 Service Unavailable if the app is unhealthy. 


