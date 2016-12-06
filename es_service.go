package main

import (
	"gopkg.in/olivere/elastic.v2"
)

type esService struct {
	elasticClient *elastic.Client
	bulkProcessor *elastic.BulkProcessor
	indexName     string
}

type esServiceI interface {
	loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResult, error)
	readData(conceptType string, uuid string) (*elastic.GetResult, error)
	deleteData(conceptType string, uuid string) (*elastic.DeleteResult, error)
	loadBulkData(conceptType string, uuid string, payload interface{})
	closeBulkProcessor() error
}

func newEsService(client *elastic.Client, indexName string, bulkProcessor *elastic.BulkProcessor) *esService {
	return &esService{elasticClient: client, bulkProcessor: bulkProcessor, indexName: indexName}
}

func (service esService) loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResult, error) {
	return service.elasticClient.Index().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do()
}

func (service esService) readData(conceptType string, uuid string) (*elastic.GetResult, error) {
	return service.elasticClient.Get().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		Do()
}

func (service esService) deleteData(conceptType string, uuid string) (*elastic.DeleteResult, error) {
	return service.elasticClient.Delete().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		Do()
}

func (service esService) loadBulkData(conceptType string, uuid string, payload interface{}) {
	r := elastic.NewBulkIndexRequest().Index(service.indexName).Type(conceptType).Id(uuid).Doc(payload)
	service.bulkProcessor.Add(r)
}

func (service esService) closeBulkProcessor() error {
	return service.bulkProcessor.Close()
}
