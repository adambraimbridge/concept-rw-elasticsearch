package main

import (
	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v2"
	"time"
)

type bulkProcessorConfig struct {
	nrWorkers     int
	nrOfRequests  int
	bulkSize      int
	flushInterval time.Duration
}

func newBulkProcessor(client *elastic.Client, bulkConfig *bulkProcessorConfig) (*elastic.BulkProcessor, error) {
	return client.BulkProcessor().Name("BackgroundWorker-1").
		Workers(bulkConfig.nrWorkers).
		BulkActions(bulkConfig.nrOfRequests).
		BulkSize(bulkConfig.bulkSize).
		FlushInterval(bulkConfig.flushInterval).
		After(handleBulkFailures).
		Do()
}

func handleBulkFailures(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		// Something went badly wrong, ES reported HTTP status outside [200,300), even after retrying
		log.Errorf("Bulk request failed with error: %v, for the following requests: %v", err, requests)
		return // response is probably nil
	}

	for _, failedItem := range response.Failed() {
		log.Errorf("Concept with uuid %s failed with the following details: %v", failedItem.Id, failedItem)
	}
}
