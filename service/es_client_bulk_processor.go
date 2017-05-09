package service

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v3"
	"time"
)

type BulkProcessorConfig struct {
	nrWorkers     int
	nrOfRequests  int
	bulkSize      int
	flushInterval time.Duration
}

func NewBulkProcessorConfig(nrWorkers int, nrOfRequests int, bulkSize int, flushInterval time.Duration) BulkProcessorConfig {
	return BulkProcessorConfig{nrWorkers: nrWorkers, nrOfRequests: nrOfRequests, bulkSize: bulkSize, flushInterval: flushInterval}
}

func newBulkProcessor(client *elastic.Client, bulkConfig *BulkProcessorConfig) (*elastic.BulkProcessor, error) {
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
		errorDetails := fmt.Sprintf("elastic: %s [type=%s] caused by %s, failed shard details: %v", failedItem.Error.Reason, failedItem.Error.Type, failedItem.Error.CausedBy, failedItem.Error.FailedShards)
		log.Errorf("Concept %s with uuid %s failed with status code %d and the following details: %v", failedItem.Type, failedItem.Id, failedItem.Status, errorDetails)
	}
}
