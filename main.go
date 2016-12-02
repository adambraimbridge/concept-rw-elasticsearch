package main

import (
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"net/http"
	"os"
	"time"
)

func main() {
	app := cli.App("concept-rw-es", "Service for loading concepts into elasticsearch")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	accessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key",
		Desc:   "AWS ACCES KEY",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	secretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-access-key",
		Desc:   "AWS SECRET ACCES KEY",
		EnvVar: "AWS_SECRET_ACCESS_KEY",
	})
	esEndpoint := app.String(cli.StringOpt{
		Name:   "elasticsearch-endpoint",
		Desc:   "AES endpoint",
		EnvVar: "ELASTICSEARCH_ENDPOINT",
	})
	esRegion := app.String(cli.StringOpt{
		Name:   "elasticsearch-region",
		Value:  "eu-west-1",
		Desc:   "AES region",
		EnvVar: "ELASTICSEARCH_REGION",
	})

	nrOfElasticsearchWorkers := app.Int(cli.IntOpt{
		Name:   "bulk-workers",
		Value:  2,
		Desc:   "Number of workers used in elasticsearch bulk processor",
		EnvVar: "ELASTICSEARCH_WORKERS",
	})

	nrOfElasticsearchRequests := app.Int(cli.IntOpt{
		Name:   "bulk-requests",
		Value:  1000,
		Desc:   "Elasticsearch bulk processor should commit if requests >= 1000 (default)",
		EnvVar: "ELASTICSEARCH_REQUEST_NR",
	})

	elasticsearchBulkSize := app.Int(cli.IntOpt{
		Name:   "bulk-size",
		Value:  2 << 20,
		Desc:   "Elasticsearch bulk processor should commit requests if size of requests >= 2 MB (default)",
		EnvVar: "ELASTICSEARCH_BULK_SIZE",
	})

	elasticsearchFlushInterval := app.Int(cli.IntOpt{
		Name:   "flush-interval",
		Value:  30,
		Desc:   "How frequently should the elasticsearch bulk processor commit requests",
		EnvVar: "ELASTICSEARCH_FLUSH_INTERVAL",
	})

	app.Action = func() {

		accessConfig := esAccessConfig{
			accessKey:  *accessKey,
			secretKey:  *secretKey,
			esEndpoint: *esEndpoint,
			esRegion:   *esRegion,
		}

		bulkProcessorConfig := bulkProcessorConfig{
			nrWorkers:     *nrOfElasticsearchWorkers,
			nrOfRequests:  *nrOfElasticsearchRequests,
			bulkSize:      *elasticsearchBulkSize,
			flushInterval: time.Duration(*elasticsearchFlushInterval) * time.Second,
		}

		elasticWriter, err := NewESWriterService(&accessConfig, &bulkProcessorConfig)
		if err != nil {
			log.Errorf("Elasticsearch read-writer failed to start: %v\n", err)
		}
		if elasticWriter.bulkProcessor != nil {
			defer elasticWriter.bulkProcessor.Close()
		}

		servicesRouter := mux.NewRouter()
		servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", elasticWriter.loadBulkData).Methods("PUT")
		servicesRouter.HandleFunc("/{concept-type}/{id}", elasticWriter.loadData).Methods("PUT")
		servicesRouter.HandleFunc("/{concept-type}/{id}", elasticWriter.readData).Methods("GET")
		servicesRouter.HandleFunc("/{concept-type}/{id}", elasticWriter.deleteData).Methods("DELETE")

		var monitoringRouter http.Handler = servicesRouter
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
		monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

		http.HandleFunc("/__health", v1a.Handler("Amazon Elasticsearch Service Healthcheck", "Checks for AES", elasticWriter.connectivityHealthyCheck(), elasticWriter.clusterIsHealthyCheck()))
		http.HandleFunc("/__health-details", elasticWriter.HealthDetails)
		http.HandleFunc("/__gtg", elasticWriter.GoodToGo)

		http.Handle("/", monitoringRouter)

		if err := http.ListenAndServe(":"+*port, nil); err != nil {
			log.Fatalf("Unable to start: %v", err)
		}
	}

	log.SetLevel(log.InfoLevel)
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}
