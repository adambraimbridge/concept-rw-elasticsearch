package main

import (
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/olivere/elastic.v2"
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
		Value:  "local",
		Desc:   "AES region",
		EnvVar: "ELASTICSEARCH_REGION",
	})
	indexName := app.String(cli.StringOpt{
		Name:   "index-name",
		Value:  "concept",
		Desc:   "The name of the elaticsearch index",
		EnvVar: "ELASTICSEARCH_INDEX",
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
		Value:  10,
		Desc:   "How frequently should the elasticsearch bulk processor commit requests",
		EnvVar: "ELASTICSEARCH_FLUSH_INTERVAL",
	})

	accessConfig := esAccessConfig{
		accessKey:  *accessKey,
		secretKey:  *secretKey,
		esEndpoint: *esEndpoint,
	}

	bulkProcessorConfig := bulkProcessorConfig{
		nrWorkers:     *nrOfElasticsearchWorkers,
		nrOfRequests:  *nrOfElasticsearchRequests,
		bulkSize:      *elasticsearchBulkSize,
		flushInterval: time.Duration(*elasticsearchFlushInterval) * time.Second,
	}

	app.Action = func() {
		var elasticClient *elastic.Client
		var err error
		if *esRegion == "local" {
			elasticClient, err = newSimpleClient(accessConfig)
		} else {
			elasticClient, err = newAmazonClient(accessConfig)
		}
		if err != nil {
			log.Fatalf("Creating elasticsearch client failed with error=[%v]\n", err)
		}

		bulkProcessor, err := newBulkProcessor(elasticClient, &bulkProcessorConfig)
		if err != nil {
			log.Fatalf("Creating bulk processor failed with error=[%v]\n", err)
		}

		var esService esServiceI = newEsService(elasticClient, *indexName, bulkProcessor)
		conceptWriter := newESWriter(&esService)
		defer (*conceptWriter.elasticService).closeBulkProcessor()

		var esHealthService esHealthServiceI = newEsHealthService(elasticClient)
		healthService := newHealthService(&esHealthService)
		routeRequests(port, conceptWriter, healthService)
	}

	log.SetLevel(log.InfoLevel)
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func routeRequests(port *string, conceptWriter *conceptWriter, healthService *healthService) {

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", conceptWriter.loadBulkData).Methods("PUT")
	servicesRouter.HandleFunc("/{concept-type}/{id}", conceptWriter.loadData).Methods("PUT")
	servicesRouter.HandleFunc("/{concept-type}/{id}", conceptWriter.readData).Methods("GET")
	servicesRouter.HandleFunc("/{concept-type}/{id}", conceptWriter.deleteData).Methods("DELETE")

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	http.HandleFunc("/__health", v1a.Handler("Amazon Elasticsearch Service Healthcheck", "Checks for AES", healthService.connectivityHealthyCheck(), healthService.clusterIsHealthyCheck()))
	http.HandleFunc("/__health-details", healthService.HealthDetails)
	http.HandleFunc("/__gtg", healthService.GoodToGo)

	http.Handle("/", monitoringRouter)

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Unable to start: %v", err)
	}

}
