package main

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Financial-Times/concept-rw-elasticsearch/health"
	"github.com/Financial-Times/concept-rw-elasticsearch/resources"
	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

func main() {
	app := cli.App("concept-rw-es", "Service for loading concepts into elasticsearch")
	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "concept-rw-elasticsearch",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	accessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key",
		Desc:   "AWS ACCESS KEY",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	secretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-access-key",
		Desc:   "AWS SECRET ACCESS KEY",
		EnvVar: "AWS_SECRET_ACCESS_KEY",
	})
	esEndpoint := app.String(cli.StringOpt{
		Name:   "elasticsearch-endpoint",
		Value:  "http://localhost:9200",
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
		Value:  "concepts",
		Desc:   "The name of the elasticsearch index",
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

	elasticsearchWhitelistedConceptTypes := app.String(cli.StringOpt{
		Name:   "whitelisted-concepts",
		Value:  "genres,topics,sections,subjects,locations,brands,organisations,people,alphaville-series,memberships",
		Desc:   "List which are currently supported by elasticsearch (already have mapping associated)",
		EnvVar: "ELASTICSEARCH_WHITELISTED_CONCEPTS",
	})

	esTraceLogging := app.Bool(cli.BoolOpt{
		Name:   "elasticsearch-trace",
		Value:  false,
		Desc:   "Whether to log ElasticSearch HTTP requests and responses",
		EnvVar: "ELASTICSEARCH_TRACE",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "info",
		Desc:   "App log level",
		EnvVar: "LOG_LEVEL",
	})

	accessConfig := service.NewAccessConfig(*accessKey, *secretKey, *esEndpoint, *esTraceLogging)

	logger.InitLogger(*appSystemCode, *logLevel)
	logger.Infof("[Startup] The writer handles the following concept types: %v\n", *elasticsearchWhitelistedConceptTypes)

	// It seems that once we have a connection, we can lose and reconnect to Elastic OK
	// so just keep going until successful
	app.Action = func() {
		ecc := make(chan *elastic.Client)
		go func() {
			defer close(ecc)
			for {
				ec, err := service.NewElasticClient(*esRegion, accessConfig)
				if err == nil {
					logger.Info("connected to ElasticSearch")
					ecc <- ec
					return
				}
				logger.Errorf("could not connect to ElasticSearch: %s", err.Error())
				time.Sleep(time.Minute)
			}
		}()

		//create writer service
		bulkProcessorConfig := service.NewBulkProcessorConfig(*nrOfElasticsearchWorkers, *nrOfElasticsearchRequests, *elasticsearchBulkSize, time.Duration(*elasticsearchFlushInterval)*time.Second)

		esService := service.NewEsService(ecc, *indexName, &bulkProcessorConfig)

		allowedConceptTypes := strings.Split(*elasticsearchWhitelistedConceptTypes, ",")
		handler := resources.NewHandler(esService, allowedConceptTypes)
		defer handler.Close()

		//create health service
		healthService := health.NewHealthService(esService)
		routeRequests(port, handler, healthService)
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func routeRequests(port *string, handler *resources.Handler, healthService *health.HealthService) {
	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", handler.LoadBulkData).Methods("PUT")
	servicesRouter.HandleFunc("/{concept-type}/{id}/metrics", handler.LoadMetrics).Methods("PUT")
	servicesRouter.HandleFunc("/{concept-type}/{id}", handler.LoadData).Methods("PUT")
	servicesRouter.HandleFunc("/{concept-type}/{id}", handler.ReadData).Methods("GET")
	servicesRouter.HandleFunc("/{concept-type}/{id}", handler.DeleteData).Methods("DELETE")
	servicesRouter.HandleFunc("/__ids", handler.GetAllIds).Methods("GET")

	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	http.HandleFunc("/__health", healthService.HealthCheckHandler())
	http.HandleFunc("/__health-details", healthService.HealthDetails)
	http.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	http.Handle("/", monitoringRouter)

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		logger.Fatalf("Unable to start: %v", err)
	}
}
