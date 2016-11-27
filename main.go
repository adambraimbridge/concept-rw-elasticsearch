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
	"os/signal"
	"syscall"
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
		Value:  "search-concept-search-mvp-k2vkgwhfgjv63nu6jvortpggha.eu-west-1.es.amazonaws.com",
		Desc:   "AES endpoint",
		EnvVar: "ELASTICSEARCH_ENDPOINT",
	})
	esRegion := app.String(cli.StringOpt{
		Name:   "elasticsearch-region",
		Value:  "eu-west-1",
		Desc:   "AES region",
		EnvVar: "ELASTICSEARCH_REGION",
	})

	app.Action = func() {

		accessConfig := amazonAccessConfig{
			accessKey:  *accessKey,
			secretKey:  *secretKey,
			esEndpoint: *esEndpoint,
			esRegion:   *esRegion,
		}

		bulkProcessorConfig := bulkProcessorConfig{
			nrWorkers:     2,
			nrOfRequests:  1000,    // commit if # requests >= 1000
			bulkSize:      2 << 20, // commit if size of requests >= 2 MB
			flushInterval: 30,      // commit every 30s
		}

		elasticWriter, err := NewESWriterService(&accessConfig, &bulkProcessorConfig)
		defer elasticWriter.bulkProcessor.Close()
		if err != nil {
			log.Errorf("Elasticsearch read-writer failed to start: %v\n", err)
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

		// Watch for SIGINT and SIGTERM from the console.
		go func() {
			ch := make(chan os.Signal)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
			<-ch
			log.Infof("Received termination signal. Quitting... \n")
		}()
	}

	log.SetLevel(log.InfoLevel)
	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}
