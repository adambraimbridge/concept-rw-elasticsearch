package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	log "github.com/sirupsen/logrus"
)

type HealthService struct {
	esHealthService service.EsService
}

func NewHealthService(esHealthService service.EsService) *HealthService {
	return &HealthService{
		esHealthService: esHealthService,
	}
}

func (service *HealthService) HealthCheckHandler() func(http.ResponseWriter, *http.Request) {
	hc := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  "up-crwes",
			Name:        "Concept RW Elasticsearch",
			Description: "Concept RW ElasticSearch is an application that writes concepts into Amazon Elasticsearch cluster in batches",
			Checks:      service.checks(true),
		},
		Timeout: 10 * time.Second,
	}

	return fthealth.Handler(hc)
}

func (service *HealthService) checks(includeReadOnlyCheck bool) []fthealth.Check {
	checks := []fthealth.Check{
		service.esConnectivityHealthyCheck(),
		service.esClusterIsHealthyCheck(),
	}

	if includeReadOnlyCheck {
		checks = append(checks, service.indexIsWriteableCheck())
	}

	return checks
}

func (service *HealthService) esClusterIsHealthyCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-elasticsearch-cluster-health",
		BusinessImpact:   "Full or partial degradation in serving requests from Elasticsearch",
		Name:             "Check Elasticsearch cluster health",
		PanicGuide:       "https://dewey.ft.com/up-crwes.html",
		Severity:         1,
		TechnicalSummary: "Elasticsearch cluster is not healthy. Details on /__health-details",
		Checker:          service.healthChecker,
	}
}

func (service *HealthService) healthChecker() (string, error) {
	output, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		return "Cluster is not healthy: ", err
	} else if output.Status != "green" {
		return "Cluster is not healthy", fmt.Errorf("Cluster is %v", output.Status)
	} else {
		return "Cluster is healthy", nil
	}
}

func (service *HealthService) esConnectivityHealthyCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-connectivity-to-elasticsearch-cluster",
		BusinessImpact:   "Concepts could not be read from or written to Elasticsearch",
		Name:             "Check connectivity to the Elasticsearch cluster",
		PanicGuide:       "https://dewey.ft.com/up-crwes.html",
		Severity:         1,
		TechnicalSummary: "Connection to Elasticsearch cluster could not be created. Please check your AWS credentials.",
		Checker:          service.esConnectivityChecker,
	}
}

func (service *HealthService) esConnectivityChecker() (string, error) {
	_, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		return "Could not connect to elasticsearch", err
	}
	return "Successfully connected to the cluster", nil
}

func (service *HealthService) indexIsWriteableCheck() fthealth.Check {
	return fthealth.Check{
		ID:             "check-elasticsearch-index-writeable",
		BusinessImpact: "Updates to concepts cannot be written to ElasticSearch",
		Name:           "Check index is writeable",
		PanicGuide:     "https://dewey.ft.com/up-crwes.html",
		Severity:       2,
		TechnicalSummary: `Elasticsearch index is locked for writing. 
		This may be because the reindexer is creating a new index version, in which case this service will become healthy 
		once that process is completed. This requires further investigation if there is no ongoing reindexing process.`,
		Checker: service.readOnlyChecker,
	}
}

func (service *HealthService) readOnlyChecker() (string, error) {
	readOnly, indexName, err := service.esHealthService.IsIndexReadOnly()
	if err != nil {
		return "Could not connect to elasticsearch", err
	}

	if readOnly {
		err = fmt.Errorf("Elasticsearch index [%v] is read-only", indexName)
		return err.Error(), err
	}

	return fmt.Sprintf("Elasticsearch index [%v] is writeable", indexName), nil
}

func (service *HealthService) GoodToGo() gtg.Status {
	var statusChecker []gtg.StatusChecker
	for _, c := range service.checks(false) {
		checkFunc := func() gtg.Status {
			return gtgCheck(c.Checker)
		}
		statusChecker = append(statusChecker, checkFunc)
	}
	return gtg.FailFastParallelCheck(statusChecker)()
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}

//HealthDetails returns the response from elasticsearch service /__health endpoint - describing the cluster health
func (service *HealthService) HealthDetails(writer http.ResponseWriter, req *http.Request) {

	writer.Header().Set("Content-Type", "application/json")

	output, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	var response []byte
	response, err = json.Marshal(*output)
	if err != nil {
		response = []byte(err.Error())
	}

	_, err = writer.Write(response)
	if err != nil {
		log.Errorf(err.Error())
	}
}
