package health

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/sirupsen/logrus"
)

type HealthService struct {
	esHealthService service.EsService
	authorService   service.AuthorService
}

func NewHealthService(esHealthService service.EsService, authorService service.AuthorService) *HealthService {
	return &HealthService{
		esHealthService: esHealthService,
		authorService:   authorService,
	}
}

func (service *HealthService) HealthCheckHandler() func(http.ResponseWriter, *http.Request) {
	hc := &fthealth.HealthCheck{
		SystemCode:  "up-crwes",
		Name:        "Concept RW Elasticsearch",
		Description: "Concept RW ElasticSearch is an application that writes concepts into Amazon Elasticsearch cluster in batches",
		Checks:      service.checks(true),
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

	checks = append(checks, service.v1AuthorsTransformerConnectivityCheck())

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

func (service *HealthService) v1AuthorsTransformerConnectivityCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-connectivity-to-v1-authors-transformer",
		BusinessImpact:   "Updates that identify FT authors cannot be processed",
		Name:             "Check connectivity to v1-authors-transformer",
		PanicGuide:       "https://dewey.ft.com/up-crwes.html",
		Severity:         1,
		TechnicalSummary: "Cannot connect to the v1 authors transformer",
		Checker:          service.v1AuthorsTransformerConnectivityChecker,
	}
}

func (service *HealthService) v1AuthorsTransformerConnectivityChecker() (string, error) {
	err := service.authorService.IsGTG()
	if err != nil {
		return "Could not connect to v1-authors-transformer", err
	}
	return "Successfully connected to v1-authors-transformer", nil
}

//GoodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (service *HealthService) GoodToGo(writer http.ResponseWriter, req *http.Request) {
	for _, c := range service.checks(false) {
		if _, err := c.Checker(); err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			writer.Write([]byte(fmt.Sprintf("gtg failed for %v, reason: %v", c.ID, err.Error())))
			return
		}
	}
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
