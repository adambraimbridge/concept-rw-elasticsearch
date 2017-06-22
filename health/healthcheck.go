package health

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/Sirupsen/logrus"
)

type HealthService struct {
	esHealthService service.EsHealthServiceI
	authorService   service.AuthorService
}

func NewHealthService(esHealthService service.EsHealthServiceI, authorService service.AuthorService) *HealthService {
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
		Checks:      service.checks(),
	}

	return fthealth.Handler(hc)
}

func (service *HealthService) checks() []fthealth.Check {
	return []fthealth.Check{
		service.esClusterIsHealthyCheck(),
		service.esConnectivityHealthyCheck(),
		service.v1AuthorsTransformerConnectivityCheck(),
	}
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
		BusinessImpact:   "Concepts could not be written to Elasticsearch",
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

func (service *HealthService) v1AuthorsTransformerConnectivityCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-connectivity-to-v1-authors-transformer",
		BusinessImpact:   "It is not possible to identify FT authors in People",
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
	for _, c := range service.checks() {
		if _, err := c.Checker(); err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
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
