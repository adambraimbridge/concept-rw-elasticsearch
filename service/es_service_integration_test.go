// +build integration

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger"
	uuid "github.com/satori/go.uuid"
	testLog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v5"
)

const (
	peopleType     = "people"
	membershipType = "memberships"

	esStatusCreated = "created"
)

func init() {
	logger.InitLogger("test-concept-rw-elasticsearch", "error")
}

func TestWrite(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	_, up, resp, err := writeTestDocument(service, organisationsType, testUUID)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")
	assert.True(t, up, "updated was true")
}

func TestWriteMakesPersonAnFTColumnist(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	testUUID := uuid.NewV4().String()
	op, _, _, err := writeTestPersonDocument(service, peopleType, testUUID, "false")
	defer deleteTestDocument(t, service, peopleType, testUUID)

	require.NoError(t, err, "expected successful write")
	ctx := context.Background()
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")

	ftColumnist := &EsMembershipModel{
		Id:             uuid.NewV4().String(),
		PersonId:       testUUID,
		OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
		Memberships:    []string{"7ef75a6a-b6bf-4eb7-a1da-03e0acabef1a", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1b", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
	}
	up, _, err := service.LoadData(newTestContext(), membershipType, ftColumnist.Id, ftColumnist)
	require.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful write")
	assert.True(t, up, "author was updated")

	p, err := service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var actual EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &actual))
	assert.Equal(t, "true", actual.IsFTAuthor)
	assert.Equal(t, op.Id, actual.Id)
	assert.Equal(t, op.ApiUrl, actual.ApiUrl)
	assert.Equal(t, op.PrefLabel, actual.PrefLabel)
}

func TestWriteMakesPersonAnFTJournalist(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	testUUID := uuid.NewV4().String()
	_, _, _, err = writeTestPersonDocument(service, peopleType, testUUID, "false")
	defer deleteTestDocument(t, service, peopleType, testUUID)
	require.NoError(t, err, "expected successful write")
	ctx := context.Background()
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")

	ftColumnist := &EsMembershipModel{
		Id:             uuid.NewV4().String(),
		PersonId:       testUUID,
		OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
		Memberships:    []string{"7ef75a6a-b6bf-4eb7-a1da-03e0acabef1a", "33ee38a4-c677-4952-a141-2ae14da3aedd", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
	}
	up, _, err := service.LoadData(newTestContext(), membershipType, ftColumnist.Id, ftColumnist)
	require.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful write")
	assert.True(t, up, "Journalist updated")

	p, err := service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var actual EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &actual))
	assert.Equal(t, "true", actual.IsFTAuthor)
}

func TestWriteDummyPersonWhenMembershipArrives(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	testUUID := uuid.NewV4().String()
	ctx := context.Background()

	membership := &EsMembershipModel{
		Id:             uuid.NewV4().String(),
		PersonId:       testUUID,
		OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
		Memberships:    []string{"7ef75a6a-b6bf-4eb7-a1da-03e0acabef1a", "33ee38a4-c677-4952-a141-2ae14da3aedd", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
	}
	up, _, err := service.LoadData(newTestContext(), membershipType, membership.Id, membership)
	defer deleteTestDocument(t, service, peopleType, testUUID)

	require.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful write")
	assert.True(t, up, "Journalist updated")
	p, err := service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var actual EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &actual))
	assert.Equal(t, testUUID, actual.Id)
	assert.Equal(t, "true", actual.IsFTAuthor)
	assert.Equal(t, time.Now().Format(time.RFC3339), actual.LastModified)
}

func TestWritePersonAfterMembership(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	testUUID := uuid.NewV4().String()
	ctx := context.Background()

	membership := &EsMembershipModel{
		Id:             uuid.NewV4().String(),
		PersonId:       testUUID,
		OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
		Memberships:    []string{"7ef75a6a-b6bf-4eb7-a1da-03e0acabef1a", "33ee38a4-c677-4952-a141-2ae14da3aedd", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
	}
	up, _, err := service.LoadData(newTestContext(), membershipType, membership.Id, membership)
	require.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful write")
	assert.True(t, up, "Journalist updated")
	op, _, _, err := writeTestDocument(service, peopleType, testUUID)
	defer deleteTestDocument(t, service, peopleType, testUUID)

	require.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	p, err := service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var actual EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &actual))
	assert.Equal(t, op.Id, actual.Id)
	assert.Equal(t, op.ApiUrl, actual.ApiUrl)
	assert.Equal(t, op.PrefLabel, actual.PrefLabel)
	assert.Equal(t, "true", actual.IsFTAuthor)
}

func TestWriteMakesDoesNotMakePersonAnFTAuthor(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	testUUID := uuid.NewV4().String()
	_, _, _, err = writeTestPersonDocument(service, peopleType, testUUID, "false")
	defer deleteTestDocument(t, service, peopleType, testUUID)

	require.NoError(t, err, "expected successful write")
	ctx := context.Background()
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")

	testCases := []struct {
		name  string
		model *EsMembershipModel
	}{
		{
			name: "Not FT org",
			model: &EsMembershipModel{
				Id:             uuid.NewV4().String(),
				PersonId:       testUUID,
				OrganisationId: "7aafe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
				Memberships:    []string{"7ef75a6a-b6bf-4eb7-a1da-03e0acabef1a", "33ee38a4-c677-4952-a141-2ae14da3aedd", "7ef75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
			},
		},
		{
			name: "FT but not a columnist or journalist",
			model: &EsMembershipModel{
				Id:             uuid.NewV4().String(),
				PersonId:       testUUID,
				OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
				Memberships:    []string{"7af75a6a-b6bf-4eb7-a1da-03e0acabef1a", "33aa38a4-c677-4952-a141-2ae14da3aedd", "7af75a6a-b6bf-4eb7-a1da-03e0acabef1c"},
			},
		},
		{
			name: "FT but has no memberships",
			model: &EsMembershipModel{
				Id:             uuid.NewV4().String(),
				PersonId:       testUUID,
				OrganisationId: "7bcfe07b-0fb1-49ce-a5fa-e51d5c01c3e0",
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			up, _, err := service.LoadData(newTestContext(), membershipType, c.model.Id, c.model)
			require.NoError(t, err, "expected successful write")
			_, err = ec.Refresh(indexName).Do(ctx)
			require.NoError(t, err, "expected successful flush")
			err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
			require.NoError(t, err, "require successful write")
			assert.False(t, up, "should not have updated person")

			p, err := service.ReadData(peopleType, testUUID)
			assert.NoError(t, err, "expected successful read")
			var actual EsPersonConceptModel
			assert.NoError(t, json.Unmarshal(*p.Source, &actual))
			assert.Equal(t, "false", actual.IsFTAuthor)
		})
	}
}

func TestWritePreservesPatchableDataForPerson(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	payload, _, _, err := writeTestPersonDocument(service, peopleType, testUUID, "true")
	defer deleteTestDocument(t, service, peopleType, testUUID)

	assert.NoError(t, err, "expected successful write")
	ctx := context.Background()
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	service.PatchUpdateConcept(ctx, peopleType, testUUID, &EsConceptModelPatch{Metrics: &ConceptMetrics{AnnotationsCount: 1234, PrevWeekAnnotationsCount: 123}})
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful metrics write")

	p, err := service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var previous EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &previous))
	assert.Equal(t, "true", previous.IsFTAuthor)

	payload.PrefLabel = "Updated PrefLabel"
	payload.Metrics = nil // blank metrics
	up, _, err := service.LoadData(ctx, peopleType, testUUID, payload)
	require.NoError(t, err, "require successful metrics write")
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful metrics write")
	_, err = ec.Refresh(indexName).Do(ctx)
	require.NoError(t, err, "expected successful flush")
	assert.True(t, up, "person should have been updated")

	p, err = service.ReadData(peopleType, testUUID)
	assert.NoError(t, err, "expected successful read")
	var actual EsPersonConceptModel
	assert.NoError(t, json.Unmarshal(*p.Source, &actual))

	assert.Equal(t, actual.EsConceptModel.Metrics.AnnotationsCount, 1234)
	assert.Equal(t, actual.EsConceptModel.Metrics.PrevWeekAnnotationsCount, 123)

	previous.PrefLabel = payload.PrefLabel
	assert.Equal(t, previous, actual)
}

func TestWritePreservesMetrics(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, 100*time.Millisecond)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	_, _, _, err = writeTestDocument(service, organisationsType, testUUID)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	require.NoError(t, err, "require successful concept write")

	testMetrics := &EsConceptModelPatch{Metrics: &ConceptMetrics{AnnotationsCount: 150000, PrevWeekAnnotationsCount: 15}}
	service.PatchUpdateConcept(newTestContext(), organisationsType, testUUID, testMetrics)
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful metrics write")

	_, _, _, _ = writeTestDocument(service, organisationsType, testUUID)
	err = service.bulkProcessor.Flush() // wait for the bulk processor to write the data
	require.NoError(t, err, "require successful concept update")

	actual, err := service.ReadData(organisationsType, testUUID)
	assert.NoError(t, err, "expected successful concept read")
	m := make(map[string]interface{})
	json.Unmarshal(*actual.Source, &m)

	actualMetrics := m["metrics"].(map[string]interface{})
	actualCount := int(actualMetrics["annotationsCount"].(float64))
	assert.NoError(t, err, "expected concept to contain annotations count")
	assert.Equal(t, 150000, actualCount)

	prevWeekAnnotationsCount := int(actualMetrics["prevWeekAnnotationsCount"].(float64))
	assert.Equal(t, 15, prevWeekAnnotationsCount)
}

func TestIsReadOnly(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}
	defer ec.Stop()
	readOnly, name, err := service.IsIndexReadOnly()
	assert.False(t, readOnly, "index should not be read-only")
	assert.Equal(t, name, indexName, "index name should be returned")
	assert.NoError(t, err, "read-only check should not return an error")

	setReadOnly(t, ec, indexName, true)
	defer setReadOnly(t, ec, indexName, false)

	readOnly, name, err = service.IsIndexReadOnly()
	assert.True(t, readOnly, "index should be read-only")
	assert.Equal(t, name, indexName, "index name should be returned")
	assert.NoError(t, err, "read-only check should not return an error")
}

func TestIsReadOnlyIndexNotFound(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, "foo", nil}
	defer ec.Stop()
	readOnly, name, err := service.IsIndexReadOnly()
	assert.False(t, readOnly, "index should not be read-only")
	assert.Empty(t, name, "no index name should be returned")
	assert.Error(t, err, "index should not be found")
}

func TestRead(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}
	defer ec.Stop()

	testUUID := uuid.NewV4().String()
	payload, _, _, err := writeTestDocument(service, organisationsType, testUUID)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")
	_, err = ec.Refresh(indexName).Do(context.Background())
	require.NoError(t, err, "expected successful flush")

	resp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestPassClientThroughChannel(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ecc := make(chan *elastic.Client)
	defer close(ecc)

	service := NewEsService(ecc, indexName, &bulkProcessorConfig)

	ec := getElasticClient(t, esURL)

	ecc <- ec

	err := waitForClientInjection(service)
	require.NoError(t, err, "ES client injection failed or timed out")

	testUUID := uuid.NewV4().String()
	payload, _, _, err := writeTestDocument(service, organisationsType, testUUID)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)

	assert.Equal(t, fmt.Sprintf("%s/%s/%s", apiBaseURL, organisationsType, testUUID), obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestDelete(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	_, _, resp, err := writeTestDocument(service, organisationsType, testUUID)
	require.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	deleteResp, err := service.DeleteData(newTestContext(), organisationsType, testUUID)
	require.NoError(t, err)
	assert.True(t, deleteResp.Found)

	getResp, err := service.ReadData(organisationsType, testUUID)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)
}

func TestDeleteNotFoundConcept(t *testing.T) {
	hook := testLog.NewGlobal()
	esURL := getElasticSearchTestURL(t)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID := uuid.NewV4().String()
	resp, _ := service.DeleteData(newTestContext(), organisationsType+"s", testUUID)

	assert.False(t, resp.Found, "document is not found")

	assert.Empty(t, hook.AllEntries(), "It logged nothing")
}

func TestCleanup(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID1 := uuid.NewV4().String()
	_, _, resp, err := writeTestDocument(service, organisationsType, testUUID1)
	defer deleteTestDocument(t, service, organisationsType, testUUID1)

	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	testUUID2 := uuid.NewV4().String()
	_, _, resp, err = writeTestDocument(service, peopleType, testUUID2)
	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	testUUID3 := uuid.NewV4().String()
	_, _, resp, err = writeTestDocument(service, organisationsType, testUUID3)
	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	concept := AggregateConceptModel{PrefUUID: testUUID1, SourceRepresentations: []SourceConcept{
		{
			UUID: testUUID1,
		},
		{
			UUID: testUUID2,
		},
		{
			UUID: testUUID3,
		},
	}}

	// ensure test data is immediately available from the index
	_, err = ec.Refresh(indexName).Do(context.Background())
	require.NoError(t, err)

	service.CleanupData(newTestContext(), concept)

	getResp, err := service.ReadData(peopleType, testUUID2)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)

	getResp, err = service.ReadData(organisationsType, testUUID3)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)

	getResp, err = service.ReadData(organisationsType, testUUID1)
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
}

func TestDeprecationFlagTrue(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:           testUUID,
		ApiUrl:       fmt.Sprintf("%s/%s/%s", apiBaseURL, organisationsType, testUUID),
		PrefLabel:    fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:        []string{},
		DirectType:   "",
		Aliases:      []string{},
		IsDeprecated: true,
		LastModified: testLastModified,
	}

	_, resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*readResp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
	assert.Equal(t, true, obj["isDeprecated"], "deprecation flag")
}

func TestDeprecationFlagFalse(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:           testUUID,
		ApiUrl:       fmt.Sprintf("%s/%ss/%s", apiBaseURL, organisationsType, testUUID),
		PrefLabel:    fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:        []string{},
		DirectType:   "",
		Aliases:      []string{},
		LastModified: testLastModified,
	}

	_, resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*readResp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
	_, deprecatedFlagExists := obj["isDeprecated"]
	assert.False(t, deprecatedFlagExists, "deprecation flag")
}

func TestMetricsUpdated(t *testing.T) {
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig}

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:           testUUID,
		ApiUrl:       fmt.Sprintf("%s/%ss/%s", apiBaseURL, organisationsType, testUUID),
		PrefLabel:    fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:        []string{},
		DirectType:   "",
		Aliases:      []string{},
		LastModified: testLastModified,
	}

	_, resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	defer deleteTestDocument(t, service, organisationsType, testUUID)

	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	testMetrics := &EsConceptModelPatch{Metrics: &ConceptMetrics{AnnotationsCount: 15000, PrevWeekAnnotationsCount: 150}}
	service.PatchUpdateConcept(newTestContext(), organisationsType, testUUID, testMetrics)

	service.bulkProcessor.Flush() // wait for the bulk processor to write the data

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	actualModel := EsConceptModel{}
	err = json.Unmarshal(*readResp.Source, &actualModel)

	assert.NoError(t, err)

	assert.Equal(t, payload.ApiUrl, actualModel.ApiUrl, "Expect the original fields to still be intact")
	assert.Equal(t, payload.PrefLabel, actualModel.PrefLabel, "Expect the original fields to still be intact")

	assert.Equal(t, testMetrics.Metrics.AnnotationsCount, actualModel.Metrics.AnnotationsCount, "Count should be set")
	assert.Equal(t, testMetrics.Metrics.PrevWeekAnnotationsCount, actualModel.Metrics.PrevWeekAnnotationsCount, "PrevWeekAnnotationsCount should be set")
}

func TestGetAllIds(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	max := 1001
	expected := make([]string, max)
	workers := 8
	ids := make(chan string, workers)
	var wg sync.WaitGroup
	wg.Add(max)

	for i := 0; i < workers; i++ {
		go func() {
			for id := range ids {
				_, _, _, err := writeTestDocument(service, organisationsType, id)
				require.NoError(t, err, "expected successful write")
				wg.Done()
			}
		}()
	}

	for i := 0; i < max; i++ {
		testUUID := uuid.NewV4().String()
		expected[i] = testUUID
		ids <- testUUID
	}

	close(ids)
	wg.Wait()
	_, err := ec.Refresh(indexName).Do(context.Background())
	require.NoError(t, err, "expected successful flush")

	ch := service.GetAllIds(context.Background())
	actual := make(map[string]struct{})
	for id := range ch {
		actual[id.ID] = struct{}{}
	}

	notFound := 0
	for _, id := range expected {
		_, found := actual[id]
		if !found {
			notFound++
			continue
		}

		deleteTestDocument(t, service, organisationsType, id)
	}
	assert.Equal(t, 0, notFound, "UUIDs not found")
}

func getElasticSearchTestURL(t *testing.T) string {
	if testing.Short() {
		t.Skip("ElasticSearch integration for long tests only.")
	}

	esURL := os.Getenv("ELASTICSEARCH_TEST_URL")
	if strings.TrimSpace(esURL) == "" {
		t.Fatal("Please set the environment variable ELASTICSEARCH_TEST_URL to run ElasticSearch integration tests (e.g. export ELASTICSEARCH_TEST_URL=http://localhost:9200). Alternatively, run `go test -short` to skip them.")
	}

	return esURL
}

func setReadOnly(t *testing.T, client *elastic.Client, indexName string, readOnly bool) {
	indexService := elastic.NewIndicesPutSettingsService(client)

	_, err := indexService.Index(indexName).BodyJson(map[string]interface{}{"index.blocks.write": strconv.FormatBool(readOnly)}).Do(context.Background())

	assert.NoError(t, err, "expected no error for putting index settings")
}

func writeTestPersonDocument(es EsService, conceptType string, uuid string, isFTAuthor string) (EsPersonConceptModel, bool, *elastic.IndexResponse, error) {
	payload := EsPersonConceptModel{
		EsConceptModel: &EsConceptModel{
			Id:           uuid,
			ApiUrl:       fmt.Sprintf("%s/%s/%s", apiBaseURL, conceptType, uuid),
			PrefLabel:    fmt.Sprintf("Test concept %s %s", conceptType, uuid),
			Types:        []string{},
			DirectType:   "",
			Aliases:      []string{},
			LastModified: testLastModified,
		},
		IsFTAuthor: isFTAuthor,
	}

	updated, resp, err := es.LoadData(newTestContext(), conceptType, uuid, payload)
	return payload, updated, resp, err
}

func waitForClientInjection(service EsService) error {
	var err error
	for i := 0; i < 10; i++ {
		_, err = service.GetClusterHealth()
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func deleteTestDocument(t *testing.T, es EsService, conceptType string, uuid string) {
	deleteResp, err := es.DeleteData(newTestContext(), conceptType, uuid)
	require.NoError(t, err)
	assert.True(t, deleteResp.Found)
}