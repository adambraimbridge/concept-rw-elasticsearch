package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testAggregateConceptModelJSON = `{"prefUUID":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`

var testConceptModelJSON = `{"uuid":"2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","type":"PublicCompany","properName":"Apple, Inc.","prefLabel":"Apple, Inc.","legalName":"Apple Inc.","shortName":"Apple","hiddenLabel":"APPLE INC","alternativeIdentifiers":{"TME":["TnN0ZWluX09OX0ZvcnR1bmVDb21wYW55X0FBUEw=-T04="],"uuids":["2384fa7a-d514-3d6a-a0ea-3a711f66d0d8","2abff0bd-544d-31c3-899b-fba2f60d53dd"],"factsetIdentifier":"000C7F-E","leiCode":"HWUPKR0MPOU8FGXBT394"},"formerNames":["Apple Computer, Inc."],"aliases":["Apple Inc","Apple Computers","Apple","Apple Canada","Apple Computer","Apple Computer, Inc.","APPLE INC","Apple Incorporated","Apple Computer Inc","Apple Inc.","Apple, Inc."],"industryClassification":"7a01c847-a9bd-33be-b991-c6fbd8871a46"}`

func newTestModelPopulator() ModelPopulator {
	testAuthorService := curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: expectedAuthorUUIDs,
		authorLock:  &sync.RWMutex{},
	}
	return NewEsModelPopulator(&testAuthorService)
}

func TestConvertToESConceptModel(t *testing.T) {
	testModelPopulator := newTestModelPopulator()

	tests := []struct {
		conceptModel   ConceptModel
		esConceptModel EsConceptModel
	}{
		{
			ConceptModel{
				UUID:       "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				DirectType: "PublicCompany",
				PrefLabel:  "Apple, Inc.",
				AlternativeIdentifiers: map[string]interface{}{
					"Factset": "789",
					"TME":     []string{"123", "456"},
					"uuids":   []string{"uuid"},
				},
				Aliases: []string{"Apple Inc", "Apple Computers",
					"Apple",
					"Apple Canada",
					"Apple Computer",
					"Apple Computer, Inc.",
					"APPLE INC",
					"Apple Incorporated",
					"Apple Computer Inc",
					"Apple Inc.",
					"Apple, Inc."},
			},
			EsConceptModel{
				Id:        "http://api.ft.com/things/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				ApiUrl:    "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				PrefLabel: "Apple, Inc.",
				Types: []string{
					"http://www.ft.com/ontology/core/Thing",
					"http://www.ft.com/ontology/concept/Concept",
					"http://www.ft.com/ontology/organisation/Organisation",
					"http://www.ft.com/ontology/company/Company",
					"http://www.ft.com/ontology/company/PublicCompany",
				},
				Authorities: []string{"Factset", "TME"},
				DirectType:  "http://www.ft.com/ontology/company/PublicCompany",
				Aliases: []string{
					"Apple Inc",
					"Apple Computers",
					"Apple",
					"Apple Canada",
					"Apple Computer",
					"Apple Computer, Inc.",
					"APPLE INC",
					"Apple Incorporated",
					"Apple Computer Inc",
					"Apple Inc.",
					"Apple, Inc.",
				},
			},
		},
		{
			ConceptModel{
				UUID:       "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				DirectType: "PublicCompany",
				PrefLabel:  "Apple, Inc.",
				Aliases:    []string{},
			},
			EsConceptModel{
				Id:        "http://api.ft.com/things/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				ApiUrl:    "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				PrefLabel: "Apple, Inc.",
				Types: []string{
					"http://www.ft.com/ontology/core/Thing",
					"http://www.ft.com/ontology/concept/Concept",
					"http://www.ft.com/ontology/organisation/Organisation",
					"http://www.ft.com/ontology/company/Company",
					"http://www.ft.com/ontology/company/PublicCompany",
				},
				DirectType: "http://www.ft.com/ontology/company/PublicCompany",
				Aliases:    []string{},
			},
		},
	}

	for _, testModel := range tests {
		esModel := testModelPopulator.ConvertConceptToESConceptModel(testModel.conceptModel, "organisations").(EsConceptModel)
		assert.Equal(t, testModel.esConceptModel.Id, esModel.Id, fmt.Sprintf("Expected Id %s differs from actual id %s ", testModel.esConceptModel.Id, esModel.Id))
		assert.Equal(t, testModel.esConceptModel.ApiUrl, esModel.ApiUrl, fmt.Sprintf("Expected ApiUrl %s differs from actual ApiUrl %s ", testModel.esConceptModel.ApiUrl, esModel.ApiUrl))
		assert.Equal(t, testModel.esConceptModel.DirectType, esModel.DirectType, fmt.Sprintf("Expected DirectType %s differs from actual DirectType %s ", testModel.esConceptModel.DirectType, esModel.DirectType))
		assert.Equal(t, testModel.esConceptModel.PrefLabel, esModel.PrefLabel, fmt.Sprintf("Expected PrefLabel %s differs from actual PrefLabel %s ", testModel.esConceptModel.PrefLabel, esModel.PrefLabel))
		assert.Equal(t, testModel.esConceptModel.Types, esModel.Types, fmt.Sprintf("Expected Types %s differ from actual Types %s ", testModel.esConceptModel.Types, esModel.Types))
		assert.Equal(t, testModel.esConceptModel.Aliases, esModel.Aliases, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esConceptModel.Aliases, esModel.Aliases))
		assert.Subset(t, testModel.esConceptModel.Authorities, esModel.Authorities, fmt.Sprintf("Expected Authorities %s differ from actual Authorities %s ", testModel.esConceptModel.Authorities, esModel.Authorities))
	}
}

func TestConvertAggregateConceptToESConceptModel(t *testing.T) {
	testModelPopulator := newTestModelPopulator()

	tests := []struct {
		conceptModel   AggregateConceptModel
		esConceptModel EsConceptModel
	}{
		{
			AggregateConceptModel{
				PrefUUID:   "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				DirectType: "PublicCompany",
				PrefLabel:  "Apple, Inc.",
				Aliases: []string{
					"Apple Inc",
					"Apple Computers",
					"Apple",
					"Apple Canada",
					"Apple Computer",
					"Apple Computer, Inc.",
					"APPLE INC",
					"Apple Incorporated",
					"Apple Computer Inc",
					"Apple Inc.",
					"Apple, Inc.",
				},
				SourceRepresentations: []SourceConcept{
					{
						UUID:      "xyz",
						Authority: "TME",
					},
					{
						UUID:      "abc",
						Authority: "Factset",
					},
				},
			},
			EsConceptModel{
				Id:        "http://api.ft.com/things/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				ApiUrl:    "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				PrefLabel: "Apple, Inc.",
				Types: []string{
					"http://www.ft.com/ontology/core/Thing",
					"http://www.ft.com/ontology/concept/Concept",
					"http://www.ft.com/ontology/organisation/Organisation",
					"http://www.ft.com/ontology/company/Company",
					"http://www.ft.com/ontology/company/PublicCompany",
				},
				Authorities: []string{"TME", "Factset"},
				DirectType:  "http://www.ft.com/ontology/company/PublicCompany",
				Aliases: []string{
					"Apple Inc",
					"Apple Computers",
					"Apple",
					"Apple Canada",
					"Apple Computer",
					"Apple Computer, Inc.",
					"APPLE INC",
					"Apple Incorporated",
					"Apple Computer Inc",
					"Apple Inc.",
					"Apple, Inc.",
				},
			},
		},
		{
			AggregateConceptModel{
				PrefUUID:   "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				DirectType: "PublicCompany",
				PrefLabel:  "Apple, Inc.",
				Aliases:    []string{},
			},
			EsConceptModel{
				Id:        "http://api.ft.com/things/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				ApiUrl:    "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				PrefLabel: "Apple, Inc.",
				Types: []string{
					"http://www.ft.com/ontology/core/Thing",
					"http://www.ft.com/ontology/concept/Concept",
					"http://www.ft.com/ontology/organisation/Organisation",
					"http://www.ft.com/ontology/company/Company",
					"http://www.ft.com/ontology/company/PublicCompany",
				},
				DirectType: "http://www.ft.com/ontology/company/PublicCompany",
				Aliases:    []string{},
			},
		},
	}

	for _, testModel := range tests {
		esModel := testModelPopulator.ConvertAggregateConceptToESConceptModel(testModel.conceptModel, "organisations").(EsConceptModel)
		assert.Equal(t, testModel.esConceptModel.Id, esModel.Id, fmt.Sprintf("Expected Id %s differs from actual id %s ", testModel.esConceptModel.Id, esModel.Id))
		assert.Equal(t, testModel.esConceptModel.ApiUrl, esModel.ApiUrl, fmt.Sprintf("Expected ApiUrl %s differs from actual ApiUrl %s ", testModel.esConceptModel.ApiUrl, esModel.ApiUrl))
		assert.Equal(t, testModel.esConceptModel.DirectType, esModel.DirectType, fmt.Sprintf("Expected DirectType %s differs from actual DirectType %s ", testModel.esConceptModel.DirectType, esModel.DirectType))
		assert.Equal(t, testModel.esConceptModel.PrefLabel, esModel.PrefLabel, fmt.Sprintf("Expected PrefLabel %s differs from actual PrefLabel %s ", testModel.esConceptModel.PrefLabel, esModel.PrefLabel))
		assert.Equal(t, testModel.esConceptModel.Types, esModel.Types, fmt.Sprintf("Expected Types %s differ from actual Types %s ", testModel.esConceptModel.Types, esModel.Types))
		assert.Equal(t, testModel.esConceptModel.Aliases, esModel.Aliases, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esConceptModel.Aliases, esModel.Aliases))
		assert.Subset(t, testModel.esConceptModel.Authorities, esModel.Authorities, fmt.Sprintf("Expected Authorities %s differ from actual Authorities %s ", testModel.esConceptModel.Authorities, esModel.Authorities))
	}
}

func TestConceptFuncsForConceptModel(t *testing.T) {
	concept := ConceptModel{}
	err := json.Unmarshal([]byte(testConceptModelJSON), &concept)
	require.NoError(t, err)

	expected := []string{"TME", "factsetIdentifier", "leiCode"}
	actual := concept.GetAuthorities()

	assert.Len(t, actual, 3)
	for _, val := range expected {
		assert.Contains(t, actual, val)
	}

	expected = []string{}
	actual = concept.ConcordedUUIDs()
	assert.Equal(t, expected, actual)
	assert.Equal(t, "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8", concept.PreferredUUID())
}

func TestConceptFuncsForAggregatedConceptModel(t *testing.T) {
	concept := AggregateConceptModel{}
	err := json.Unmarshal([]byte(testAggregateConceptModelJSON), &concept)
	require.NoError(t, err)

	expected := []string{"TME", "Smartlogic"}
	actual := concept.GetAuthorities()

	assert.Len(t, actual, 2)
	for _, val := range expected {
		assert.Contains(t, actual, val)
	}

	expected = []string{"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966"}
	actual = concept.ConcordedUUIDs()
	assert.Equal(t, expected, actual)
	assert.Equal(t, "56388858-38d6-4dfc-a001-506394259b51", concept.PreferredUUID())
}

func TestConvertPersonToESConceptModel(t *testing.T) {
	assert := assert.New(t)
	testModelPopulator := newTestModelPopulator()

	tests := []struct {
		conceptModel         ConceptModel
		esPersonConceptModel EsPersonConceptModel
	}{
		{
			ConceptModel{
				UUID:       "0f07d468-fc37-3c44-bf19-a81f2aae9f36",
				DirectType: "Person",
				PrefLabel:  "Martin Wolf",
				Aliases:    []string{},
			},
			EsPersonConceptModel{
				EsConceptModel: EsConceptModel{
					Id:        "http://api.ft.com/things/0f07d468-fc37-3c44-bf19-a81f2aae9f36",
					ApiUrl:    "http://api.ft.com/people/0f07d468-fc37-3c44-bf19-a81f2aae9f36",
					PrefLabel: "Martin Wolf",
					Types: []string{
						"http://www.ft.com/ontology/core/Thing",
						"http://www.ft.com/ontology/concept/Concept",
						"http://www.ft.com/ontology/person/Person",
					},
					DirectType: "http://www.ft.com/ontology/person/Person",
					Aliases:    []string{},
				},
				IsFTAuthor: "false",
			},
		},
	}

	for _, testModel := range tests {
		esModel := testModelPopulator.ConvertConceptToESConceptModel(testModel.conceptModel, "people").(EsPersonConceptModel)
		assert.Equal(testModel.esPersonConceptModel.Id, esModel.Id, fmt.Sprintf("Expected Id %s differs from actual id %s ", testModel.esPersonConceptModel.Id, esModel.Id))
		assert.Equal(testModel.esPersonConceptModel.ApiUrl, esModel.ApiUrl, fmt.Sprintf("Expected ApiUrl %s differs from actual ApiUrl %s ", testModel.esPersonConceptModel.ApiUrl, esModel.ApiUrl))
		assert.Equal(testModel.esPersonConceptModel.DirectType, esModel.DirectType, fmt.Sprintf("Expected DirectType %s differs from actual DirectType %s ", testModel.esPersonConceptModel.DirectType, esModel.DirectType))
		assert.Equal(testModel.esPersonConceptModel.PrefLabel, esModel.PrefLabel, fmt.Sprintf("Expected PrefLabel %s differs from actual PrefLabel %s ", testModel.esPersonConceptModel.PrefLabel, esModel.PrefLabel))
		assert.Equal(testModel.esPersonConceptModel.Types, esModel.Types, fmt.Sprintf("Expected Types %s differ from actual Types %s ", testModel.esPersonConceptModel.Types, esModel.Types))
		assert.Equal(testModel.esPersonConceptModel.Aliases, esModel.Aliases, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esPersonConceptModel.Aliases, esModel.Aliases))
		assert.Equal(testModel.esPersonConceptModel.IsFTAuthor, esModel.IsFTAuthor, fmt.Sprintf("Expected IsFTAuthor %s differ from actual IsFTAuthor %s ", testModel.esPersonConceptModel.IsFTAuthor, esModel.IsFTAuthor))
	}
}

func TestReverse(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		input          []string
		expectedResult []string
	}{
		{
			input:          []string{},
			expectedResult: []string{},
		},
		{
			input:          nil,
			expectedResult: nil,
		},
		{
			input:          []string{"foo"},
			expectedResult: []string{"foo"},
		},
		{
			input:          []string{"foo", "bar"},
			expectedResult: []string{"bar", "foo"},
		},
		{
			input:          []string{"foo", "bar", "word"},
			expectedResult: []string{"word", "bar", "foo"},
		},
	}

	for _, testCase := range tests {
		actualResult := reverse(testCase.input)
		assert.Equal(testCase.expectedResult, actualResult)
	}
}
