package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertToESConceptModel(t *testing.T) {
	assert := assert.New(t)
	testAuthorService := curatedAuthorService{
		httpClient: nil,
		serviceURL: "url",
		authorIds:  []AuthorUUID{{"2916ded0-6d1f-4449-b54c-3805da729c1d"}, {"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}},
	}

	testModelPopulater := NewEsModelPopulater(&testAuthorService)

	tests := []struct {
		conceptModel   ConceptModel
		esConceptModel EsConceptModel
	}{
		{
			ConceptModel{
				UUID:       "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8",
				DirectType: "PublicCompany",
				PrefLabel:  "Apple, Inc.",
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
				DirectType: "http://www.ft.com/ontology/company/PublicCompany",
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
		esModel := testModelPopulater.ConvertToESConceptModel(testModel.conceptModel, "organisations").(EsConceptModel)
		assert.Equal(testModel.esConceptModel.Id, esModel.Id, fmt.Sprintf("Expected Id %s differs from actual id %s ", testModel.esConceptModel.Id, esModel.Id))
		assert.Equal(testModel.esConceptModel.ApiUrl, esModel.ApiUrl, fmt.Sprintf("Expected ApiUrl %s differs from actual ApiUrl %s ", testModel.esConceptModel.ApiUrl, esModel.ApiUrl))
		assert.Equal(testModel.esConceptModel.DirectType, esModel.DirectType, fmt.Sprintf("Expected DirectType %s differs from actual DirectType %s ", testModel.esConceptModel.DirectType, esModel.DirectType))
		assert.Equal(testModel.esConceptModel.PrefLabel, esModel.PrefLabel, fmt.Sprintf("Expected PrefLabel %s differs from actual PrefLabel %s ", testModel.esConceptModel.PrefLabel, esModel.PrefLabel))
		assert.Equal(testModel.esConceptModel.Types, esModel.Types, fmt.Sprintf("Expected Types %s differ from actual Types %s ", testModel.esConceptModel.Types, esModel.Types))
		assert.Equal(testModel.esConceptModel.Aliases, esModel.Aliases, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esConceptModel.Aliases, esModel.Aliases))
	}

}

func TestConvertPersonToESConceptModel(t *testing.T) {
	assert := assert.New(t)
	testAuthorService := curatedAuthorService{
		httpClient: nil,
		serviceURL: "url",
		authorIds:  []AuthorUUID{{"2916ded0-6d1f-4449-b54c-3805da729c1d"}, {"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}},
	}
	testModelPopulater := NewEsModelPopulater(&testAuthorService)

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
		esModel := testModelPopulater.ConvertToESConceptModel(testModel.conceptModel, "people").(EsPersonConceptModel)
		assert.Equal(testModel.esPersonConceptModel.Id, esModel.Id, fmt.Sprintf("Expected Id %s differs from actual id %s ", testModel.esPersonConceptModel.Id, esModel.Id))
		assert.Equal(testModel.esPersonConceptModel.ApiUrl, esModel.ApiUrl, fmt.Sprintf("Expected ApiUrl %s differs from actual ApiUrl %s ", testModel.esPersonConceptModel.ApiUrl, esModel.ApiUrl))
		assert.Equal(testModel.esPersonConceptModel.DirectType, esModel.DirectType, fmt.Sprintf("Expected DirectType %s differs from actual DirectType %s ", testModel.esPersonConceptModel.DirectType, esModel.DirectType))
		assert.Equal(testModel.esPersonConceptModel.PrefLabel, esModel.PrefLabel, fmt.Sprintf("Expected PrefLabel %s differs from actual PrefLabel %s ", testModel.esPersonConceptModel.PrefLabel, esModel.PrefLabel))
		assert.Equal(testModel.esPersonConceptModel.Types, esModel.Types, fmt.Sprintf("Expected Types %s differ from actual Types %s ", testModel.esPersonConceptModel.Types, esModel.Types))
		assert.Equal(testModel.esPersonConceptModel.Aliases, esModel.Aliases, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esPersonConceptModel.Aliases, esModel.Aliases))
		assert.Equal(testModel.esPersonConceptModel.IsFTAuthor, esModel.IsFTAuthor, fmt.Sprintf("Expected Aliases %s differ from actual Aliases %s ", testModel.esPersonConceptModel.IsFTAuthor, esModel.IsFTAuthor))
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
