package service

import (
	//	log "github.com/Sirupsen/logrus"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

const (
	PERSON = "people"
)

type ModelPopulater interface {
	ConvertToESConceptModel(concept ConceptModel, conceptType string) interface{}
}

type EsModelPopulater struct {
	authorService AuthorService
}

func NewEsModelPopulater(authorService AuthorService) ModelPopulater {
	return &EsModelPopulater{authorService}
}

func (mp *EsModelPopulater) ConvertToESConceptModel(concept ConceptModel, conceptType string) interface{} {
	switch conceptType {

	case PERSON:
		return mp.convertToESPersonConceptModel(concept, conceptType)

	default:
		return convertToESDefaultConceptModel(concept, conceptType)
	}
}

func convertToESDefaultConceptModel(concept ConceptModel, conceptType string) EsConceptModel {
	esModel := EsConceptModel{}
	esModel.ApiUrl = mapper.APIURL(concept.UUID, []string{concept.DirectType}, "")
	esModel.Id = mapper.IDURL(concept.UUID)
	esModel.Types = mapper.TypeURIs(getTypes(concept.DirectType))
	directTypeArray := mapper.TypeURIs([]string{concept.DirectType})
	if len(directTypeArray) == 1 {
		esModel.DirectType = directTypeArray[0]
	}
	esModel.Aliases = concept.Aliases
	esModel.PrefLabel = concept.PrefLabel

	return esModel
}

func (mp *EsModelPopulater) convertToESPersonConceptModel(concept ConceptModel, conceptType string) EsPersonConceptModel {
	esConceptModel := convertToESDefaultConceptModel(concept, conceptType)
	esPersonModel := EsPersonConceptModel{EsConceptModel{esConceptModel.Id, esConceptModel.ApiUrl, esConceptModel.PrefLabel, esConceptModel.Types, esConceptModel.DirectType, esConceptModel.Aliases}, mp.authorService.IsFTAuthor(concept.UUID)}
	return esPersonModel
}

func getTypes(conceptType string) []string {
	conceptTypes := []string{conceptType}
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		conceptTypes = append(conceptTypes, parentType)
		parentType = mapper.ParentType(parentType)
	}
	return reverse(conceptTypes)
}

func reverse(strings []string) []string {
	if strings == nil {
		return nil
	}
	if len(strings) == 0 {
		return []string{}
	}
	var reversed []string
	for i := len(strings) - 1; i >= 0; i = i - 1 {
		reversed = append(reversed, strings[i])
	}
	return reversed
}
