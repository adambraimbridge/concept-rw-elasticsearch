package service

import (
	"time"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

const (
	person     = "people"
	membership = "membership"
)

func ConvertConceptToESConceptModel(concept ConceptModel, conceptType string, publishRef string) EsModel {
	esModel := newESConceptModel(concept.UUID, conceptType, concept.DirectType, concept.Aliases, concept.GetAuthorities(), concept.PrefLabel, publishRef, concept.IsDeprecated, concept.ScopeNote)

	switch conceptType {
	case person: // person type should not come through as the old model.
		esPersonModel := &EsPersonConceptModel{
			EsConceptModel: esModel,
		}
		return esPersonModel
	default:
		return esModel
	}
}

func ConvertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string, publishRef string) (esModel EsModel) {

	switch conceptType {
	case membership:
		memberships := make([]string, len(concept.MembershipRoles))
		for i, m := range concept.MembershipRoles {
			memberships[i] = m.RoleUUID
		}
		esModel = &EsMembershipModel{
			Id:             concept.PrefUUID,
			PersonId:       concept.PersonUUID,
			OrganisationId: concept.OrganisationUUID,
			Memberships:    memberships,
		}
	case person:
		esModel = &EsPersonConceptModel{
			EsConceptModel: getEsConcept(concept, conceptType, publishRef),
		}
	default:
		esModel = getEsConcept(concept, conceptType, publishRef)
	}

	return esModel
}

func getEsConcept(concept AggregateConceptModel, conceptType string, publishRef string) *EsConceptModel {
	return newESConceptModel(
		concept.PrefUUID,
		conceptType,
		concept.DirectType,
		concept.Aliases,
		concept.GetAuthorities(),
		concept.PrefLabel,
		publishRef,
		concept.IsDeprecated,
		concept.ScopeNote)
}

func newESConceptModel(uuid string, conceptType string, directType string, aliases []string, authorities []string, prefLabel string, publishRef string, isDeprecated bool, scopeNote string) (esModel *EsConceptModel) {
	esModel = &EsConceptModel{}
	esModel.ApiUrl = mapper.APIURL(uuid, []string{directType}, "")
	esModel.Id = mapper.IDURL(uuid)
	esModel.Types = mapper.TypeURIs(getTypes(directType))
	directTypeArray := mapper.TypeURIs([]string{directType})
	if len(directTypeArray) == 1 {
		esModel.DirectType = directTypeArray[0]
	} else {
		log.WithField("conceptType", conceptType).WithField("prefUUID", uuid).Warn("More than one directType found during type mapping.")
	}

	esModel.Aliases = aliases
	esModel.PrefLabel = prefLabel
	esModel.Authorities = authorities
	esModel.LastModified = time.Now().Format(time.RFC3339)
	esModel.PublishReference = publishRef
	esModel.IsDeprecated = isDeprecated
	esModel.ScopeNote = scopeNote

	return esModel
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
		return strings
	}
	var reversed []string
	for i := len(strings) - 1; i >= 0; i = i - 1 {
		reversed = append(reversed, strings[i])
	}
	return reversed
}
