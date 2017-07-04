package service

// Concept contains common function between both concept models
type Concept interface {
	// GetAuthorities returns an array containing all authorities that this concept is identified by
	GetAuthorities() []string
	// ConcordedUUIDs returns an array containing all concorded concept uuids - N.B. it will not contain the canonical prefUUID.
	ConcordedUUIDs() []string

	PreferredUUID() string
}

type ConceptModel struct {
	UUID                   string                 `json:"uuid"`
	DirectType             string                 `json:"type"`
	PrefLabel              string                 `json:"prefLabel"`
	Aliases                []string               `json:"aliases,omitempty"`
	AlternativeIdentifiers map[string]interface{} `json:"alternativeIdentifiers,omitempty"`
}

type AggregateConceptModel struct {
	PrefUUID              string          `json:"prefUUID"`
	DirectType            string          `json:"type"`
	PrefLabel             string          `json:"prefLabel"`
	Aliases               []string        `json:"aliases,omitempty"`
	SourceRepresentations []SourceConcept `json:"sourceRepresentations"`
}

type SourceConcept struct {
	UUID      string `json:"uuid"`
	Authority string `json:"authority"`
}

type EsConceptModel struct {
	Id          string   `json:"id"`
	ApiUrl      string   `json:"apiUrl"`
	PrefLabel   string   `json:"prefLabel"`
	Types       []string `json:"types"`
	Authorities []string `json:"authorities"`
	DirectType  string   `json:"directType"`
	Aliases     []string `json:"aliases,omitempty"`
}

type EsPersonConceptModel struct {
	EsConceptModel
	IsFTAuthor string `json:"isFTAuthor"`
}

func (c AggregateConceptModel) PreferredUUID() string {
	return c.PrefUUID
}

func (c ConceptModel) PreferredUUID() string {
	return c.UUID
}

func (c ConceptModel) GetAuthorities() []string {
	var authorities []string
	for authority := range c.AlternativeIdentifiers {
		if authority == "uuids" {
			continue // exclude the "uuids" alternativeIdentifier
		}
		authorities = append(authorities, authority)
	}
	return authorities
}

func (c AggregateConceptModel) GetAuthorities() []string {
	var authorities []string
	for _, src := range c.SourceRepresentations {
		authorities = append(authorities, src.Authority)
	}
	return authorities
}

func (c ConceptModel) ConcordedUUIDs() []string {
	return make([]string, 0) // we don't want to remove concorded concepts for the original concept model.
}

func (c AggregateConceptModel) ConcordedUUIDs() []string {
	var uuids []string
	for _, src := range c.SourceRepresentations {
		if src.UUID != c.PrefUUID {
			uuids = append(uuids, src.UUID)
		}
	}
	return uuids
}
