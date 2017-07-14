package service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	transactionid "github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
)

const contentType = "application/json"
const authorTransformerIdsPath = "/__v1-authors-transformer/transformers/authors/__ids"
const gtgPath = "/__v1-authors-transformer/__gtg"

type AuthorUUID struct {
	UUID string `json:"ID"`
}

type AuthorService interface {
	LoadAuthorIdentifiers() error
	IsFTAuthor(UUID string) bool
	IsGTG() error
}

//uses v1 transformer author list
type curatedAuthorService struct {
	httpClient             *http.Client
	serviceURL             string
	authorUUIDs            map[string]struct{}
	publishClusterUser     string
	publishClusterpassword string
}

func NewAuthorService(serviceURL string, pubClusterKey string, client *http.Client) (AuthorService, error) {
	if len(pubClusterKey) == 0 {
		return nil, fmt.Errorf("credentials missing credentials, author service cannot make request to author transformer")
	}
	credentials := strings.Split(pubClusterKey, ":")
	cas := &curatedAuthorService{client, serviceURL, nil, credentials[0], credentials[1]}
	return cas, cas.LoadAuthorIdentifiers()
}

func (as *curatedAuthorService) LoadAuthorIdentifiers() error {
	tid := transactionid.NewTransactionID()
	req, err := http.NewRequest("GET", as.serviceURL+authorTransformerIdsPath, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("X-Request-Id", tid)
	req.Header.Add("User-Agent", "UPP concept-rw-elasticsearch")
	req.SetBasicAuth(as.publishClusterUser, as.publishClusterpassword)
	log.WithField("transaction_id", tid).Info("Requesting author list from v1 authors transformer." + req.RequestURI)

	resp, err := as.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("A non-200 error code from v1 authors transformer! Status: %v", resp.StatusCode)
	}

	as.authorUUIDs = make(map[string]struct{})

	scan := bufio.NewScanner(resp.Body)
	for scan.Scan() {
		var id AuthorUUID
		err = json.Unmarshal(scan.Bytes(), &id)
		if err != nil {
			return err
		}
		as.authorUUIDs[id.UUID] = struct{}{}
	}
	log.Infof("Found %v authors", len(as.authorUUIDs))

	return nil
}

func (as *curatedAuthorService) IsFTAuthor(uuid string) bool {
	_, found := as.authorUUIDs[uuid]
	return found
}

func (as *curatedAuthorService) IsGTG() error {
	resp, err := http.Get(as.serviceURL + gtgPath)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gtg endpoint returned a non-200 status: %v", resp.StatusCode)
	}
	return nil
}
