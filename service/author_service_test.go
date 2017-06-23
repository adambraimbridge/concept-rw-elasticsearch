package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var expectedAuthorUUIDs = map[string]struct{}{
	"2916ded0-6d1f-4449-b54c-3805da729c1d": struct{}{},
	"ddc22d37-624a-4a3d-88e5-ba508e38c8ba": struct{}{},
}

func (m *mockAuthorTransformerServer) startMockAuthorTransformerServer(t *testing.T) *httptest.Server {
	r := mux.NewRouter()
	r.HandleFunc(idsPath, func(w http.ResponseWriter, req *http.Request) {
		ua := req.Header.Get("User-Agent")
		assert.Equal(t, "UPP concept-rw-elasticsearch", ua, "user-agent header")
		tid := req.Header.Get("X-Request-Id")
		assert.NotEmpty(t, tid, "transactio id")
		contentType := req.Header.Get("Content-Type")
		user, password, _ := req.BasicAuth()

		w.WriteHeader(m.Ids(contentType, user, password))

		authorIds := `{"ID":"2916ded0-6d1f-4449-b54c-3805da729c1d"}
{"ID":"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}`
		w.Write([]byte(authorIds))

	}).Methods("GET")

	return httptest.NewServer(r)
}

func TestLoadAuthorIdentifiersResponseSuccess(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", &http.Client{})
	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")

	for expectedUUID := range expectedAuthorUUIDs {
		assert.True(t, as.IsFTAuthor(expectedUUID), "The UUID should belong to an author")
	}
	m.AssertExpectations(t)
}

func TestLoadAuthorIdentifiersResponseNot200(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusBadRequest)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	_, err := NewAuthorService(testServer.URL, "username:password", &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)

}

func TestLoadAuthorIdentifiersRequestError(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	_, err := NewAuthorService("#:", "username:password", &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)

}

func TestLoadAuthorIdentifiersResponseError(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	testServer := m.startMockAuthorTransformerServer(t)
	testServer.Close()

	_, err := NewAuthorService(testServer.URL, "username:password", &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)

}

func TestIsFTAuthorTrue(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: expectedAuthorUUIDs,
	}
	isAuthor := testService.IsFTAuthor("2916ded0-6d1f-4449-b54c-3805da729c1d")
	assert.True(t, isAuthor)
}

func TestIsIsFTAuthorFalse(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: expectedAuthorUUIDs,
	}
	isAuthor := testService.IsFTAuthor("61346cf7-008b-49e0-945a-832a90cd60ac")
	assert.False(t, isAuthor)
}

type mockAuthorTransformerServer struct {
	mock.Mock
}

func (m *mockAuthorTransformerServer) Ids(contentType string, user string, password string) int {
	args := m.Called(contentType, user, password)
	return args.Int(0)
}
