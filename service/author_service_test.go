package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func (m *mockAuthorTransformerServer) startMockAuthorTransformerServer(t *testing.T) *httptest.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		ua := req.Header.Get("User-Agent")
		assert.Equal(t, "UPP concept-rw-elasticsearch", ua, "user-agent header")
		tid := req.Header.Get("X-Request-Id")
		assert.NotEmpty(t, tid, "transactio id")
		contentType := req.Header.Get("Content-Type")
		user, password, _ := req.BasicAuth()

		w.WriteHeader(m.Ids(contentType, user, password))

		authorIds := `{"ID":"004079c8-9193-3e99-8045-2dea1bb7cfe1"}
{"ID":"005ab900-0897-394a-a79c-dda0932d1f13"}`
		w.Write([]byte(authorIds))

	}).Methods("GET")

	return httptest.NewServer(r)
}

func TestLoadAuthorIdentifiersResponseSuccess(t *testing.T) {
	//authorIds := []AuthorUUID{{"2916ded0-6d1f-4449-b54c-3805da729c1d"}, {"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}}
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	testService, _ := NewAuthorService(testServer.URL, "username:password", &http.Client{})
	assert.Equal(t, "004079c8-9193-3e99-8045-2dea1bb7cfe1", testService.(*curatedAuthorService).authorIds[0].UUID)
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
		httpClient: nil,
		serviceURL: "url",
		authorIds:  []AuthorUUID{{"2916ded0-6d1f-4449-b54c-3805da729c1d"}, {"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}},
	}
	isAuthor := testService.IsFTAuthor("2916ded0-6d1f-4449-b54c-3805da729c1d")
	assert.Equal(t, "true", isAuthor)
}

func TestIsIsFTAuthorFalse(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient: nil,
		serviceURL: "url",
		authorIds:  []AuthorUUID{{"2916ded0-6d1f-4449-b54c-3805da729c1d"}, {"ddc22d37-624a-4a3d-88e5-ba508e38c8ba"}},
	}
	isAuthor := testService.IsFTAuthor("61346cf7-008b-49e0-945a-832a90cd60ac")
	assert.Equal(t, "false", isAuthor)

}

type mockAuthorTransformerServer struct {
	mock.Mock
}

func (m *mockAuthorTransformerServer) Ids(contentType string, user string, password string) int {
	args := m.Called(contentType, user, password)
	return args.Int(0)
}
