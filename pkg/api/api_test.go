package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mock_engine "github.com/pedro-r-marques/workflow/pkg/engine/mock"
)

func TestListWorkflows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := httptest.NewRecorder()
	engine := mock_engine.NewMockWorkflowEngine(ctrl)
	engine.EXPECT().ListWorkflows().Return([]string{"example"})
	apiSrv := NewApiServer(engine)
	req, _ := http.NewRequest(http.MethodGet, "/api/workflows", nil)
	apiSrv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestCreateJobIDParamNoBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := httptest.NewRecorder()
	engine := mock_engine.NewMockWorkflowEngine(ctrl)

	jobID, _ := uuid.Parse("f557697b-f911-401c-86b7-6d9b62f1f2bb")
	engine.EXPECT().Create(gomock.Eq("example"), gomock.Eq(jobID), gomock.Any()).Return(nil)

	apiSrv := NewApiServer(engine)
	req, _ := http.NewRequest(http.MethodPost, "/api/workflow/example?id=f557697b-f911-401c-86b7-6d9b62f1f2bb", nil)
	apiSrv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	wbody, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	var response struct {
		ID string
	}
	assert.NoError(t, json.Unmarshal(wbody, &response))
	v, err := uuid.Parse(response.ID)
	require.NoError(t, err)
	require.Equal(t, jobID, v)
}

func TestCreateJobBodyId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := httptest.NewRecorder()
	engine := mock_engine.NewMockWorkflowEngine(ctrl)

	jobID, _ := uuid.Parse("f557697b-f911-401c-86b7-6d9b62f1f2bb")
	engine.EXPECT().Create(gomock.Eq("example"), gomock.Eq(jobID), gomock.Any()).Return(nil)

	msg := map[string]json.RawMessage{
		"id": json.RawMessage("\"f557697b-f911-401c-86b7-6d9b62f1f2bb\""),
	}
	body, err := json.Marshal(msg)
	assert.NoError(t, err)
	apiSrv := NewApiServer(engine)
	req, _ := http.NewRequest(http.MethodPost, "/api/workflow/example", bytes.NewReader(body))
	apiSrv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
func TestCreateJobRandId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := httptest.NewRecorder()
	engine := mock_engine.NewMockWorkflowEngine(ctrl)

	engine.EXPECT().Create(gomock.Eq("example"), gomock.Any(), gomock.Any()).Return(nil)

	msg := map[string]json.RawMessage{
		"foo": json.RawMessage("\"bar\""),
	}
	body, err := json.Marshal(msg)
	assert.NoError(t, err)
	apiSrv := NewApiServer(engine)
	req, _ := http.NewRequest(http.MethodPost, "/api/workflow/example", bytes.NewReader(body))
	apiSrv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	wbody, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	var response struct {
		ID string
	}
	assert.NoError(t, json.Unmarshal(wbody, &response))
	_, err = uuid.Parse(response.ID)
	require.NoError(t, err)
}

func TestListJobs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := httptest.NewRecorder()
	engine := mock_engine.NewMockWorkflowEngine(ctrl)

	jobID := uuid.New()
	engine.EXPECT().ListJobs().Return([]uuid.UUID{jobID})

	apiSrv := NewApiServer(engine)
	req, _ := http.NewRequest(http.MethodGet, "/api/jobs", nil)
	apiSrv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	wbody, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	var response struct {
		Jobs []string
	}
	assert.NoError(t, json.Unmarshal(wbody, &response))

	require.EqualValues(t, []string{jobID.String()}, response.Jobs)
}
