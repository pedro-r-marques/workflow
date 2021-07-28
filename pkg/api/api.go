package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/pedro-r-marques/workflow/pkg/engine"
)

type ApiServer struct {
	engine engine.WorkflowEngine
}

func NewApiServer(engine engine.WorkflowEngine) *ApiServer {
	return &ApiServer{engine: engine}
}

// GET /api/workflows
func (s *ApiServer) listWorkflows(w http.ResponseWriter, req *http.Request) {
	list := s.engine.ListWorkflows()
	msg, err := json.Marshal(list)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-type", "application/json")
	w.Write(msg)
}

// POST /api/workflow/<name>?id=<>
func (s *ApiServer) createJob(w http.ResponseWriter, req *http.Request) {
	workflow := string(req.URL.Path[len("/api/workflow/"):])
	q := req.URL.Query()
	var jobID uuid.UUID
	var idSet bool
	if idList, exists := q["id"]; exists {
		if len(idList) != 1 {
			http.Error(w, "invalid format for query parameter \"id\"", http.StatusBadRequest)
			return
		}
		var err error
		jobID, err = uuid.Parse(idList[0])
		if err != nil {
			http.Error(w, fmt.Sprintf("unable to parse uuid: %s", err.Error()), http.StatusBadRequest)
			return
		}
		idSet = true
	}

	var msg map[string]json.RawMessage
	if req.Body != nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(body, &msg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if idStr, exists := msg["id"]; exists {
		v, err := uuid.Parse(string(idStr))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if idSet && v != jobID {
			http.Error(w, "different \"id\" values in query and body", http.StatusBadRequest)
			return
		}
		if !idSet {
			jobID = v
		}
	} else if !idSet {
		var err error
		if jobID, err = uuid.NewRandom(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := s.engine.Create(workflow, jobID, msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := struct {
		ID string `json:"id"`
	}{
		ID: jobID.String(),
	}

	if rbody, err := json.Marshal(response); err == nil {
		w.Header().Set("Content-type", "application/json")
		w.Write(rbody)
	}
}

// GET /api/workflow/<name>
func (s *ApiServer) listWorkflowJobs(w http.ResponseWriter, req *http.Request) {
	name := req.URL.Path[len("/api/workflow/"):]
	q := req.URL.Query()
	if vhost := q.Get("vhost"); vhost != "" {
		name = vhost + "/" + name
	}
	uuids, err := s.engine.ListWorkflowJobs(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	response := struct {
		Jobs []uuid.UUID `json:"jobs"`
	}{
		Jobs: uuids,
	}
	body, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-type", "application/json")
	w.Write(body)
}

// GET /api/jobs
func (s *ApiServer) listJobs(w http.ResponseWriter, req *http.Request) {
	jobIDs := s.engine.ListJobs()
	var response struct {
		Jobs []string `json:"jobs"`
	}
	response.Jobs = make([]string, 0, len(jobIDs))
	for _, j := range jobIDs {
		response.Jobs = append(response.Jobs, j.String())
	}

	body, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-type", "application/json")
	w.Write(body)
}

// GET /api/job/id
func (s *ApiServer) getJob(w http.ResponseWriter, req *http.Request) {
	jobIDStr := req.URL.Path[len("/api/job/"):]
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid uuid %s", jobIDStr), http.StatusBadRequest)
		return
	}

	open, closed, err := s.engine.JobStatus(jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	response := struct {
		ID     string                  `json:"uuid"`
		Open   []engine.JobStatusEntry `json:"running"`
		Closed []engine.JobStatusEntry `json:"completed"`
	}{jobID.String(), open, closed}
	body, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-type", "application/json")
	w.Write(body)
}

// DELETE /api/job/id
func (s *ApiServer) deleteJob(w http.ResponseWriter, req *http.Request) {
}

func (s *ApiServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !strings.HasPrefix(req.URL.Path, "/api/") {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	command := req.URL.Path[len("/api/"):]
	loc := strings.Index(command, "/")
	if loc != -1 {
		command = command[:loc]
	}
	switch command {
	case "workflows":
		if req.Method == http.MethodGet {
			s.listWorkflows(w, req)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	case "workflow":
		switch req.Method {
		case http.MethodPost:
			s.createJob(w, req)
		case http.MethodGet:
			s.listWorkflowJobs(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	case "jobs":
		if req.Method == http.MethodGet {
			s.listJobs(w, req)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	case "job":
		switch req.Method {
		case http.MethodGet:
			s.getJob(w, req)
		case http.MethodDelete:
			s.deleteJob(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
