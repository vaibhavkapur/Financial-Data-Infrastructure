package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
	"distributed-tick-data-database/src/query"
	"distributed-tick-data-database/src/storage"
)

type Server struct {
	engine   *storage.Engine
	pipeline *ingest.Pipeline
	qsvc     *query.Service
	mux      *http.ServeMux
}

func New(engine *storage.Engine, pipeline *ingest.Pipeline, qsvc *query.Service) *Server {
	s := &Server{
		engine:   engine,
		pipeline: pipeline,
		qsvc:     qsvc,
		mux:      http.NewServeMux(),
	}
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.mux.HandleFunc("/v1/ingest", s.handleIngest)
	s.mux.HandleFunc("/v1/query", s.handleQuery)
	s.mux.HandleFunc("/v1/gaps", s.handleGaps)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"node_id":  s.engine.Config().NodeID,
		"topology": s.engine.Config().Topology,
		"shards":   s.engine.Config().NumShards,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"storage":      s.engine.StorageStats(),
		"pipeline":     s.pipeline.Metrics(),
		"freshness":    s.engine.Freshness(),
		"completeness": s.engine.Completeness(),
	})
}

type ingestHTTPRequest struct {
	BatchID      string           `json:"batch_id"`
	IngestTimeNS int64            `json:"ingest_time_ns,omitempty"`
	Ticks        []domain.RawTick `json:"ticks"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"code": "ERR_METHOD_NOT_ALLOWED"})
		return
	}
	var req ingestHTTPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "ERR_INVALID_JSON", "message": err.Error()})
		return
	}
	if req.BatchID == "" {
		req.BatchID = "http-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	}
	res, err := s.pipeline.Submit(r.Context(), req.BatchID, req.Ticks, req.IngestTimeNS)
	if err != nil {
		var bpErr *ingest.BackpressureError
		if errors.As(err, &bpErr) {
			w.Header().Set("Retry-After", strconv.Itoa((bpErr.RetryAfterMS+999)/1000))
			writeJSON(w, http.StatusTooManyRequests, bpErr)
			return
		}
		var domErr *domain.DomainError
		if errors.As(err, &domErr) {
			writeJSON(w, http.StatusBadRequest, domErr)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "ERR_INTERNAL", "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	startNS, err := strconv.ParseInt(q.Get("start_time_ns"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "ERR_INVALID_PARAM", "field": "start_time_ns"})
		return
	}
	endNS, err := strconv.ParseInt(q.Get("end_time_ns"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "ERR_INVALID_PARAM", "field": "end_time_ns"})
		return
	}
	limit := 0
	if ls := q.Get("limit"); ls != "" {
		limit, err = strconv.Atoi(ls)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "ERR_INVALID_PARAM", "field": "limit"})
			return
		}
	}
	resp, err := s.qsvc.QueryRange(r.Context(), query.RangeRequest{
		InstrumentID: q.Get("instrument_id"),
		StartTimeNS:  startNS,
		EndTimeNS:    endNS,
		Limit:        limit,
	})
	if err != nil {
		var qErr *query.QueryError
		if errors.As(err, &qErr) {
			writeJSON(w, http.StatusBadRequest, qErr)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "ERR_INTERNAL", "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGaps(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"open_gaps":    s.engine.OpenGaps(),
		"completeness": s.engine.Completeness(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
