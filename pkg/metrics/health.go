package metrics

import (
	"net/http"
	"sync"
)

// healthState holds the current health state of the application.
type healthState struct {
	mu        sync.RWMutex
	ready     bool
	lastError string
}

// SetReady sets the ready state of the application.
func (h *healthState) SetReady(ready bool, errMsg string) {
	h.mu.Lock()
	h.ready = ready
	h.lastError = errMsg
	h.mu.Unlock()
}

// IsReady returns the current ready state.
func (h *healthState) IsReady() (bool, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ready, h.lastError
}

// globalHealth holds the global health state for the collector.
// This is used for the /health and /ready endpoints.
var globalHealth healthState

// SetHealthStatus sets the global health status for the /health and /ready endpoints.
// ready should be true when all components are initialized and the application
// is ready to process traffic.
func SetHealthStatus(ready bool, errMsg string) {
	globalHealth.SetReady(ready, errMsg)
}

// healthHandler handles the /health liveness probe.
// Returns 200 OK when the service is running.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// readyHandler handles the /ready readiness probe.
// Returns 200 OK when the service is ready to process traffic.
// Returns 503 Service Unavailable when not ready.
func readyHandler(w http.ResponseWriter, r *http.Request) {
	ready, errMsg := globalHealth.IsReady()
	if ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if errMsg != "" {
			w.Write([]byte(errMsg))
		} else {
			w.Write([]byte("Not ready"))
		}
	}
}
