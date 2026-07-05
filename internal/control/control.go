// Package control implements the external signal-driven control plane for
// pgshadow. It exposes HTTP endpoints that allow external orchestration
// (Patroni callbacks, CNPG lifecycle hooks, K8s preStop/postStart, cron
// scripts) to activate or pause packet capture without pgshadow needing to
// understand the deployment topology.
//
// Endpoints:
//
//	POST /control/activate  - start/resume capture
//	POST /control/pause     - stop/pause capture
//	GET  /control/status    - return current state (JSON)
package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"pgshadow/internal/logger"
)

// State represents the capture control state.
type State int

const (
	// StateActive means capture is running.
	StateActive State = iota
	// StatePaused means capture is paused by external signal.
	StatePaused
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StatePaused:
		return "paused"
	default:
		return "unknown"
	}
}

// Callbacks defines the operations the control plane invokes on state changes.
type Callbacks struct {
	// OnActivate is called when the control plane transitions to active.
	// It should start or resume capture. May be nil if no action needed.
	OnActivate func() error
	// OnPause is called when the control plane transitions to paused.
	// It should stop or pause capture. May be nil if no action needed.
	OnPause func() error
}

// Config configures the control plane HTTP server.
type Config struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"` // default 9091
}

// DefaultPort is the control plane port when none is configured.
const DefaultPort = 9091

// Server is the control plane HTTP server.
type Server struct {
	mu        sync.RWMutex
	state     State
	lastChange time.Time
	callbacks Callbacks
	srv       *http.Server
}

// New creates a new control plane server. The initial state is active (capture
// running) unless the caller explicitly pauses it.
func New(cb Callbacks) *Server {
	return &Server{
		state:      StateActive,
		lastChange: time.Now(),
		callbacks:  cb,
	}
}

// State returns the current control state.
func (s *Server) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// IsActive returns true if capture should be running.
func (s *Server) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == StateActive
}

// Serve starts the control plane HTTP server on the given port. It blocks until
// the server is shut down. Call Shutdown to stop it gracefully.
func (s *Server) Serve(port int) error {
	if port <= 0 {
		port = DefaultPort
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/control/activate", s.handleActivate)
	mux.HandleFunc("/control/pause", s.handlePause)
	mux.HandleFunc("/control/status", s.handleStatus)

	s.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	logger.Infof("control plane listening on :%d", port)
	return s.srv.ListenAndServe()
}

// Shutdown gracefully shuts down the control plane server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

// handleActivate transitions to active state and triggers capture start.
func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	prev := s.state
	s.state = StateActive
	s.lastChange = time.Now()
	s.mu.Unlock()

	if prev != StateActive && s.callbacks.OnActivate != nil {
		if err := s.callbacks.OnActivate(); err != nil {
			logger.Errorf("control: activate callback failed: %v", err)
			http.Error(w, fmt.Sprintf("activate failed: %v", err), http.StatusInternalServerError)
			return
		}
		logger.Info("control: activated (capture started)")
	}

	writeJSON(w, statusResponse{
		State:      StateActive.String(),
		Since:      s.lastChange,
		Transition: prev.String() + " -> active",
	})
}

// handlePause transitions to paused state and triggers capture stop.
func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	prev := s.state
	s.state = StatePaused
	s.lastChange = time.Now()
	s.mu.Unlock()

	if prev != StatePaused && s.callbacks.OnPause != nil {
		if err := s.callbacks.OnPause(); err != nil {
			logger.Errorf("control: pause callback failed: %v", err)
			http.Error(w, fmt.Sprintf("pause failed: %v", err), http.StatusInternalServerError)
			return
		}
		logger.Info("control: paused (capture stopped)")
	}

	writeJSON(w, statusResponse{
		State:      StatePaused.String(),
		Since:      s.lastChange,
		Transition: prev.String() + " -> paused",
	})
}

// handleStatus returns the current state as JSON.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	state := s.state
	since := s.lastChange
	s.mu.RUnlock()

	writeJSON(w, statusResponse{
		State: state.String(),
		Since: since,
	})
}

// statusResponse is the JSON payload for control endpoints.
type statusResponse struct {
	State      string    `json:"state"`
	Since      time.Time `json:"since"`
	Transition string    `json:"transition,omitempty"`
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
