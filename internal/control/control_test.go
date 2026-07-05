package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestStatusReturnsActiveByDefault(t *testing.T) {
	srv := New(Callbacks{})
	req := httptest.NewRequest(http.MethodGet, "/control/status", nil)
	w := httptest.NewRecorder()

	srv.handleStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp statusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "active" {
		t.Errorf("expected state=active, got %q", resp.State)
	}
}

func TestActivateFromPaused(t *testing.T) {
	var activated atomic.Int32
	srv := New(Callbacks{
		OnActivate: func() error {
			activated.Add(1)
			return nil
		},
	})

	// First pause it.
	req := httptest.NewRequest(http.MethodPost, "/control/pause", nil)
	w := httptest.NewRecorder()
	srv.handlePause(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("pause: expected 200, got %d", w.Code)
	}

	// Now activate.
	req = httptest.NewRequest(http.MethodPost, "/control/activate", nil)
	w = httptest.NewRecorder()
	srv.handleActivate(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("activate: expected 200, got %d", w.Code)
	}

	if activated.Load() != 1 {
		t.Errorf("expected OnActivate called once, got %d", activated.Load())
	}

	var resp statusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "active" {
		t.Errorf("expected state=active, got %q", resp.State)
	}
}

func TestPauseFromActive(t *testing.T) {
	var paused atomic.Int32
	srv := New(Callbacks{
		OnPause: func() error {
			paused.Add(1)
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/control/pause", nil)
	w := httptest.NewRecorder()
	srv.handlePause(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if paused.Load() != 1 {
		t.Errorf("expected OnPause called once, got %d", paused.Load())
	}

	var resp statusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "paused" {
		t.Errorf("expected state=paused, got %q", resp.State)
	}
}

func TestDoubleActivateNoOp(t *testing.T) {
	var count atomic.Int32
	srv := New(Callbacks{
		OnActivate: func() error {
			count.Add(1)
			return nil
		},
	})

	// Already active by default, activating again should not call callback.
	req := httptest.NewRequest(http.MethodPost, "/control/activate", nil)
	w := httptest.NewRecorder()
	srv.handleActivate(w, req)

	if count.Load() != 0 {
		t.Errorf("expected OnActivate not called (already active), got %d", count.Load())
	}
}

func TestDoublePauseNoOp(t *testing.T) {
	var count atomic.Int32
	srv := New(Callbacks{
		OnPause: func() error {
			count.Add(1)
			return nil
		},
	})

	// Pause first time.
	req := httptest.NewRequest(http.MethodPost, "/control/pause", nil)
	w := httptest.NewRecorder()
	srv.handlePause(w, req)

	// Pause second time - should not call callback again.
	req = httptest.NewRequest(http.MethodPost, "/control/pause", nil)
	w = httptest.NewRecorder()
	srv.handlePause(w, req)

	if count.Load() != 1 {
		t.Errorf("expected OnPause called once, got %d", count.Load())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := New(Callbacks{})

	// GET on /control/activate should fail.
	req := httptest.NewRequest(http.MethodGet, "/control/activate", nil)
	w := httptest.NewRecorder()
	srv.handleActivate(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("activate GET: expected 405, got %d", w.Code)
	}

	// GET on /control/pause should fail.
	req = httptest.NewRequest(http.MethodGet, "/control/pause", nil)
	w = httptest.NewRecorder()
	srv.handlePause(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("pause GET: expected 405, got %d", w.Code)
	}

	// POST on /control/status should fail.
	req = httptest.NewRequest(http.MethodPost, "/control/status", nil)
	w = httptest.NewRecorder()
	srv.handleStatus(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status POST: expected 405, got %d", w.Code)
	}
}
