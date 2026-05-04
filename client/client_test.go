package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

func TestResolveTargetAddress(t *testing.T) {
	tests := []struct {
		name        string
		coordinator string
		leader      string
		want        string
		wantErr     string
	}{
		{
			name:        "coordinator only",
			coordinator: "127.0.0.1:8090",
			want:        "127.0.0.1:8090",
		},
		{
			name:   "leader only",
			leader: "127.0.0.1:9090",
			want:   "127.0.0.1:9090",
		},
		{
			name:    "neither set",
			wantErr: "must specify one of -coordinator or -leader",
		},
		{
			name:        "both set",
			coordinator: "127.0.0.1:8090",
			leader:      "127.0.0.1:9090",
			wantErr:     "must specify only one of -coordinator or -leader",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTargetAddress(tc.coordinator, tc.leader)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTargetAddress returned unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected target %q, got %q", tc.want, got)
			}
		})
	}
}

func TestResolveMessageProvider(t *testing.T) {
	tests := []struct {
		name        string
		x           int
		y           int
		payload     string
		wantX       int
		wantY       int
		wantPayload string
		wantErr     string
	}{
		{
			name:        "exact message",
			x:           7,
			y:           11,
			payload:     "phase3-check",
			wantX:       7,
			wantY:       11,
			wantPayload: "phase3-check",
		},
		{
			name:    "partial exact message",
			x:       7,
			y:       11,
			wantErr: "must specify all of -x, -y, and -payload",
		},
		{
			name:    "x out of range",
			x:       256,
			y:       0,
			payload: "payload",
			wantErr: "-x must be in [0,256)",
		},
		{
			name:    "y out of range",
			x:       0,
			y:       256,
			payload: "payload",
			wantErr: "-y must be in [0,256)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, err := resolveMessageProvider(tc.x, tc.y, tc.payload)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
			}
			msg, err := provider()
			if err != nil {
				t.Fatalf("message provider returned unexpected error: %v", err)
			}
			if msg.X != tc.wantX || msg.Y != tc.wantY {
				t.Fatalf("expected coordinates (%d,%d), got (%d,%d)", tc.wantX, tc.wantY, msg.X, msg.Y)
			}
			gotPayload := string(msg.Message[:len(tc.wantPayload)])
			if gotPayload != tc.wantPayload {
				t.Fatalf("expected payload %q, got %q", tc.wantPayload, gotPayload)
			}
		})
	}
}

func TestResolveMessageProviderRandomFallback(t *testing.T) {
	provider, err := resolveMessageProvider(-1, -1, "")
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	msg, err := provider()
	if err != nil {
		t.Fatalf("message provider returned unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("message provider returned nil message")
	}
	if msg.X < 0 || msg.X >= 256 {
		t.Fatalf("random message X out of range: %d", msg.X)
	}
	if msg.Y < 0 || msg.Y >= 256 {
		t.Fatalf("random message Y out of range: %d", msg.Y)
	}
}

func TestResolveMessageProviderExactReusesMessage(t *testing.T) {
	provider, err := resolveMessageProvider(3, 5, "same")
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	first, err := provider()
	if err != nil {
		t.Fatalf("message provider returned unexpected error: %v", err)
	}
	second, err := provider()
	if err != nil {
		t.Fatalf("message provider returned unexpected error: %v", err)
	}
	if first != second {
		t.Fatal("expected exact-message provider to reuse the deterministic message")
	}
}

func TestResolveMessageProviderRandomGeneratesEachTime(t *testing.T) {
	oldRandomMessage := randomMessage
	defer func() {
		randomMessage = oldRandomMessage
	}()

	var calls int
	randomMessage = func() (*db.Plaintext, error) {
		calls++
		return &db.Plaintext{X: calls, Y: calls}, nil
	}

	provider, err := resolveMessageProvider(-1, -1, "")
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	first, err := provider()
	if err != nil {
		t.Fatalf("first message returned unexpected error: %v", err)
	}
	second, err := provider()
	if err != nil {
		t.Fatalf("second message returned unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected random generator to be called twice, got %d", calls)
	}
	if first == second || first.X == second.X || first.Y == second.Y {
		t.Fatalf("expected fresh random messages, got first=%v second=%v", first, second)
	}
}

func TestResolveMessageProviderRandomError(t *testing.T) {
	oldRandomMessage := randomMessage
	defer func() {
		randomMessage = oldRandomMessage
	}()

	wantErr := errors.New("random failed")
	randomMessage = func() (*db.Plaintext, error) {
		return nil, wantErr
	}

	provider, err := resolveMessageProvider(-1, -1, "")
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	if _, err := provider(); err != wantErr {
		t.Fatalf("expected random error %v, got %v", wantErr, err)
	}
}

func TestRunClientLoopSignalsStopOnNoActiveEpoch(t *testing.T) {
	var signaled bool
	var calls int
	err := runClientLoop(
		true,
		func() bool { return false },
		func() { signaled = true },
		func() error {
			calls++
			return errors.New("No active epoch")
		},
	)
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected No active epoch error, got %v", err)
	}
	if !signaled {
		t.Fatal("expected No active epoch to signal hammer shutdown")
	}
	if calls != 1 {
		t.Fatalf("expected one upload attempt, got %d", calls)
	}
}

func TestRunClientLoopHonorsStopBeforeStartingUpload(t *testing.T) {
	var calls int
	err := runClientLoop(
		true,
		func() bool { return true },
		nil,
		func() error {
			calls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected clean stop, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected no upload attempts after stop, got %d", calls)
	}
}

func TestRunClientLoopStopsBetweenHammerUploads(t *testing.T) {
	var calls int
	stopped := false
	err := runClientLoop(
		true,
		func() bool { return stopped },
		nil,
		func() error {
			calls++
			stopped = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected clean stop, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one upload before stop, got %d", calls)
	}
}

func TestRunHammerClientsIgnoresNoActiveEpochAndSignalsStop(t *testing.T) {
	var calls int32
	err := runHammerClients(2, func(shouldStop func() bool, signalStop func()) error {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return errors.New("No active epoch")
		}
		deadline := time.After(time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-deadline:
				return errors.New("timed out waiting for hammer stop signal")
			case <-tick.C:
				if shouldStop() {
					return nil
				}
			}
		}
	})
	if err != nil {
		t.Fatalf("expected No active epoch to be treated as clean hammer completion, got %v", err)
	}
}

func TestRunHammerClientsReturnsUnexpectedError(t *testing.T) {
	wantErr := errors.New("unexpected EOF")
	err := runHammerClients(1, func(func() bool, func()) error {
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("expected unexpected error %v, got %v", wantErr, err)
	}
}

func TestRunHammerClientsReturnsOverloadError(t *testing.T) {
	wantErr := errors.New("server overloaded: ready queue full")
	err := runHammerClients(1, func(func() bool, func()) error {
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("expected overload error %v, got %v", wantErr, err)
	}
}

func TestRunHammerClientsUsesConfiguredConcurrency(t *testing.T) {
	var calls int32
	err := runHammerClients(3, func(func() bool, func()) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("runHammerClients returned unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 hammer workers, got %d", calls)
	}
}

func TestResolveHammerConcurrency(t *testing.T) {
	if *concurrencyFlag != 16 {
		t.Fatalf("expected default concurrency flag 16, got %d", *concurrencyFlag)
	}

	got, err := resolveHammerConcurrency(16)
	if err != nil {
		t.Fatalf("resolveHammerConcurrency returned unexpected error: %v", err)
	}
	if got != 16 {
		t.Fatalf("expected concurrency 16, got %d", got)
	}

	_, err = resolveHammerConcurrency(0)
	if err == nil || err.Error() != "-concurrency must be positive" {
		t.Fatalf("expected positive concurrency error, got %v", err)
	}
}
