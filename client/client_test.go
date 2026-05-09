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
			provider, err := resolveMessageProvider(tc.x, tc.y, tc.payload, db.TABLE_HEIGHT)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
			}
			req, err := provider()
			if err != nil {
				t.Fatalf("message provider returned unexpected error: %v", err)
			}
			if req.msg.X != tc.wantX || req.msg.Y != tc.wantY || req.routeRow != tc.wantY {
				t.Fatalf("expected coordinates (%d,%d) route row %d, got (%d,%d) route row %d", tc.wantX, tc.wantY, tc.wantY, req.msg.X, req.msg.Y, req.routeRow)
			}
			gotPayload := string(req.msg.Message[:len(tc.wantPayload)])
			if gotPayload != tc.wantPayload {
				t.Fatalf("expected payload %q, got %q", tc.wantPayload, gotPayload)
			}
		})
	}
}

func TestResolveMessageProviderRandomFallback(t *testing.T) {
	provider, err := resolveMessageProvider(-1, -1, "", db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	req, err := provider()
	if err != nil {
		t.Fatalf("message provider returned unexpected error: %v", err)
	}
	if req.msg == nil {
		t.Fatal("message provider returned nil message")
	}
	if req.msg.X < 0 || req.msg.X >= db.TABLE_WIDTH {
		t.Fatalf("random message X out of range: %d", req.msg.X)
	}
	if req.msg.Y < 0 || req.msg.Y >= db.TABLE_HEIGHT {
		t.Fatalf("random message Y out of range: %d", req.msg.Y)
	}
	if req.routeRow != req.msg.Y {
		t.Fatalf("expected local random route row %d, got %d", req.msg.Y, req.routeRow)
	}
}

func TestResolveMessageProviderCoordinatorGlobalRows(t *testing.T) {
	provider, err := resolveMessageProvider(3, db.TABLE_HEIGHT, "global", 2*db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	req, err := provider()
	if err != nil {
		t.Fatalf("message provider returned unexpected error: %v", err)
	}
	if req.routeRow != db.TABLE_HEIGHT {
		t.Fatalf("expected global route row %d, got %d", db.TABLE_HEIGHT, req.routeRow)
	}
	if req.msg.Y != 0 {
		t.Fatalf("expected local plaintext row 0 for global row %d, got %d", db.TABLE_HEIGHT, req.msg.Y)
	}
}

func TestResolveMessageProviderExactReusesMessage(t *testing.T) {
	provider, err := resolveMessageProvider(3, 5, "same", db.TABLE_HEIGHT)
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
	if first.msg != second.msg {
		t.Fatal("expected exact-message provider to reuse the deterministic message")
	}
	if first.routeRow != 5 || second.routeRow != 5 {
		t.Fatalf("expected exact route row 5, got first=%d second=%d", first.routeRow, second.routeRow)
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

	provider, err := resolveMessageProvider(-1, -1, "", db.TABLE_HEIGHT)
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
	if first.msg == second.msg || first.msg.X == second.msg.X || first.msg.Y == second.msg.Y {
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

	provider, err := resolveMessageProvider(-1, -1, "", db.TABLE_HEIGHT)
	if err != nil {
		t.Fatalf("resolveMessageProvider returned unexpected error: %v", err)
	}
	if _, err := provider(); err != wantErr {
		t.Fatalf("expected random error %v, got %v", wantErr, err)
	}
}

func TestUploadWithOverloadRetryDisabledReturnsOverload(t *testing.T) {
	wantErr := errors.New("server overloaded: ready queue full")
	msg := &db.Plaintext{X: 1, Y: 2}
	req := uploadRequest{msg: msg, routeRow: msg.Y}
	calls := 0
	err := uploadWithOverloadRetry(
		req,
		overloadRetryConfig{},
		func(got uploadRequest) error {
			calls++
			if got.msg != msg || got.routeRow != msg.Y {
				t.Fatal("expected upload to receive original message")
			}
			return wantErr
		},
		func(time.Duration) {
			t.Fatal("sleep should not run when retry is disabled")
		},
	)
	if err != wantErr {
		t.Fatalf("expected overload error %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected one upload attempt, got %d", calls)
	}
}

func TestUploadWithOverloadRetryRetriesSamePlaintext(t *testing.T) {
	msg := &db.Plaintext{X: 3, Y: 4}
	req := uploadRequest{msg: msg, routeRow: msg.Y}
	config := overloadRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     25 * time.Millisecond,
	}
	calls := 0
	var sleeps []time.Duration
	err := uploadWithOverloadRetry(
		req,
		config,
		func(got uploadRequest) error {
			calls++
			if got.msg != msg || got.routeRow != msg.Y {
				t.Fatal("expected retry to reuse the same plaintext")
			}
			if calls <= 3 {
				return errors.New("server overloaded: ready queue full")
			}
			return nil
		},
		func(delay time.Duration) {
			sleeps = append(sleeps, delay)
		},
	)
	if err != nil {
		t.Fatalf("expected retry to eventually succeed, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected four upload attempts, got %d", calls)
	}
	wantSleeps := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
	}
	for i := range wantSleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
		}
	}
}

func TestUploadWithOverloadRetryBackoffResetsAfterSuccess(t *testing.T) {
	config := overloadRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     40 * time.Millisecond,
	}
	var sleeps []time.Duration
	for i := 0; i < 2; i++ {
		calls := 0
		req := uploadRequest{msg: &db.Plaintext{X: i, Y: i}, routeRow: i}
		err := uploadWithOverloadRetry(
			req,
			config,
			func(uploadRequest) error {
				calls++
				if calls == 1 {
					return errors.New("server overloaded: ready queue full")
				}
				return nil
			},
			func(delay time.Duration) {
				sleeps = append(sleeps, delay)
			},
		)
		if err != nil {
			t.Fatalf("expected retry %d to succeed, got %v", i, err)
		}
	}
	wantSleeps := []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
	}
	for i := range wantSleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
		}
	}
}

func TestUploadWithOverloadRetryStopsOnNoActiveEpoch(t *testing.T) {
	config := overloadRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     20 * time.Millisecond,
	}
	calls := 0
	err := uploadWithOverloadRetry(
		uploadRequest{msg: &db.Plaintext{}},
		config,
		func(uploadRequest) error {
			calls++
			if calls == 1 {
				return errors.New("server overloaded: ready queue full")
			}
			return errors.New("No active epoch")
		},
		func(time.Duration) {},
	)
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected No active epoch after retry, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected two upload attempts, got %d", calls)
	}
}

func TestUploadWithOverloadRetryReturnsUnexpectedError(t *testing.T) {
	config := overloadRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     20 * time.Millisecond,
	}
	wantErr := errors.New("unexpected EOF")
	err := uploadWithOverloadRetry(
		uploadRequest{msg: &db.Plaintext{}},
		config,
		func(uploadRequest) error {
			return wantErr
		},
		func(time.Duration) {
			t.Fatal("sleep should not run for unexpected errors")
		},
	)
	if err != wantErr {
		t.Fatalf("expected unexpected error %v, got %v", wantErr, err)
	}
}

func TestIsCoordinatorNotActiveError(t *testing.T) {
	if !isCoordinatorNotActiveError(errors.New("Coordinator not active")) {
		t.Fatal("expected Coordinator not active to be classified")
	}
	if isCoordinatorNotActiveError(errors.New("No active epoch")) {
		t.Fatal("No active epoch must not be classified as coordinator-not-active")
	}
}

func TestIsShardSessionLostError(t *testing.T) {
	if !isShardSessionLostError(errors.New("Shard session lost")) {
		t.Fatal("expected Shard session lost to be classified")
	}
	if isShardSessionLostError(errors.New("Bogus UUID")) {
		t.Fatal("Bogus UUID must not be classified as shard-session-lost")
	}
}

func TestUploadWithCoordinatorRetryRetriesSamePlaintext(t *testing.T) {
	msg := &db.Plaintext{X: 5, Y: 6}
	req := uploadRequest{msg: msg, routeRow: msg.Y}
	config := coordinatorRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     25 * time.Millisecond,
	}
	calls := 0
	var sleeps []time.Duration
	err := uploadWithCoordinatorRetry(
		req,
		config,
		func(got uploadRequest) error {
			calls++
			if got.msg != msg || got.routeRow != msg.Y {
				t.Fatal("expected retry to reuse the same plaintext")
			}
			if calls <= 3 {
				return errors.New("Coordinator not active")
			}
			return nil
		},
		func(delay time.Duration) {
			sleeps = append(sleeps, delay)
		},
	)
	if err != nil {
		t.Fatalf("expected retry to eventually succeed, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected four upload attempts, got %d", calls)
	}
	wantSleeps := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
	}
	for i := range wantSleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("expected sleeps %v, got %v", wantSleeps, sleeps)
		}
	}
}

func TestUploadWithCoordinatorRetryRetriesShardSessionLostFromUpload1(t *testing.T) {
	msg := &db.Plaintext{X: 5, Y: 6}
	req := uploadRequest{msg: msg, routeRow: msg.Y}
	config := coordinatorRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     20 * time.Millisecond,
	}
	calls := 0
	err := uploadWithCoordinatorRetry(
		req,
		config,
		func(got uploadRequest) error {
			calls++
			if got.msg != msg || got.routeRow != msg.Y {
				t.Fatal("expected retry to reuse the same upload request")
			}
			if calls == 1 {
				return errors.New("Shard session lost")
			}
			return nil
		},
		func(time.Duration) {},
	)
	if err != nil {
		t.Fatalf("expected retry to eventually succeed, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected two upload attempts, got %d", calls)
	}
}

func TestUploadWithCoordinatorRetryDisabledReturnsCoordinatorNotActive(t *testing.T) {
	wantErr := errors.New("Coordinator not active")
	calls := 0
	err := uploadWithCoordinatorRetry(
		uploadRequest{msg: &db.Plaintext{}},
		coordinatorRetryConfig{},
		func(uploadRequest) error {
			calls++
			return wantErr
		},
		func(time.Duration) {
			t.Fatal("sleep should not run when retry is disabled")
		},
	)
	if err != wantErr {
		t.Fatalf("expected coordinator-not-active error %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected one upload attempt, got %d", calls)
	}
}

func TestUploadWithCoordinatorRetryDisabledReturnsShardSessionLost(t *testing.T) {
	wantErr := errors.New("Shard session lost")
	calls := 0
	err := uploadWithCoordinatorRetry(
		uploadRequest{msg: &db.Plaintext{}},
		coordinatorRetryConfig{},
		func(uploadRequest) error {
			calls++
			return wantErr
		},
		func(time.Duration) {
			t.Fatal("sleep should not run when retry is disabled")
		},
	)
	if err != wantErr {
		t.Fatalf("expected shard-session-lost error %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected one upload attempt, got %d", calls)
	}
}

func TestUploadWithCoordinatorRetryStopsOnNoActiveEpoch(t *testing.T) {
	config := coordinatorRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     20 * time.Millisecond,
	}
	err := uploadWithCoordinatorRetry(
		uploadRequest{msg: &db.Plaintext{}},
		config,
		func(uploadRequest) error {
			return errors.New("No active epoch")
		},
		func(time.Duration) {
			t.Fatal("sleep should not run for No active epoch")
		},
	)
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected No active epoch error, got %v", err)
	}
}

func TestUploadWithCoordinatorRetryReturnsFatalErrors(t *testing.T) {
	config := coordinatorRetryConfig{
		enabled:        true,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     20 * time.Millisecond,
	}
	for _, wantErr := range []error{
		errors.New("Bogus UUID"),
		errors.New("unexpected EOF"),
	} {
		err := uploadWithCoordinatorRetry(
			uploadRequest{msg: &db.Plaintext{}},
			config,
			func(uploadRequest) error {
				return wantErr
			},
			func(time.Duration) {
				t.Fatal("sleep should not run for fatal errors")
			},
		)
		if err != wantErr {
			t.Fatalf("expected fatal error %v, got %v", wantErr, err)
		}
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

func TestRunClientLoopDoesNotSignalStopOnCoordinatorNotActive(t *testing.T) {
	var signaled bool
	err := runClientLoop(
		true,
		func() bool { return false },
		func() { signaled = true },
		func() error {
			return errors.New("Coordinator not active")
		},
	)
	if err == nil || err.Error() != "Coordinator not active" {
		t.Fatalf("expected Coordinator not active error, got %v", err)
	}
	if signaled {
		t.Fatal("Coordinator not active must not signal hammer shutdown")
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
	var calls atomic.Int32
	err := runHammerClients(2, func(shouldStop func() bool, signalStop func()) error {
		call := calls.Add(1)
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
	var calls atomic.Int32
	err := runHammerClients(3, func(func() bool, func()) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("runHammerClients returned unexpected error: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 hammer workers, got %d", got)
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

func TestResolveOverloadRetryConfig(t *testing.T) {
	config, err := resolveOverloadRetryConfig(false, 0, 0)
	if err != nil {
		t.Fatalf("disabled retry should ignore backoff values, got %v", err)
	}
	if config.enabled {
		t.Fatal("expected disabled retry config")
	}

	config, err = resolveOverloadRetryConfig(true, 10, 250)
	if err != nil {
		t.Fatalf("resolveOverloadRetryConfig returned unexpected error: %v", err)
	}
	if !config.enabled || config.initialBackoff != 10*time.Millisecond || config.maxBackoff != 250*time.Millisecond {
		t.Fatalf("unexpected retry config: %+v", config)
	}

	tests := []struct {
		name      string
		initialMS uint
		maxMS     uint
		wantErr   string
	}{
		{
			name:      "zero initial",
			initialMS: 0,
			maxMS:     250,
			wantErr:   "-overload-backoff-initial-ms must be positive",
		},
		{
			name:      "zero max",
			initialMS: 10,
			maxMS:     0,
			wantErr:   "-overload-backoff-max-ms must be positive",
		},
		{
			name:      "max below initial",
			initialMS: 20,
			maxMS:     10,
			wantErr:   "-overload-backoff-max-ms must be greater than or equal to -overload-backoff-initial-ms",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveOverloadRetryConfig(true, tc.initialMS, tc.maxMS)
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("expected error %q, got %v", tc.wantErr, err)
			}
		})
	}
}
