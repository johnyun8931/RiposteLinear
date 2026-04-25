package main

import "testing"

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

func TestResolveMessageInput(t *testing.T) {
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
			msg, err := resolveMessageInput(tc.x, tc.y, tc.payload)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMessageInput returned unexpected error: %v", err)
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

func TestResolveMessageInputRandomFallback(t *testing.T) {
	msg, err := resolveMessageInput(-1, -1, "")
	if err != nil {
		t.Fatalf("resolveMessageInput returned unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("resolveMessageInput returned nil message")
	}
	if msg.X < 0 || msg.X >= 256 {
		t.Fatalf("random message X out of range: %d", msg.X)
	}
	if msg.Y < 0 || msg.Y >= 256 {
		t.Fatalf("random message Y out of range: %d", msg.Y)
	}
}
