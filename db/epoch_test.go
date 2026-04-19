package db

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUploadRejectedWithoutActiveEpoch(t *testing.T) {
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})
	s.incoming1 = make(chan bool, 1)
	s.incoming1 <- true

	var reply UploadReply1
	err := s.Upload1(&UploadArgs1{}, &reply)
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
}

func TestStartEpochSetsActiveState(t *testing.T) {
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	defer s.stopEpochTimer()

	meta := s.currentEpochMeta()
	if meta.State != EpochStateActive {
		t.Fatalf("expected active state, got %v", meta.State)
	}
	if reply.EpochID != 1 {
		t.Fatalf("expected epoch id 1, got %d", reply.EpochID)
	}
	if !s.acceptingWrites() {
		t.Fatal("expected writes to be accepted")
	}
}

func TestFinishEpochTransitionsToCompleted(t *testing.T) {
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})
	s.mergeFn = func() (string, error) { return "", nil }

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("finish epoch failed: %v", err)
	}
	meta := s.currentEpochMeta()
	if meta.State != EpochStateCompleted {
		t.Fatalf("expected completed state, got %v", meta.State)
	}
	if s.acceptingWrites() {
		t.Fatal("expected writes to be rejected after epoch completion")
	}
}

func TestWritePublishedResultCreatesDeterministicFile(t *testing.T) {
	dir := t.TempDir()
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})
	s.SetResultsDir(dir)

	var matrix BitMatrix
	copy(matrix[2][3*SLOT_LENGTH:(3+1)*SLOT_LENGTH], []byte("hello"))
	meta := EpochMeta{
		ID:              7,
		State:           EpochStateCompleted,
		StartTime:       time.Unix(100, 0).UTC(),
		EndTime:         time.Unix(160, 0).UTC(),
		DurationSeconds: 60,
	}
	path, err := s.writePublishedResult(&matrix, meta, time.Unix(160, 0).UTC())
	if err != nil {
		t.Fatalf("writePublishedResult failed: %v", err)
	}

	expected := filepath.Join(dir, "epoch-000007-server-0.json")
	if path != expected {
		t.Fatalf("expected result path %s, got %s", expected, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read result file failed: %v", err)
	}

	var result PublishedResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal result failed: %v", err)
	}
	if result.EpochID != 7 {
		t.Fatalf("expected epoch id 7, got %d", result.EpochID)
	}
	if result.NonZeroSlotCount != 1 || len(result.Slots) != 1 {
		t.Fatalf("expected one non-zero slot, got count=%d slots=%d", result.NonZeroSlotCount, len(result.Slots))
	}
	if result.Slots[0].Row != 2 || result.Slots[0].Column != 3 {
		t.Fatalf("unexpected slot coordinates: %+v", result.Slots[0])
	}
}
