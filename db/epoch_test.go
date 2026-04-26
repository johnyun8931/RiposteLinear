package db

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestLeaderServer() *Server {
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})
	s.incoming1 = make(chan bool, 1)
	s.incoming2 = make(chan bool, 1)
	s.incoming3 = make(chan bool, 1)
	s.ready = make(chan int64, 4)
	s.incoming1 <- true
	s.incoming2 <- true
	s.incoming3 <- true
	s.setPeerConnectionState(PeerConnectionsReady, nil)
	return s
}

func newTestLeaderServerWithPeerState(state PeerConnectionState, err error) *Server {
	s := newTestLeaderServer()
	s.setPeerConnectionState(state, err)
	return s
}

func TestUploadRejectedWithoutActiveEpoch(t *testing.T) {
	s := newTestLeaderServer()

	var reply UploadReply1
	err := s.Upload1(&UploadArgs1{}, &reply)
	if err == nil || err.Error() != "No active epoch" {
		t.Fatalf("expected no active epoch error, got %v", err)
	}
}

func TestStartEpochSetsActiveState(t *testing.T) {
	s := newTestLeaderServer()

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

func TestStartEpochHonorsCoordinatorMetadataAndAbort(t *testing.T) {
	s := newTestLeaderServer()

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{
		DurationSeconds: 30,
		EpochID:         9,
		StartUnix:       170,
	}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}

	meta := s.currentEpochMeta()
	if meta.ID != 9 {
		t.Fatalf("expected epoch id 9, got %d", meta.ID)
	}
	if !meta.StartTime.Equal(time.Unix(170, 0).UTC()) {
		t.Fatalf("unexpected start time %v", meta.StartTime)
	}

	var abortReply AbortEpochReply
	if err := s.AbortEpoch(&AbortEpochArgs{EpochID: 9}, &abortReply); err != nil {
		t.Fatalf("abort epoch failed: %v", err)
	}
	meta = s.currentEpochMeta()
	if meta.State != EpochStateNoActive || meta.ID != 0 {
		t.Fatalf("expected no-active epoch after abort, got %+v", meta)
	}
}

func TestFinishEpochTransitionsToCompleted(t *testing.T) {
	s := newTestLeaderServer()
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

func TestUpload2RejectsBogusUUID(t *testing.T) {
	s := newTestLeaderServer()

	var reply UploadReply2
	err := s.Upload2(&UploadArgs2{Uuid: 12345}, &reply)
	if err == nil || err.Error() != "Bogus UUID" {
		t.Fatalf("expected bogus uuid error, got %v", err)
	}
}

func TestUpload3RejectsBogusUUID(t *testing.T) {
	s := newTestLeaderServer()

	var reply UploadReply3
	err := s.Upload3(&UploadArgs3{Uuid: 12345}, &reply)
	if err == nil || err.Error() != "Bogus UUID" {
		t.Fatalf("expected bogus uuid error, got %v", err)
	}
}

func TestStartEpochRejectsNonPositiveDuration(t *testing.T) {
	s := newTestLeaderServer()

	var reply StartEpochReply
	err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 0}, &reply)
	if err == nil || err.Error() != "Epoch duration must be positive" {
		t.Fatalf("expected duration error, got %v", err)
	}
}

func TestStartEpochRejectsWhilePeerConnectionsStillInitializing(t *testing.T) {
	s := newTestLeaderServerWithPeerState(PeerConnectionsConnecting, nil)

	var reply StartEpochReply
	err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply)
	if err == nil || err.Error() != "leader not ready for start epoch: peer RPC connections still initializing" {
		t.Fatalf("expected readiness error, got %v", err)
	}
}

func TestStartEpochRejectsAfterPeerConnectionFailure(t *testing.T) {
	s := newTestLeaderServerWithPeerState(PeerConnectionsFailed, errors.New("dial tcp 127.0.0.1:9001: connect: refused"))

	var reply StartEpochReply
	err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply)
	if err == nil || !strings.Contains(err.Error(), "leader not ready for start epoch: peer RPC connection setup failed:") {
		t.Fatalf("expected peer connection failure, got %v", err)
	}
}

func TestStartEpochRejectsWhileActive(t *testing.T) {
	s := newTestLeaderServer()

	var first StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &first); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	defer s.stopEpochTimer()

	var second StartEpochReply
	err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &second)
	if err == nil || err.Error() != "An epoch is already in progress" {
		t.Fatalf("expected in-progress error, got %v", err)
	}
}

func TestEpochStatusReportsLifecycle(t *testing.T) {
	s := newTestLeaderServer()
	s.mergeFn = func() (string, error) { return "/tmp/epoch-000001-server-0.json", nil }

	var status EpochStatusReply
	if err := s.EpochStatus(&EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("initial status failed: %v", err)
	}
	if status.State != EpochStateNoActive.String() || status.Accepting {
		t.Fatalf("unexpected initial status: %+v", status)
	}

	var startReply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &startReply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	if err := s.EpochStatus(&EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("active status failed: %v", err)
	}
	if status.State != EpochStateActive.String() || !status.Accepting || status.EpochID != 1 {
		t.Fatalf("unexpected active status: %+v", status)
	}

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("finish epoch failed: %v", err)
	}
	if err := s.EpochStatus(&EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("completed status failed: %v", err)
	}
	if status.State != EpochStateCompleted.String() || status.Accepting {
		t.Fatalf("unexpected completed status: %+v", status)
	}
	if status.LastResult != "/tmp/epoch-000001-server-0.json" {
		t.Fatalf("unexpected result path %q", status.LastResult)
	}
}

func TestFinishEpochMergeFailureLeavesEpochIncomplete(t *testing.T) {
	s := newTestLeaderServer()
	s.mergeFn = func() (string, error) { return "", errors.New("merge failed") }

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	err := s.finishEpoch()
	if err == nil || err.Error() != "merge failed" {
		t.Fatalf("expected merge failure, got %v", err)
	}

	var status EpochStatusReply
	if err := s.EpochStatus(&EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("status failed: %v", err)
	}
	if status.State != EpochStateMerging.String() {
		t.Fatalf("expected merging state after failed merge, got %+v", status)
	}
	if status.Accepting {
		t.Fatalf("expected writes to be rejected after failed merge, got %+v", status)
	}
	if status.LastResult != "" {
		t.Fatalf("expected no result path after failed merge, got %q", status.LastResult)
	}
}

func TestFinishEpochRejectsWhenPeerConnectionsNotReady(t *testing.T) {
	s := newTestLeaderServer()

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()
	s.setPeerConnectionState(PeerConnectionsConnecting, nil)

	err := s.finishEpoch()
	if err == nil || err.Error() != "leader not ready for merge: peer RPC connections still initializing" {
		t.Fatalf("expected readiness error from finishEpoch, got %v", err)
	}
}

func TestUploadSessionAssemblyAndReadyQueue(t *testing.T) {
	s := newTestLeaderServer()

	var startReply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &startReply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	defer s.stopEpochTimer()

	var up1Reply UploadReply1
	if err := s.Upload1(&UploadArgs1{}, &up1Reply); err != nil {
		t.Fatalf("upload1 failed: %v", err)
	}

	up2Args := &UploadArgs2{Uuid: up1Reply.Uuid, HashKey: up1Reply.HashKey}
	var up2Reply UploadReply2
	if err := s.Upload2(up2Args, &up2Reply); err != nil {
		t.Fatalf("upload2 failed: %v", err)
	}

	up3Args := &UploadArgs3{Uuid: up1Reply.Uuid, HashKey: up1Reply.HashKey}
	var up3Reply UploadReply3
	if err := s.Upload3(up3Args, &up3Reply); err != nil {
		t.Fatalf("upload3 failed: %v", err)
	}

	replyCh := make(chan takeAcceptedSessionResult, 1)
	s.controlCh <- takeAcceptedSessionCommand{uuid: up1Reply.Uuid, reply: replyCh}
	session := <-replyCh
	if !session.ok || session.session == nil {
		t.Fatalf("expected accepted session for uuid %d", up1Reply.Uuid)
	}
	if session.session.args1 == nil || session.session.args2 == nil || session.session.args3 == nil {
		t.Fatalf("expected all upload stages to be present, got %+v", session.session)
	}
	if session.session.args2.Uuid != up1Reply.Uuid || session.session.args3.Uuid != up1Reply.Uuid {
		t.Fatalf("expected consistent uuids in session, got %+v", session.session)
	}

	select {
	case queued := <-s.ready:
		if queued != up1Reply.Uuid {
			t.Fatalf("expected queued uuid %d, got %d", up1Reply.Uuid, queued)
		}
	default:
		t.Fatal("expected upload3 to enqueue one request")
	}

	select {
	case queued := <-s.ready:
		t.Fatalf("expected only one queued request, got extra uuid %d", queued)
	default:
	}
}

func TestFinishEpochUpdatesLastResultPath(t *testing.T) {
	s := newTestLeaderServer()
	s.mergeFn = func() (string, error) { return "/tmp/final-result.json", nil }

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("finish epoch failed: %v", err)
	}
	if got := s.getLastResultPath(); got != "/tmp/final-result.json" {
		t.Fatalf("expected last result path to be updated, got %q", got)
	}
}

func TestEpochTimerAutomaticallyCompletesEpoch(t *testing.T) {
	s := newTestLeaderServer()
	done := make(chan struct{}, 1)
	s.mergeFn = func() (string, error) {
		done <- struct{}{}
		return "/tmp/timer-result.json", nil
	}

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 1}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for epoch timer to trigger merge")
	}

	var status EpochStatusReply
	if err := s.EpochStatus(&EpochStatusArgs{}, &status); err != nil {
		t.Fatalf("status failed: %v", err)
	}
	if status.State != EpochStateCompleted.String() || status.Accepting {
		t.Fatalf("unexpected timer-driven completed status: %+v", status)
	}
	if status.LastResult != "/tmp/timer-result.json" {
		t.Fatalf("unexpected timer-driven result path: %q", status.LastResult)
	}
}

func TestFinishEpochRunsMergeExactlyOnce(t *testing.T) {
	s := newTestLeaderServer()
	mergeCalls := 0
	s.mergeFn = func() (string, error) {
		mergeCalls++
		return "", nil
	}

	var reply StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &reply); err != nil {
		t.Fatalf("start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("finish epoch failed: %v", err)
	}
	if mergeCalls != 1 {
		t.Fatalf("expected one merge call, got %d", mergeCalls)
	}

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("second finish epoch failed: %v", err)
	}
	if mergeCalls != 1 {
		t.Fatalf("expected merge to remain at one call, got %d", mergeCalls)
	}
}

func TestSecondEpochCanStartAfterCompletion(t *testing.T) {
	s := newTestLeaderServer()
	s.mergeFn = func() (string, error) { return "", nil }

	var first StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &first); err != nil {
		t.Fatalf("first start epoch failed: %v", err)
	}
	s.stopEpochTimer()

	if err := s.finishEpoch(); err != nil {
		t.Fatalf("finish epoch failed: %v", err)
	}

	var second StartEpochReply
	if err := s.StartEpoch(&StartEpochArgs{DurationSeconds: 60}, &second); err != nil {
		t.Fatalf("second start epoch failed: %v", err)
	}
	defer s.stopEpochTimer()

	if second.EpochID != first.EpochID+1 {
		t.Fatalf("expected second epoch id %d, got %d", first.EpochID+1, second.EpochID)
	}
	if !s.acceptingWrites() {
		t.Fatal("expected writes to be accepted in second epoch")
	}
}

func TestWritePublishedResultCreatesDeterministicFile(t *testing.T) {
	dir := t.TempDir()
	s := NewServer(0, []string{"127.0.0.1:9000", "127.0.0.1:9001"})
	s.SetShardID(3)
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

	expected := filepath.Join(dir, "epoch-000007-shard-3-server-0.json")
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
	if !result.StartTime.Equal(meta.StartTime) {
		t.Fatalf("expected start time %v, got %v", meta.StartTime, result.StartTime)
	}
	if !result.EndTime.Equal(meta.EndTime) {
		t.Fatalf("expected end time %v, got %v", meta.EndTime, result.EndTime)
	}
	if result.DurationSeconds != meta.DurationSeconds {
		t.Fatalf("expected duration %d, got %d", meta.DurationSeconds, result.DurationSeconds)
	}
	if result.ShardID != 3 {
		t.Fatalf("expected shard id 3, got %d", result.ShardID)
	}
	if result.NonZeroSlotCount != 1 || len(result.Slots) != 1 {
		t.Fatalf("expected one non-zero slot, got count=%d slots=%d", result.NonZeroSlotCount, len(result.Slots))
	}
	if result.Slots[0].Row != 2 || result.Slots[0].Column != 3 {
		t.Fatalf("unexpected slot coordinates: %+v", result.Slots[0])
	}
	expectedHex := hex.EncodeToString(matrix[2][3*SLOT_LENGTH : (3+1)*SLOT_LENGTH])
	if result.Slots[0].MessageHex != expectedHex {
		t.Fatalf("expected message hex %s, got %s", expectedHex, result.Slots[0].MessageHex)
	}
}
