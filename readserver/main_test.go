package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

type fakeFetcher struct {
	objects map[string][]byte
}

func (f *fakeFetcher) GetObject(_ context.Context, _ string, key string) ([]byte, error) {
	data, ok := f.objects[key]
	if !ok {
		return nil, errors.New("missing object")
	}
	return data, nil
}

func testTableAndManifest(t *testing.T, shardID int, startRow int, epoch int64, payload string, x int, localY int) ([]byte, []byte) {
	t.Helper()
	table := make([]byte, expectedTableBytes())
	offset := localY*db.TABLE_WIDTH*db.SLOT_LENGTH + x*db.SLOT_LENGTH
	copy(table[offset:offset+db.SLOT_LENGTH], []byte(payload))
	sum := sha256.Sum256(table)
	manifest := db.TableManifest{
		EpochID:        epoch,
		ShardID:        shardID,
		GlobalStartRow: startRow,
		GlobalEndRow:   startRow + db.TABLE_HEIGHT,
		TableHeight:    db.TABLE_HEIGHT,
		TableWidth:     db.TABLE_WIDTH,
		SlotLength:     db.SLOT_LENGTH,
		ByteLength:     len(table),
		SHA256Hex:      hex.EncodeToString(sum[:]),
		TableKey:       db.ResultTableKey("prefix", shardID),
		ManifestKey:    db.ResultManifestKey("prefix", shardID),
		PublishedAt:    time.Unix(100, 0).UTC(),
	}
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest failed: %v", err)
	}
	return table, rawManifest
}

func TestReadServerLoadsAndReadsSlot(t *testing.T) {
	table, manifest := testTableAndManifest(t, 1, db.TABLE_HEIGHT, 9, "hello", 3, 2)
	fetcher := &fakeFetcher{objects: map[string][]byte{
		db.ResultManifestKey("prefix", 1): manifest,
		db.ResultTableKey("prefix", 1):    table,
	}}
	server, err := newReadServer("bucket", "prefix", []shardConfig{{ID: 1, StartRow: db.TABLE_HEIGHT, EndRow: 2 * db.TABLE_HEIGHT}}, fetcher, "test-server")
	if err != nil {
		t.Fatalf("newReadServer failed: %v", err)
	}
	if server.ready() {
		t.Fatal("server should not be ready before loading")
	}
	if err := server.refreshShard(context.Background(), server.shards[0]); err != nil {
		t.Fatalf("refreshShard failed: %v", err)
	}
	if !server.ready() {
		t.Fatal("server should be ready after loading")
	}
	slot, loaded, err := server.readSlot(3, db.TABLE_HEIGHT+2)
	if err != nil {
		t.Fatalf("readSlot failed: %v", err)
	}
	if loaded.manifest.EpochID != 9 {
		t.Fatalf("expected epoch 9, got %d", loaded.manifest.EpochID)
	}
	if got := string(slot[:5]); got != "hello" {
		t.Fatalf("expected payload hello, got %q", got)
	}
}

func TestReadServerRejectsOutOfRangeCoordinates(t *testing.T) {
	table, manifest := testTableAndManifest(t, 0, 0, 1, "hello", 0, 0)
	fetcher := &fakeFetcher{objects: map[string][]byte{
		db.ResultManifestKey("", 0):    manifest,
		db.ResultTableKey("prefix", 0): table,
	}}
	server, err := newReadServer("bucket", "", []shardConfig{{ID: 0, StartRow: 0, EndRow: db.TABLE_HEIGHT}}, fetcher, "test-server")
	if err != nil {
		t.Fatalf("newReadServer failed: %v", err)
	}
	server.loaded[0] = loadedShard{config: server.shards[0], table: table, manifest: db.TableManifest{EpochID: 1}}
	if _, _, err := server.readSlot(db.TABLE_WIDTH, 0); err == nil {
		t.Fatal("expected out-of-range x to fail")
	}
	if _, _, err := server.readSlot(0, db.TABLE_HEIGHT); err == nil {
		t.Fatal("expected out-of-range y to fail")
	}
}

func TestHealthzRequiresLoadedShards(t *testing.T) {
	fetcher := &fakeFetcher{objects: map[string][]byte{}}
	server, err := newReadServer("bucket", "", []shardConfig{{ID: 0, StartRow: 0, EndRow: db.TABLE_HEIGHT}}, fetcher, "test-server")
	if err != nil {
		t.Fatalf("newReadServer failed: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.handleHealth(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before load, got %d", rec.Code)
	}
	server.loaded[0] = loadedShard{config: server.shards[0], table: make([]byte, expectedTableBytes()), manifest: db.TableManifest{EpochID: 1}}
	rec = httptest.NewRecorder()
	server.handleHealth(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after load, got %d", rec.Code)
	}
}

func TestValidateTableRejectsChecksumMismatch(t *testing.T) {
	table, manifestData := testTableAndManifest(t, 0, 0, 1, "hello", 0, 0)
	var manifest db.TableManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("unmarshal manifest failed: %v", err)
	}
	table[0] ^= 1
	if err := validateTable(table, manifest); err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestValidateTableRejectsWrongSize(t *testing.T) {
	table, manifestData := testTableAndManifest(t, 0, 0, 1, "hello", 0, 0)
	var manifest db.TableManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("unmarshal manifest failed: %v", err)
	}
	if err := validateTable(table[:len(table)-1], manifest); err == nil {
		t.Fatal("expected wrong size to fail")
	}
}
