package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"bitbucket.org/henrycg/riposte/db"
)

const pollInterval = 2 * time.Second

type shardConfig struct {
	ID       int `json:"id"`
	StartRow int `json:"start_row"`
	EndRow   int `json:"end_row"`
}

type shardFlags []shardConfig

func (s *shardFlags) String() string {
	parts := make([]string, 0, len(*s))
	for _, shard := range *s {
		parts = append(parts, fmt.Sprintf("%d,%d,%d", shard.ID, shard.StartRow, shard.EndRow))
	}
	return strings.Join(parts, ";")
}

func (s *shardFlags) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 3 {
		return fmt.Errorf("invalid shard %q: expected id,start,end", value)
	}
	id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return fmt.Errorf("invalid shard id: %w", err)
	}
	start, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return fmt.Errorf("invalid shard start row: %w", err)
	}
	end, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return fmt.Errorf("invalid shard end row: %w", err)
	}
	if id < 0 || start < 0 || end <= start {
		return fmt.Errorf("invalid shard range %q", value)
	}
	*s = append(*s, shardConfig{ID: id, StartRow: start, EndRow: end})
	return nil
}

type objectFetcher interface {
	GetObject(ctx context.Context, bucket string, key string) ([]byte, error)
}

type s3ObjectFetcher struct {
	client *s3.Client
}

func (f *s3ObjectFetcher) GetObject(ctx context.Context, bucket string, key string) ([]byte, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

type loadedShard struct {
	config      shardConfig
	manifest    db.TableManifest
	table       []byte
	loadedAt    time.Time
	lastError   string
	lastErrorAt time.Time
}

type readServer struct {
	mu          sync.RWMutex
	bucket      string
	prefix      string
	shards      []shardConfig
	fetcher     objectFetcher
	serverID    string
	loaded      map[int]loadedShard
	lastRefresh time.Time
}

func newReadServer(bucket string, prefix string, shards []shardConfig, fetcher objectFetcher, serverID string) (*readServer, error) {
	if bucket == "" {
		return nil, errors.New("-result-s3-bucket is required")
	}
	if len(shards) == 0 {
		return nil, errors.New("at least one -shard is required")
	}
	if fetcher == nil {
		return nil, errors.New("object fetcher is required")
	}
	for _, shard := range shards {
		if shard.EndRow-shard.StartRow != db.TABLE_HEIGHT {
			return nil, fmt.Errorf("shard %d range [%d,%d) must have height %d", shard.ID, shard.StartRow, shard.EndRow, db.TABLE_HEIGHT)
		}
	}
	return &readServer{
		bucket:   bucket,
		prefix:   strings.Trim(strings.TrimSpace(prefix), "/"),
		shards:   append([]shardConfig(nil), shards...),
		fetcher:  fetcher,
		serverID: serverID,
		loaded:   make(map[int]loadedShard),
	}, nil
}

func (s *readServer) startPolling(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		s.refreshAll(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshAll(ctx)
			}
		}
	}()
}

func (s *readServer) refreshAll(ctx context.Context) {
	for _, shard := range s.shards {
		if err := s.refreshShard(ctx, shard); err != nil {
			log.Printf("refresh shard %d failed: %v", shard.ID, err)
			s.recordShardError(shard, err)
		}
	}
	s.mu.Lock()
	s.lastRefresh = time.Now().UTC()
	s.mu.Unlock()
}

func (s *readServer) refreshShard(ctx context.Context, shard shardConfig) error {
	manifestKey := db.ResultManifestKey(s.prefix, shard.ID)
	raw, err := s.fetcher.GetObject(ctx, s.bucket, manifestKey)
	if err != nil {
		return err
	}
	var manifest db.TableManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	if err := validateManifest(manifest, shard); err != nil {
		return err
	}

	s.mu.RLock()
	current, ok := s.loaded[shard.ID]
	s.mu.RUnlock()
	if ok && current.manifest.EpochID == manifest.EpochID && current.manifest.SHA256Hex == manifest.SHA256Hex {
		return nil
	}

	table, err := s.fetcher.GetObject(ctx, s.bucket, manifest.TableKey)
	if err != nil {
		return err
	}
	if err := validateTable(table, manifest); err != nil {
		return err
	}

	s.mu.Lock()
	s.loaded[shard.ID] = loadedShard{
		config:   shard,
		manifest: manifest,
		table:    table,
		loadedAt: time.Now().UTC(),
	}
	s.mu.Unlock()
	log.Printf("loaded shard=%d epoch=%d bytes=%d", shard.ID, manifest.EpochID, len(table))
	return nil
}

func validateManifest(manifest db.TableManifest, shard shardConfig) error {
	if manifest.ShardID != shard.ID {
		return fmt.Errorf("manifest shard id %d does not match configured shard %d", manifest.ShardID, shard.ID)
	}
	if manifest.GlobalStartRow != shard.StartRow || manifest.GlobalEndRow != shard.EndRow {
		return fmt.Errorf("manifest range [%d,%d) does not match configured range [%d,%d)", manifest.GlobalStartRow, manifest.GlobalEndRow, shard.StartRow, shard.EndRow)
	}
	if manifest.TableHeight != db.TABLE_HEIGHT || manifest.TableWidth != db.TABLE_WIDTH || manifest.SlotLength != db.SLOT_LENGTH {
		return fmt.Errorf("manifest table dimensions do not match build constants")
	}
	if manifest.ByteLength != expectedTableBytes() {
		return fmt.Errorf("manifest byte length %d does not match expected %d", manifest.ByteLength, expectedTableBytes())
	}
	if manifest.TableKey == "" || manifest.SHA256Hex == "" {
		return errors.New("manifest missing table key or sha256")
	}
	return nil
}

func validateTable(table []byte, manifest db.TableManifest) error {
	if len(table) != manifest.ByteLength {
		return fmt.Errorf("table length %d does not match manifest %d", len(table), manifest.ByteLength)
	}
	sum := sha256.Sum256(table)
	if got := hex.EncodeToString(sum[:]); got != manifest.SHA256Hex {
		return fmt.Errorf("table sha256 %s does not match manifest %s", got, manifest.SHA256Hex)
	}
	return nil
}

func expectedTableBytes() int {
	return db.TABLE_HEIGHT * db.TABLE_WIDTH * db.SLOT_LENGTH
}

func (s *readServer) recordShardError(shard shardConfig, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.loaded[shard.ID]
	current.config = shard
	current.lastError = err.Error()
	current.lastErrorAt = time.Now().UTC()
	s.loaded[shard.ID] = current
}

func (s *readServer) ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, shard := range s.shards {
		loaded := s.loaded[shard.ID]
		if loaded.table == nil {
			return false
		}
	}
	return true
}

func (s *readServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if !s.ready() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *readServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	type shardStatus struct {
		ID          int    `json:"id"`
		StartRow    int    `json:"start_row"`
		EndRow      int    `json:"end_row"`
		EpochID     int64  `json:"epoch_id"`
		Loaded      bool   `json:"loaded"`
		LoadedAt    string `json:"loaded_at"`
		LastError   string `json:"last_error"`
		LastErrorAt string `json:"last_error_at"`
	}
	out := struct {
		ServerID    string        `json:"server_id"`
		Ready       bool          `json:"ready"`
		LastRefresh string        `json:"last_refresh"`
		Shards      []shardStatus `json:"shards"`
	}{
		ServerID: s.serverID,
		Ready:    true,
		Shards:   make([]shardStatus, 0, len(s.shards)),
	}
	if !s.lastRefresh.IsZero() {
		out.LastRefresh = s.lastRefresh.Format(time.RFC3339)
	}
	for _, shard := range s.shards {
		loaded := s.loaded[shard.ID]
		status := shardStatus{
			ID:        shard.ID,
			StartRow:  shard.StartRow,
			EndRow:    shard.EndRow,
			EpochID:   loaded.manifest.EpochID,
			Loaded:    loaded.table != nil,
			LastError: loaded.lastError,
		}
		if !loaded.loadedAt.IsZero() {
			status.LoadedAt = loaded.loadedAt.Format(time.RFC3339)
		}
		if !loaded.lastErrorAt.IsZero() {
			status.LastErrorAt = loaded.lastErrorAt.Format(time.RFC3339)
		}
		if !status.Loaded {
			out.Ready = false
		}
		out.Shards = append(out.Shards, status)
	}
	writeJSON(w, out)
}

func (s *readServer) handleRead(w http.ResponseWriter, r *http.Request) {
	x, err := parseQueryInt(r, "x")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	y, err := parseQueryInt(r, "y")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slot, loaded, err := s.readSlot(x, y)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := struct {
		EpochID    int64  `json:"epoch_id"`
		ShardID    int    `json:"shard_id"`
		X          int    `json:"x"`
		Y          int    `json:"y"`
		MessageHex string `json:"message_hex"`
		ServerID   string `json:"server_id"`
	}{
		EpochID:    loaded.manifest.EpochID,
		ShardID:    loaded.config.ID,
		X:          x,
		Y:          y,
		MessageHex: hex.EncodeToString(slot),
		ServerID:   s.serverID,
	}
	writeJSON(w, resp)
}

func (s *readServer) readSlot(x int, globalY int) ([]byte, loadedShard, error) {
	if x < 0 || x >= db.TABLE_WIDTH {
		return nil, loadedShard{}, fmt.Errorf("x must be in [0,%d)", db.TABLE_WIDTH)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, shard := range s.shards {
		if globalY < shard.StartRow || globalY >= shard.EndRow {
			continue
		}
		loaded := s.loaded[shard.ID]
		if loaded.table == nil {
			return nil, loadedShard{}, fmt.Errorf("shard %d not loaded", shard.ID)
		}
		localY := globalY - shard.StartRow
		offset := localY*db.TABLE_WIDTH*db.SLOT_LENGTH + x*db.SLOT_LENGTH
		slot := make([]byte, db.SLOT_LENGTH)
		copy(slot, loaded.table[offset:offset+db.SLOT_LENGTH])
		return slot, loaded, nil
	}
	return nil, loadedShard{}, fmt.Errorf("y must be in a configured shard range")
}

func parseQueryInt(r *http.Request, name string) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, fmt.Errorf("missing %s", name)
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func defaultServerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "readserver"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func main() {
	var listen string
	var bucket string
	var prefix string
	var awsRegion string
	var shards shardFlags
	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&bucket, "result-s3-bucket", "", "S3 bucket containing current result tables")
	flag.StringVar(&prefix, "result-s3-prefix", "", "S3 key prefix for current result tables")
	flag.StringVar(&awsRegion, "aws-region", "", "AWS region override")
	flag.Var(&shards, "shard", "Shard config id,start,end; may be repeated")
	flag.Parse()

	options := []func(*awsconfig.LoadOptions) error{}
	if awsRegion != "" {
		options = append(options, awsconfig.WithRegion(awsRegion))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		log.Fatal(err)
	}
	server, err := newReadServer(bucket, prefix, shards, &s3ObjectFetcher{client: s3.NewFromConfig(cfg)}, defaultServerID())
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	server.startPolling(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.handleHealth)
	mux.HandleFunc("/status", server.handleStatus)
	mux.HandleFunc("/read", server.handleRead)
	log.Printf("readserver listening on %s", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}
