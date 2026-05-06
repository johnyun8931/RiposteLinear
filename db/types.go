package db

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bitbucket.org/henrycg/riposte/mulproof"
	"bitbucket.org/henrycg/riposte/prf"
)

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 2
const NUM_SERVERS = 2 //1 << NUM_DIMENSIONS

// Size of a side of the data array
const TABLE_WIDTH int = 256
const TABLE_HEIGHT int = 65536 / TABLE_WIDTH

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 64

// Length of plaintext messages (in bytes)
const SLOT_LENGTH int = 160 // 64 KB

type BitMatrix [TABLE_HEIGHT]BitMatrixRow
type BitMatrixRow [TABLE_WIDTH * SLOT_LENGTH]byte

type SlotTable struct {
	table BitMatrix

	// Each worker gets its own copy of the whole table
	freeTables  chan int
	localTables [WORKER_THREADS]BitMatrix
}

var IntModulus *big.Int

type DbState int

const (
	EpochStateNoActive DbState = iota
	EpochStateActive
	EpochStateClosing
	EpochStateMerging
	EpochStateCompleted
)

type PeerConnectionState int

const (
	PeerConnectionsConnecting PeerConnectionState = iota
	PeerConnectionsReady
	PeerConnectionsFailed
)

type SlotContents [SLOT_LENGTH]byte

type Plaintext struct {
	Message SlotContents
	X       int
	Y       int
}

type EncryptedInsertQuery struct {
	SenderPublicKey [32]byte
	Nonce           [24]byte
	Ciphertext      []byte
}

type EncryptedInsertQuery2 struct {
	SenderPublicKey [32]byte
	Nonce           [24]byte
	Ciphertext      []byte
}

type EncryptedInsertQuery3 struct {
	SenderPublicKey [32]byte
	Nonce           [24]byte
	Ciphertext      []byte
}

type UploadArgs1 struct {
	RouteRow int
	Query    [NUM_SERVERS]EncryptedInsertQuery
}

type UploadArgs2 struct {
	Uuid    int64
	HashKey [32]byte
	Query   [NUM_SERVERS]EncryptedInsertQuery
}

type UploadArgs3 struct {
	Uuid    int64
	HashKey [32]byte
	Query   [NUM_SERVERS]EncryptedInsertQuery
}

type AcceptQueryTuple struct {
	hashKey   [32]byte
	challenge [16]byte

	args1 *UploadArgs1
	args2 *UploadArgs2
	args3 *UploadArgs3
}

type InsertQueryTuple struct {
	hashKey   [32]byte
	challenge [16]byte

	q1 InsertQuery1
	q2 InsertQuery2
	q3 InsertQuery3
}

type InsertQuery1 struct {
	KeyIndex    int
	Keys        [TABLE_HEIGHT]prf.Key
	KeyMask     [TABLE_HEIGHT]bool
	MessageMask BitMatrixRow
}

type InsertQuery2 struct {
	MsgShare *big.Int
}

type InsertQuery3 struct {
	TShare1 *big.Int
	TShare2 *big.Int
	TProof1 mulproof.ProofShare
	TProof2 mulproof.ProofShare
}

type UploadReply1 struct {
	Uuid    int64
	HashKey [32]byte
}

type UploadReply2 struct {
	Challenge [16]byte
	Magic     int
}

type UploadReply3 struct {
	Magic int
}

type DumpReply struct {
	Entries *BitMatrix
}

type PrepareArgs struct {
	Uuid        int64
	HashKey     [32]byte
	Challenge   [16]byte
	RandomPoint *big.Int
	Query1      EncryptedInsertQuery
	Query2      EncryptedInsertQuery
	Query3      EncryptedInsertQuery
}

type PrepareReply struct {
	AnsShare1 *mulproof.AnsShare
	AnsShare2 *mulproof.AnsShare

	OutShare *big.Int
}

type CommitArgs struct {
	// COMMIT
	Uuid   int64
	Commit bool
}

type CommitReply struct {
	// Ack
	// uuid
}

type PlaintextArgs struct {
	Plaintext *BitMatrix
}

type PlaintextReply struct {
	// Nothing
}

type StartEpochArgs struct {
	DurationSeconds int64
	EpochID         int64
	StartUnix       int64
}

type StartEpochReply struct {
	EpochID      int64
	State        string
	StartUnix    int64
	EndUnix      int64
	DurationSecs int64
}

type EpochStatusArgs struct{}

type EpochStatusReply struct {
	EpochID      int64
	State        string
	StartUnix    int64
	EndUnix      int64
	DurationSecs int64
	Accepting    bool
	LastResult   string
}

type ReadLatestArgs struct {
	ShardID int
}

type ReadLatestReply struct {
	EpochID   int64
	ShardID   int
	ResultKey string
	Content   []byte
}

type AbortEpochArgs struct {
	EpochID int64
}

type AbortEpochReply struct{}

type EpochMeta struct {
	ID              int64     `json:"id"`
	State           DbState   `json:"state"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationSeconds int64     `json:"duration_seconds"`
}

type PublishedSlot struct {
	Row        int    `json:"row"`
	Column     int    `json:"column"`
	MessageHex string `json:"message_hex"`
}

type PublishedResult struct {
	EpochID          int64           `json:"epoch_id"`
	StartTime        time.Time       `json:"start_time"`
	EndTime          time.Time       `json:"end_time"`
	DurationSeconds  int64           `json:"duration_seconds"`
	ShardID          int             `json:"shard_id"`
	ServerIndex      int             `json:"server_index"`
	CompletedAt      time.Time       `json:"completed_at"`
	TableHeight      int             `json:"table_height"`
	TableWidth       int             `json:"table_width"`
	SlotLength       int             `json:"slot_length"`
	NonZeroSlotCount int             `json:"non_zero_slot_count"`
	Slots            []PublishedSlot `json:"slots"`
}

type PublishedResultManifest struct {
	EpochID     int64     `json:"epoch_id"`
	ShardID     int       `json:"shard_id"`
	ResultKey   string    `json:"result_key"`
	CompletedAt time.Time `json:"completed_at"`
}

type objectStore interface {
	PutObject(bucket, key string, body []byte, contentType string) error
}

type leaderControlRuntime struct {
	epoch          EpochMeta
	epochTimer     *time.Timer
	accepted       map[int64](*AcceptQueryTuple)
	lastResultPath string
	peerState      PeerConnectionState
	peerError      string
	mergeWaiters   []chan beginEpochMergeResult
}

type leaderControlCommand interface{}

type startEpochCommand struct {
	durationSeconds int64
	epochID         int64
	startUnix       int64
	reply           chan startEpochResult
}

type startEpochResult struct {
	reply StartEpochReply
	err   error
}

type epochStatusCommand struct {
	reply chan EpochStatusReply
}

type controlSnapshot struct {
	epoch      EpochMeta
	accepting  bool
	lastResult string
	peerState  PeerConnectionState
	peerError  string
}

type controlSnapshotCommand struct {
	reply chan controlSnapshot
}

type updatePeerConnectionStateCommand struct {
	state PeerConnectionState
	err   string
	reply chan struct{}
}

type upload1Command struct {
	args  *UploadArgs1
	reply chan upload1Result
}

type upload1Result struct {
	reply UploadReply1
	err   error
}

type upload2Command struct {
	args  *UploadArgs2
	reply chan upload2Result
}

type upload2Result struct {
	reply UploadReply2
	err   error
}

type upload3Command struct {
	args  *UploadArgs3
	reply chan upload3Result
}

type upload3Result struct {
	reply UploadReply3
	err   error
}

type beginEpochMergeCommand struct {
	reply chan beginEpochMergeResult
}

type beginEpochMergeResult struct {
	meta      EpochMeta
	shouldRun bool
}

type completeEpochMergeCommand struct {
	epochID    int64
	resultPath string
	err        error
	reply      chan struct{}
}

type takeAcceptedSessionCommand struct {
	uuid  int64
	reply chan takeAcceptedSessionResult
}

type takeAcceptedSessionResult struct {
	session *AcceptQueryTuple
	ok      bool
}

type stopEpochTimerCommand struct {
	reply chan struct{}
}

type abortEpochCommand struct {
	epochID int64
	reply   chan error
}

type Server struct {
	ServerIdx   int
	ShardID     int
	ServerAddrs []string

	clientsTotal       int
	clientsServed      int
	rateHistory        []float64
	clientsServedMutex sync.Mutex

	incoming1 chan bool
	incoming2 chan bool
	incoming3 chan bool
	ready     chan int64

	pending      map[int64](*InsertQueryTuple)
	pendingMutex sync.RWMutex

	entries *SlotTable

	plain      *BitMatrix
	plainMutex sync.Mutex

	// Hold this in write mode while aggregating
	amPublishingMutex sync.RWMutex

	rpcClients [NUM_SERVERS + 1]*rpc.Client

	resultsDir   string
	resultBucket string
	resultPrefix string
	resultStore  objectStore
	controlCh    chan leaderControlCommand
	mergeFn      func() (string, error)
}

func init() {
	// This is a 109-bit modulus
	IntModulus = fromString("80000000000000000000080001")
}

func fromString(s string) *big.Int {
	out := new(big.Int)
	out.SetString(s, 16)
	return out
}

func (s DbState) String() string {
	switch s {
	case EpochStateNoActive:
		return "no_active_epoch"
	case EpochStateActive:
		return "active"
	case EpochStateClosing:
		return "closing"
	case EpochStateMerging:
		return "merging"
	case EpochStateCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

func (s PeerConnectionState) String() string {
	switch s {
	case PeerConnectionsConnecting:
		return "connecting"
	case PeerConnectionsReady:
		return "ready"
	case PeerConnectionsFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (t *Server) SetResultsDir(resultsDir string) {
	t.resultsDir = resultsDir
}

func (t *Server) setResultObjectStore(bucket, prefix string, store objectStore) {
	t.resultBucket = bucket
	t.resultPrefix = strings.Trim(prefix, "/")
	t.resultStore = store
}

func (t *Server) SetShardID(shardID int) {
	t.ShardID = shardID
}

func (t *Server) currentEpochMeta() EpochMeta {
	reply := make(chan controlSnapshot, 1)
	t.controlCh <- controlSnapshotCommand{reply: reply}
	return (<-reply).epoch
}

func (t *Server) currentControlSnapshot() controlSnapshot {
	reply := make(chan controlSnapshot, 1)
	t.controlCh <- controlSnapshotCommand{reply: reply}
	return <-reply
}

func (t *Server) currentPeerState() (PeerConnectionState, string) {
	snapshot := t.currentControlSnapshot()
	return snapshot.peerState, snapshot.peerError
}

func (t *Server) acceptingWrites() bool {
	return t.currentControlSnapshot().accepting
}

func (t *Server) getLastResultPath() string {
	return t.currentControlSnapshot().lastResult
}

func (t *Server) stopEpochTimer() {
	reply := make(chan struct{}, 1)
	t.controlCh <- stopEpochTimerCommand{reply: reply}
	<-reply
}

func extractPublishedSlots(matrix *BitMatrix) []PublishedSlot {
	var zeros SlotContents
	out := make([]PublishedSlot, 0)
	for row := 0; row < TABLE_HEIGHT; row++ {
		for col := 0; col < TABLE_WIDTH; col++ {
			start := col * SLOT_LENGTH
			end := start + SLOT_LENGTH
			var slot SlotContents
			copy(slot[:], matrix[row][start:end])
			if slot != zeros {
				out = append(out, PublishedSlot{
					Row:        row,
					Column:     col,
					MessageHex: fmt.Sprintf("%x", slot[:]),
				})
			}
		}
	}
	return out
}

func (t *Server) buildPublishedResult(plaintext *BitMatrix, epoch EpochMeta, completedAt time.Time) PublishedResult {
	result := PublishedResult{
		EpochID:          epoch.ID,
		StartTime:        epoch.StartTime.UTC(),
		EndTime:          epoch.EndTime.UTC(),
		DurationSeconds:  epoch.DurationSeconds,
		ShardID:          t.ShardID,
		ServerIndex:      t.ServerIdx,
		CompletedAt:      completedAt.UTC(),
		TableHeight:      TABLE_HEIGHT,
		TableWidth:       TABLE_WIDTH,
		SlotLength:       SLOT_LENGTH,
		Slots:            extractPublishedSlots(plaintext),
		NonZeroSlotCount: 0,
	}
	result.NonZeroSlotCount = len(result.Slots)
	return result
}

func marshalPublishedResult(result PublishedResult) ([]byte, error) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func publishedResultFilename(epoch EpochMeta, shardID, serverIdx int) string {
	return fmt.Sprintf("epoch-%06d-shard-%d-server-%d.json", epoch.ID, shardID, serverIdx)
}

func publishedResultObjectKey(prefix string, shardID int, epochID int64) string {
	key := fmt.Sprintf("shards/%d/epochs/%06d/result.json", shardID, epochID)
	if prefix == "" {
		return key
	}
	return path.Join(strings.Trim(prefix, "/"), key)
}

func publishedResultManifestKey(prefix string, shardID int) string {
	key := fmt.Sprintf("shards/%d/latest.json", shardID)
	if prefix == "" {
		return key
	}
	return path.Join(strings.Trim(prefix, "/"), key)
}

func marshalPublishedResultManifest(manifest PublishedResultManifest) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (t *Server) writePublishedResult(plaintext *BitMatrix, epoch EpochMeta, completedAt time.Time) (string, error) {
	if t.resultsDir == "" && (t.resultStore == nil || t.resultBucket == "") {
		return "", nil
	}

	data, err := marshalPublishedResult(t.buildPublishedResult(plaintext, epoch, completedAt))
	if err != nil {
		return "", err
	}

	locations := make([]string, 0, 2)
	if t.resultsDir != "" {
		if err := os.MkdirAll(t.resultsDir, 0o755); err != nil {
			return "", err
		}

		filePath := filepath.Join(t.resultsDir, publishedResultFilename(epoch, t.ShardID, t.ServerIdx))
		if err := os.WriteFile(filePath, data, 0o644); err != nil {
			return "", err
		}
		locations = append(locations, filePath)
	}

	if t.resultStore != nil && t.resultBucket != "" {
		key := publishedResultObjectKey(t.resultPrefix, t.ShardID, epoch.ID)
		if err := t.resultStore.PutObject(t.resultBucket, key, data, "application/json"); err != nil {
			return "", err
		}
		manifest := PublishedResultManifest{
			EpochID:     epoch.ID,
			ShardID:     t.ShardID,
			ResultKey:   key,
			CompletedAt: completedAt.UTC(),
		}
		manifestData, err := marshalPublishedResultManifest(manifest)
		if err != nil {
			return "", err
		}
		manifestKey := publishedResultManifestKey(t.resultPrefix, t.ShardID)
		if err := t.resultStore.PutObject(t.resultBucket, manifestKey, manifestData, "application/json"); err != nil {
			return "", err
		}
		locations = append(locations, fmt.Sprintf("s3://%s/%s", t.resultBucket, key))
	}

	return strings.Join(locations, ","), nil
}
