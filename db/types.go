package db

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/rpc"
	"os"
	"path/filepath"
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

type StatusArgs struct{}

type StatusReply struct {
	Healthy      bool   `json:"healthy"`
	IsLeader     bool   `json:"is_leader"`
	ServerIndex  int    `json:"server_index"`
	ShardID      int    `json:"shard_id"`
	EpochID      int64  `json:"epoch_id"`
	State        string `json:"state"`
	StartUnix    int64  `json:"start_unix"`
	EndUnix      int64  `json:"end_unix"`
	DurationSecs int64  `json:"duration_secs"`
	Accepting    bool   `json:"accepting"`
	LastResult   string `json:"last_result"`
	PeerState    string `json:"peer_state"`
	PeerError    string `json:"peer_error"`
}

type CoordinatorStatusArgs struct {
	ShardTimeoutMs int64 `json:"shard_timeout_ms"`
}

type CoordinatorShardStatus struct {
	ID                  int         `json:"id"`
	StartRow            int         `json:"start_row"`
	EndRow              int         `json:"end_row"`
	ActiveLeaderAddr    string      `json:"active_leader_addr"`
	ActiveFollowerAddr  string      `json:"active_follower_addr"`
	HasStandby          bool        `json:"has_standby"`
	StandbyLeaderAddr   string      `json:"standby_leader_addr"`
	StandbyFollowerAddr string      `json:"standby_follower_addr"`
	Reachable           bool        `json:"reachable"`
	Status              StatusReply `json:"status"`
	StatusError         string      `json:"status_error"`
	ActiveReachable     bool        `json:"active_reachable"`
	ActiveStatus        StatusReply `json:"active_status"`
	ActiveStatusError   string      `json:"active_status_error"`
	ActiveLastChecked   int64       `json:"active_last_checked_unix"`
	StandbyReachable    bool        `json:"standby_reachable"`
	StandbyStatus       StatusReply `json:"standby_status"`
	StandbyStatusError  string      `json:"standby_status_error"`
	StandbyLastChecked  int64       `json:"standby_last_checked_unix"`
}

type CoordinatorStatusReply struct {
	Healthy                   bool                     `json:"healthy"`
	Role                      string                   `json:"role"`
	LeaderAddr                string                   `json:"leader_addr"`
	GlobalTableHeight         int                      `json:"global_table_height"`
	EpochID                   int64                    `json:"epoch_id"`
	State                     string                   `json:"state"`
	StartUnix                 int64                    `json:"start_unix"`
	EndUnix                   int64                    `json:"end_unix"`
	DurationSecs              int64                    `json:"duration_secs"`
	Accepting                 bool                     `json:"accepting"`
	LeaseHolder               string                   `json:"lease_holder"`
	LeaseFencingToken         int64                    `json:"lease_fencing_token"`
	LeaseExpiresUnixMs        int64                    `json:"lease_expires_unix_ms"`
	LeaseActive               bool                     `json:"lease_active"`
	ActiveHolder              string                   `json:"active_holder"`
	CurrentShardCount         int                      `json:"current_shard_count"`
	RecommendedNextShardCount int                      `json:"recommended_next_shard_count"`
	TargetRowsPerShard        int                      `json:"target_rows_per_shard"`
	ScalingAction             string                   `json:"scaling_action"`
	ScalingReason             string                   `json:"scaling_reason"`
	RequestDensity            float64                  `json:"request_density"`
	ScalingEpochID            int64                    `json:"scaling_epoch_id"`
	ScalingAcceptedRequests   int64                    `json:"scaling_accepted_requests"`
	ScalingDurationSecs       int64                    `json:"scaling_duration_secs"`
	Shards                    []CoordinatorShardStatus `json:"shards"`
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
	GlobalStartRow   int             `json:"global_start_row"`
	GlobalEndRow     int             `json:"global_end_row"`
	TableHeight      int             `json:"table_height"`
	TableWidth       int             `json:"table_width"`
	SlotLength       int             `json:"slot_length"`
	NonZeroSlotCount int             `json:"non_zero_slot_count"`
	Slots            []PublishedSlot `json:"slots"`
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

	resultsDir     string
	globalRowStart int
	controlCh      chan leaderControlCommand
	mergeFn        func() (string, error)
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

func (t *Server) SetShardID(shardID int) {
	t.ShardID = shardID
}

func (t *Server) SetGlobalRowStart(globalRowStart int) error {
	if globalRowStart < 0 {
		return fmt.Errorf("global row start must be non-negative")
	}
	t.globalRowStart = globalRowStart
	return nil
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

func extractPublishedSlots(matrix *BitMatrix, globalRowStart int) []PublishedSlot {
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
					Row:        globalRowStart + row,
					Column:     col,
					MessageHex: fmt.Sprintf("%x", slot[:]),
				})
			}
		}
	}
	return out
}

func (t *Server) writePublishedResult(plaintext *BitMatrix, epoch EpochMeta, completedAt time.Time) (string, error) {
	if t.resultsDir == "" {
		return "", nil
	}

	if err := os.MkdirAll(t.resultsDir, 0o755); err != nil {
		return "", err
	}

	result := PublishedResult{
		EpochID:          epoch.ID,
		StartTime:        epoch.StartTime.UTC(),
		EndTime:          epoch.EndTime.UTC(),
		DurationSeconds:  epoch.DurationSeconds,
		ShardID:          t.ShardID,
		ServerIndex:      t.ServerIdx,
		CompletedAt:      completedAt.UTC(),
		GlobalStartRow:   t.globalRowStart,
		GlobalEndRow:     t.globalRowStart + TABLE_HEIGHT,
		TableHeight:      TABLE_HEIGHT,
		TableWidth:       TABLE_WIDTH,
		SlotLength:       SLOT_LENGTH,
		Slots:            extractPublishedSlots(plaintext, t.globalRowStart),
		NonZeroSlotCount: 0,
	}
	result.NonZeroSlotCount = len(result.Slots)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}

	path := filepath.Join(t.resultsDir, fmt.Sprintf("epoch-%06d-shard-%d-server-%d.json", epoch.ID, t.ShardID, t.ServerIdx))
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
