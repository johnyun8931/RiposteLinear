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
	EpochStateMerging
	EpochStateCompleted
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
	Query [NUM_SERVERS]EncryptedInsertQuery
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
	ServerIndex      int             `json:"server_index"`
	CompletedAt      time.Time       `json:"completed_at"`
	TableHeight      int             `json:"table_height"`
	TableWidth       int             `json:"table_width"`
	SlotLength       int             `json:"slot_length"`
	NonZeroSlotCount int             `json:"non_zero_slot_count"`
	Slots            []PublishedSlot `json:"slots"`
}

type Server struct {
	ServerIdx   int
	State       DbState
	ServerAddrs []string

	clientsTotal       int
	clientsServed      int
	rateHistory        []float64
	clientsServedMutex sync.Mutex

	accepted      map[int64](*AcceptQueryTuple)
	acceptedMutex sync.RWMutex

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

	epochMutex      sync.RWMutex
	epoch           EpochMeta
	epochTimer      *time.Timer
	resultsDir      string
	lastResultPath  string
	lastResultMutex sync.RWMutex

	mergeFn func() error
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
	case EpochStateMerging:
		return "merging"
	case EpochStateCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

func (t *Server) SetResultsDir(resultsDir string) {
	t.resultsDir = resultsDir
}

func (t *Server) currentEpochMeta() EpochMeta {
	t.epochMutex.RLock()
	defer t.epochMutex.RUnlock()
	return t.epoch
}

func (t *Server) acceptingWrites() bool {
	t.epochMutex.RLock()
	defer t.epochMutex.RUnlock()
	return t.State == EpochStateActive
}

func (t *Server) setLastResultPath(path string) {
	t.lastResultMutex.Lock()
	defer t.lastResultMutex.Unlock()
	t.lastResultPath = path
}

func (t *Server) getLastResultPath() string {
	t.lastResultMutex.RLock()
	defer t.lastResultMutex.RUnlock()
	return t.lastResultPath
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

func (t *Server) writePublishedResult(plaintext *BitMatrix, epoch EpochMeta, completedAt time.Time) (string, error) {
	if t.resultsDir == "" {
		return "", nil
	}

	if err := os.MkdirAll(t.resultsDir, 0o755); err != nil {
		return "", err
	}

	result := PublishedResult{
		EpochID:          epoch.ID,
		ServerIndex:      t.ServerIdx,
		CompletedAt:      completedAt.UTC(),
		TableHeight:      TABLE_HEIGHT,
		TableWidth:       TABLE_WIDTH,
		SlotLength:       SLOT_LENGTH,
		Slots:            extractPublishedSlots(plaintext),
		NonZeroSlotCount: 0,
	}
	result.NonZeroSlotCount = len(result.Slots)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}

	path := filepath.Join(t.resultsDir, fmt.Sprintf("epoch-%06d-server-%d.json", epoch.ID, t.ServerIdx))
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", err
	}
	t.setLastResultPath(path)
	return path, nil
}
