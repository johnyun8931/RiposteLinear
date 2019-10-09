package db

import (
	"math/big"
	"net/rpc"
	"sync"
	"time"

	"bitbucket.org/henrycg/riposte/prf"
)

const AUDIT_SERVER int = 2

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 2
const NUM_SERVERS = 2 //1 << NUM_DIMENSIONS

// Size of a side of the data array
const TABLE_WIDTH int = 10
const TABLE_HEIGHT int = 10 //100 / TABLE_WIDTH

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 128

// Length of plaintext messages (in bytes)
const SLOT_LENGTH int = 8 // 64 KB

type BitMatrix [TABLE_HEIGHT]BitMatrixRow
type BitMatrixRow [TABLE_WIDTH * SLOT_LENGTH]byte

type SlotTable struct {
	table      BitMatrix
	tableMutex sync.Mutex
}

var IntModulus *big.Int

type DbState int

const (
	State_Booting         = iota
	State_AcceptUpload    = iota
	State_PrepareForMerge = iota
	State_Merge           = iota
	State_AcceptPlaintext = iota
)

type SlotContents [SLOT_LENGTH]byte

type EncryptedInsertQuery struct {
	SenderPublicKey [32]byte
	Nonce           [24]byte
	Ciphertext      []byte
}

type EncryptedAuditQuery EncryptedInsertQuery

type UploadArgs struct {
	Query [NUM_SERVERS]EncryptedInsertQuery
}

type DPFKey struct {
	KeyIndex    int
	Keys        [TABLE_HEIGHT]prf.Key
	KeyMask     [TABLE_HEIGHT]bool
	MessageMask BitMatrixRow

	// Share of client's message
	MessageShare *big.Int

	// Random blinding value
	Nonce [16]byte
}

type CorProof struct {
}

type InsertQuery struct {
	Key DPFKey

	// TODO: Add real proof
	Proof CorProof
}

type UploadReply struct {
	Magic int
}

type DumpReply struct {
	Entries *BitMatrix
}

type PrepareArgs struct {
	Uuid  int64
	Query EncryptedInsertQuery
}

type PrepareReply struct {
	// VOTE: YES/NO
	QueryToAudit EncryptedAuditQuery
	Okay         bool
}

/*
type AuditArgs struct {
	Uuid      int64
	Challenge *big.Int
}

type AuditReply struct {
	Okay bool
}*/

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

type Server struct {
	ServerIdx   int
	State       DbState
	ServerAddrs []string

	clientsServed      int
	clientsServedStart time.Time
	clientsServedMutex sync.Mutex

	pending      map[int64](*InsertQuery)
	pendingMutex sync.Mutex

	entries *SlotTable

	plain      *BitMatrix
	plainMutex sync.Mutex

	rpcClients [NUM_SERVERS + 1]*rpc.Client
}

func init() {
	IntModulus = fromString("2000000000000000000000000000000000000000000000000000000000000040001")
}

func fromString(s string) *big.Int {
	out := new(big.Int)
	out.SetString(s, 16)
	return out
}
