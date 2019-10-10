package db

import (
	"math/big"
	"net/rpc"
	"sync"

	"bitbucket.org/henrycg/riposte/prf"
)

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

type SlotContents [SLOT_LENGTH]byte

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

type DPFKey struct {
	KeyIndex    int
	Keys        [TABLE_HEIGHT]prf.Key
	KeyMask     [TABLE_HEIGHT]bool
	MessageMask BitMatrixRow

	// Share of client's message
	//MessageShare *big.Int
}

type CorProof struct {
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

	q1 *InsertQuery1
	q2 *InsertQuery2
	q3 *InsertQuery3
}

type InsertQuery1 struct {
	Key DPFKey

	// TODO: Add real proof
	Proof CorProof
}

type InsertQuery2 struct {
	Uuid int64
	Tag  int64
}

type InsertQuery3 struct {
	Uuid int64
	Tag  int64
}

type UploadReply1 struct {
	Uuid    int64
	HashKey [32]byte
}

type UploadReply2 struct {
	Magic int
}

type UploadReply3 struct {
	Magic int
}

type DumpReply struct {
	Entries *BitMatrix
}

type PrepareArgs struct {
	Uuid   int64
	Query1 EncryptedInsertQuery
	Query2 EncryptedInsertQuery
	Query3 EncryptedInsertQuery
}

type PrepareReply struct {
	QueryAnswers *big.Int
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

type Server struct {
	ServerIdx   int
	State       DbState
	ServerAddrs []string

	clientsTotal       int
	clientsServed      int
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
}

func init() {
	IntModulus = fromString("2000000000000000000000000000000000000000000000000000000000000040001")
}

func fromString(s string) *big.Int {
	out := new(big.Int)
	out.SetString(s, 16)
	return out
}
