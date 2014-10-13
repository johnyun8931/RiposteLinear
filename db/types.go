package db

import (
  "net/rpc"
  "sync"

//  "code.google.com/p/go.crypto/nacl/box"
  "henrycg/email/prf"
)

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 2
const NUM_SERVERS = 2

// Size of a side of the data array
const TABLE_WIDTH int = 1 << 9
const TABLE_HEIGHT int = 1 << 8

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 48

// Length of NaCl "box" public key
const BOX_PUBLIC_KEY_LEN int = 32
const BOX_OVERHEAD int = 16

// Length of plaintext messages (in bytes)
const PLAIN_LENGTH int = 140

// Length of plaintext plus two public keys and two
// message digests
const SLOT_LENGTH int = PLAIN_LENGTH + 2*(BOX_PUBLIC_KEY_LEN + BOX_OVERHEAD)

type BitMatrix [TABLE_HEIGHT]BitMatrixRow
type BitMatrixRow [TABLE_WIDTH*SLOT_LENGTH]byte

type SlotTable struct {
  table BitMatrix
  tableMutex sync.Mutex
}

type DbState int
const (
  State_AcceptUpload = iota
  State_PrepareForMerge = iota
  State_Merge = iota
  State_AcceptPlaintext = iota
)

type PlainContents [PLAIN_LENGTH]byte
type SlotContents [SLOT_LENGTH]byte

type EncryptedInsertQuery struct {
  SenderPublicKey [32]byte
  Nonce [24]byte
  Ciphertext []byte
}

type UploadArgs struct {
  Query [NUM_SERVERS]EncryptedInsertQuery
}

type InsertQuery struct {
  Keys [TABLE_HEIGHT]prf.Key
  KeyMask [TABLE_HEIGHT]bool
  MessageMask BitMatrixRow
}

type UploadReply struct {
  Magic int
}

type DumpReply struct {
  Entries *BitMatrix
}

/*
type DownloadArgs struct {
  XCoords [NUM_SLOTS]bool
  YCoords [NUM_SLOTS]bool
}

type DownloadReply struct {
  Data SlotContents
}
*/

type PrepareArgs struct {
  Uuid int64
  Queries []EncryptedInsertQuery
}

type PrepareReply struct {
  // VOTE: YES/NO
  Okay bool
}

type CommitArgs struct {
  // COMMIT
  // uuid
  Uuid int64
}

type CommitReply struct {
  // Ack
  // uuid
}

type DecryptArgs struct {
  ToDecrypt [][]byte
}

type DecryptReply struct {
  Cleartexts [][]byte
}

type BlameArgs struct {
  // Nothing
}

type BlameReply struct {
  Queries map[int64]([]*InsertQuery)
}

type Server struct {
  ServerIdx int
  State DbState

  clientsServedMutex sync.Mutex
  clientsServed int

  pending map[int64]([]*InsertQuery)
  pendingMutex sync.Mutex

  committed map[int64]([]*InsertQuery)
  committedMutex sync.Mutex

  entries *SlotTable

  plain *BitMatrix
  plainMutex sync.Mutex

  rpcClients [NUM_SERVERS]*rpc.Client
}

