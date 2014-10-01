package db

import (
  "net/rpc"
  "sync"

  "henrycg/email/prf"
)

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 2
const NUM_SERVERS = 2//1 << NUM_DIMENSIONS

// Size of a side of the data array
const TABLE_WIDTH int = 1 << 8
const TABLE_HEIGHT int = 1 << 9

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 48

// Length of plaintext messages (in bytes)
const SLOT_LENGTH int = 256// 64 KB

type BitMatrix [TABLE_HEIGHT][TABLE_WIDTH]SlotContents

type DbState int
const (
  State_AcceptUpload = iota
  State_PrepareForMerge = iota
  State_Merge = iota
  State_AcceptPlaintext = iota
)

type SlotContents struct {
  Message [SLOT_LENGTH]byte
}

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
  MessageMask [TABLE_WIDTH]SlotContents
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

type PlaintextArgs struct {
  Plaintext *BitMatrix
}

type PlaintextReply struct {
  // Nothing
}

type SlotTable struct {
  ServerIdx int
  State DbState

  ClientsServed int

  pending map[int64]([]*InsertQuery)
  pendingMutex sync.Mutex

  entries *BitMatrix
  entriesMutex sync.Mutex

  plain *BitMatrix
  plainMutex sync.Mutex

  rpcClients [NUM_SERVERS]*rpc.Client
}


