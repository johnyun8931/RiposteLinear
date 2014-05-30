package db

import (
  "net/rpc"
  "sync"

  "henrycg/zkp/group"
  "henrycg/zkp/schnorr"
)

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 2
const NUM_SERVERS = 1 << NUM_DIMENSIONS

// Size of a side of the data array
const TABLE_WIDTH int = 1 << 3
const TABLE_HEIGHT int = 1 << 2

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 48

// Length of plaintext messages (in bytes)
const SLOT_LENGTH int = 2

type BitMatrix [TABLE_WIDTH][TABLE_HEIGHT]SlotContents

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

type CommitRow [TABLE_WIDTH]group.Element
type CommitCol [TABLE_WIDTH]group.Element

type EncryptedInsertQuery struct {
  SenderPublicKey [32]byte
  Nonce [24]byte
  Ciphertext []byte
}

type UploadArgs struct {
  Query [NUM_SERVERS]EncryptedInsertQuery
}

type InsertQuery struct {
  XCoords [TABLE_WIDTH]bool
  YCoords [TABLE_HEIGHT]SlotContents

  XCommits CommitRow
  YCommits CommitCol

  XProof schnorr.ManyEvidence
  YProof schnorr.ManyEvidence
}

type UploadReply struct {
  Magic int
}

type DumpReply struct {
  Entries BitMatrix
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
  // TODO Dont need to send all stuff
  Uuid int64
  Query EncryptedInsertQuery
}

type PrepareReply struct {
  // VOTE: YES/NO
  Okay bool
}

type CommitArgs struct {
  // COMMIT
  // uuid
  Uuid int64
  Commit bool
}

type CommitReply struct {
  // Ack
  // uuid
}

type PlaintextArgs struct {
  Plaintext BitMatrix
}

type PlaintextReply struct {
  // Nothing
}

type SlotTable struct {
  ServerIdx int
  State DbState

  pending map[int64]InsertQuery
  pendingMutex sync.Mutex

  entries BitMatrix
  entriesMutex sync.Mutex

  plain BitMatrix
  plainMutex sync.Mutex

  rpcClients [NUM_SERVERS]*rpc.Client
}


