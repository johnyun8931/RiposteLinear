package db

import (
  "net/rpc"
  "sync"
)

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 3
const NUM_SERVERS = 1 << NUM_DIMENSIONS

// Size of a side of the data array
const NUM_SLOTS int = 1 << 2

// Number of upload requests to buffer
const REQ_BUFFER_SIZE int = 48

type DbState int
const (
  State_AcceptUpload = iota
  State_PrepareForMerge = iota
  State_Merge = iota
)

type SlotContents struct {
  Bit bool
}

type UploadArgs struct {
  Query [NUM_SERVERS]InsertQuery
}

type InsertQuery struct {
  XCoords [NUM_SLOTS]bool
  YCoords [NUM_SLOTS]bool
  ZCoords [NUM_SLOTS]bool
}

type UploadReply struct {
  Magic int
}

type DumpReply struct {
  Entries [NUM_SLOTS][NUM_SLOTS][NUM_SLOTS]SlotContents
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
  Queries [NUM_SERVERS]InsertQuery
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

type SlotTable struct {
  ServerIdx int
  State DbState

  pending map[int64]PrepareArgs
  pendingMutex sync.Mutex

  entries [NUM_SLOTS][NUM_SLOTS][NUM_SLOTS]SlotContents
  entriesMutex sync.Mutex

  rpcClients [NUM_SERVERS]*rpc.Client
}

