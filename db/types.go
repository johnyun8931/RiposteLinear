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

type DbState int
const (
  State_AcceptUpload = iota
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
  uuid int64
  queries [NUM_SERVERS]InsertQuery
}

type PrepareReply struct {
  // VOTE: YES/NO
  okay bool
}

type CommitArgs struct {
  // COMMIT
  // uuid
  uuid int64
  commit bool
}

type CommitReply struct {
  // Ack
  // uuid
}

type SlotTable struct {
  ServerIdx int
  State DbState

  entries [NUM_SLOTS][NUM_SLOTS][NUM_SLOTS]SlotContents
  entriesMutex sync.Mutex

  pending map[int64]PrepareArgs
  pendingMutex sync.Mutex

  rpcClients [NUM_SERVERS]*rpc.Client
}

