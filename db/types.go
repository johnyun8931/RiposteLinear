package db

import "sync"

// Number of "dimensions" for PIR scheme
const NUM_DIMENSIONS = 3
const NUM_SERVERS = 1 << NUM_DIMENSIONS

// Size of a side of the data array
const NUM_SLOTS int = 1 << 4

type SlotContents struct {
  Bit bool
}

type UploadArgs struct {
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

type SlotTable struct {
  Mutex sync.Mutex
  Entries [NUM_SLOTS][NUM_SLOTS][NUM_SLOTS]SlotContents
}
