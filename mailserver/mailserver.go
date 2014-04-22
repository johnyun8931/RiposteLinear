package mailserver

import "log"
import "sync"

// Number of data slots held at this server.
const NUM_SLOTS = 1<<16
const SLOT_LENGTH = 1<<12

type SlotContents struct {
  buffer [SLOT_LENGTH]byte
}

type UploadArgs struct {
  // Arbitrary constant
  destination_slots [128]int
  message SlotContents
}

type UploadReply struct {
  success bool
}

type DownloadArgs struct {
  // Arbitrary constant for now
  requested_slots [128]int
}

type DownloadReply struct {
  success bool
  // Arbitrary constant for now...
  data [4]SlotContents
}

type SlotData struct {
  Mutex sync.Mutex
  Is_filled bool
  Buffer SlotContents
}

type SlotTable struct {
  Entries [NUM_SLOTS]SlotData
}


func (t *SlotTable) Upload(args *UploadArgs, reply *UploadReply) error {
  log.Printf("Got upload request")
  return nil
}

func (t *SlotTable) Download(args *DownloadArgs, reply *DownloadReply) error {
  log.Printf("Got download request")
  return nil
}


