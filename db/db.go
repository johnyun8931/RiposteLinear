package db

import "errors"
import "log"
import "sync"

// Number of data slots held at this server.
const NUM_SLOTS = 1<<16
const SLOT_LENGTH = 1<<12

type SlotContents struct {
  Buffer [SLOT_LENGTH]byte
}

type UploadArgs struct {
  DestinationSlot int
  Message SlotContents
}

type UploadReply struct {
}

type DownloadArgs struct {
  RequestedSlot int
}

type DownloadReply struct {
  Data SlotContents
}

type SlotData struct {
  Mutex sync.Mutex
  IsFilled bool
  Data SlotContents
}

type SlotTable struct {
  Entries [NUM_SLOTS]SlotData
}

func RangeIsValid(t *SlotTable, slot int) bool {
  return !(slot < 0 || slot >= NUM_SLOTS)
}

func (t *SlotTable) Upload(args *UploadArgs, reply *UploadReply) error {
  log.Printf("Got upload request")
  log.Printf("Request:", args)

  if !RangeIsValid(t, args.DestinationSlot) {
    return errors.New("Out of range")
  }

  var slot = &t.Entries[args.DestinationSlot]
  log.Printf("idx: ", args.DestinationSlot)

  slot.Mutex.Lock()
  slot.Data.Buffer = args.Message.Buffer
  slot.IsFilled = true
  slot.Mutex.Unlock()

  return nil
}

func (t *SlotTable) Download(args *DownloadArgs, reply *DownloadReply) error {
  log.Printf("Got download request")
  log.Printf("Request:", args)

  if !RangeIsValid(t, args.RequestedSlot) {
    return errors.New("Out of range")
  }

  var slot = &t.Entries[args.RequestedSlot]
  log.Printf("idx: ", args.RequestedSlot)

  slot.Mutex.Lock()
  if slot.IsFilled {
    reply.Data.Buffer = slot.Data.Buffer
  }
  slot.Mutex.Unlock()

  return nil
}

