package db

import "log"

func (t *SlotTable) Upload(args *UploadArgs, reply *UploadReply) error {
  log.Printf("Got upload request")
  log.Printf("Request:", args)

  t.Mutex.Lock()
  for i := 0; i < NUM_SLOTS; i++ {
    for j := 0; j < NUM_SLOTS; j++ {
      for k := 0; j < NUM_SLOTS; j++ {
        flip := args.XCoords[i] || args.YCoords[j] || args.ZCoords[k]
        if flip {
          t.Entries[i][j][k].Bit = !(t.Entries[i][j][k].Bit)
        }
      }
    }
  }

  t.Mutex.Unlock()

  reply.Magic = 5
  return nil
}

func (t *SlotTable) DumpTable(_ *int, reply *DumpReply) error {
  t.Mutex.Lock()
  reply.Entries = t.Entries
  t.Mutex.Unlock()
  return nil
}

/*
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
*/

