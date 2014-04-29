package db

import (
  "errors"
  "log"
  "math"
  "net/rpc"

  "henrycg/email/utils"
)

func (t *SlotTable) isLeader() bool {
  return (t.ServerIdx == 0)
}

// Upload from client to leader
  // Check validity of first msg
  // Send msg to other nodes

// First phase from leader to node PREPARE
  // Check validity of msg

// First phase from node to leader VOTE
  // Add to pending pool or return NO

// Second phase from leader to node COMMIT

// Second phase from node to leader ACK

func (t *SlotTable) processQuery(query InsertQuery) error {
  t.entriesMutex.Lock()
  for i := 0; i < NUM_SLOTS; i++ {
    for j := 0; j < NUM_SLOTS; j++ {
      for k := 0; k < NUM_SLOTS; k++ {
        flip := query.XCoords[i] || query.YCoords[j] || query.ZCoords[k]
        if flip {
          t.entries[i][j][k].Bit = !(t.entries[i][j][k].Bit)
        }
      }
    }
  }

  t.entriesMutex.Unlock()
  return nil
}

func (t *SlotTable) validateUpload(query InsertQuery) bool {
  // XXX bogus for now
  return true
}

func (t *SlotTable) Upload(args *UploadArgs, reply *UploadReply) error {
  if !t.isLeader() {
    return errors.New("Only leader can accept uploads")
  }

  log.Printf("Got upload request")
  log.Printf("Request:", args)

  if t.State != State_AcceptUpload {
    return errors.New("Not accepting uploads")
  }

  var prep PrepareArgs
  var err error
  prep.queries = args.Query

  prep.uuid, err = utils.RandomInt64(math.MaxInt64)
  if err != nil {
    return err
  }

  t.processQuery(args.Query[0])
  return nil
}

func (t *SlotTable) Prepare(prep *PrepareArgs, reply *PrepareReply) error {
  t.pendingMutex.Lock()
  t.pending[prep.uuid] = *prep
  t.pendingMutex.Unlock()

  // XXX check if good
  isGood := true

  reply.okay = isGood
  return nil
}

func (t *SlotTable) DumpTable(_ *int, reply *DumpReply) error {
  t.entriesMutex.Lock()
  reply.Entries = t.entries
  t.entriesMutex.Unlock()
  return nil
}

func (t *SlotTable) connectToServer(client **rpc.Client, serverAddr string, c chan int) {
  var err error
  *client, err = rpc.DialHTTP("tcp", serverAddr)

  if err == nil {
    c <- 1
  } else {
    c <- -1
  }
}

func (t *SlotTable) InitializeLeader() error {
  if !t.isLeader() {
    return errors.New("only valid for leader")
  }

  c := make(chan int, NUM_SERVERS)
  servers := utils.AllServers()
  for i := 0; i < NUM_SERVERS; i++ {
    go t.connectToServer(&t.rpcClients[i], servers[i], c)
  }

  // Wait for all connections
  for i := 0; i < NUM_SERVERS; i++ {

    if <-c != 1 {
      return errors.New("Connection failed")
    }
  }

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

