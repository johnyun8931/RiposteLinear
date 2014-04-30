package db

import (
  "errors"
  "log"
  "math"
  "net/rpc"

  "henrycg/email/utils"
)

var (
  incomingReqs = make(chan [NUM_SERVERS]InsertQuery, REQ_BUFFER_SIZE)
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

  log.Printf("Leng buffer: ", len(incomingReqs))
  incomingReqs<- args.Query
  reply.Magic = 5
  return nil
}

func (t *SlotTable) HandleIncomingUploads() {
  log.Printf("Handling start!")
  for {
    log.Printf("Handling!")
    var err error
    var prep PrepareArgs
    prep.queries = <-incomingReqs
    prep.uuid, err = utils.RandomInt64(math.MaxInt64)
    if err != nil {
      log.Printf("error in random")
      continue
    }

    c := make(chan int, NUM_SERVERS)
    var replies [NUM_SERVERS]PrepareReply
    for i:=0; i<NUM_SERVERS; i++ {
      go func(prep *PrepareArgs, reply *PrepareReply, i int, c chan int) {
        err := t.rpcClients[i].Call("SlotTable.Prepare", prep, reply)
        if err != nil {
          c <- -1
        } else {
          c <- 1
        }
      }(&prep, &replies[i], i, c)
    }

    var r int
    for i:=0; i<NUM_SERVERS; i++ {
      r = <-c
      if r != 1 {
        log.Fatal("Error in prepare!")
      }
    }

    for i:=0; i<NUM_SERVERS; i++ {
      log.Printf("Got reply ", i, replies[i].okay)
    }
  }
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

func (t *SlotTable) OpenConnections() error {
  if t.isLeader() {
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

