package db

import (
  "errors"
  "fmt"
  "log"
  "math"
  "net/rpc"
  "time"

  "henrycg/email/utils"
)

var (
  incomingReqs = make(chan [NUM_SERVERS]InsertQuery, REQ_BUFFER_SIZE)
  commitReqs = make(chan CommitArgs, REQ_BUFFER_SIZE)

  beginMergeMarker [NUM_SERVERS]InsertQuery
  beginMergeMarkerCommit CommitArgs
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

/*******************
 * Leader code
 */

func (t *SlotTable) Upload(args *UploadArgs, reply *UploadReply) error {
  if !t.isLeader() {
    return errors.New("Only leader can accept uploads")
  }

  log.Printf("Got upload request")
  log.Printf("Request:", args)

  if t.State != State_AcceptUpload {
    return errors.New("Not accepting uploads")
  }

  incomingReqs<- args.Query
  reply.Magic = 5
  return nil
}

func (t *SlotTable) submitPrepares() {
  for {
    queries := <-incomingReqs

    // If we're starting to merge, then the marker down
    // the pipeline
    if queries == beginMergeMarker {
      commitReqs <-CommitArgs
      continue
    }

    var err error
    var prep PrepareArgs
    prep.Queries = queries
    prep.Uuid, err = utils.RandomInt64(math.MaxInt64)
    if err != nil {
      log.Printf("error in random")
      continue
    }

    log.Printf("Send PREPARE %d", prep.Uuid)

    // Send out PREPARE request
    c := make(chan error, NUM_SERVERS)
    var replies [NUM_SERVERS]PrepareReply
    for i:=0; i<NUM_SERVERS; i++ {
      go func(prep *PrepareArgs, reply *PrepareReply, i int, c chan error) {
        err := t.rpcClients[i].Call("SlotTable.Prepare", prep, reply)
        if err != nil {
          c <- err
        } else {
          c <- nil
        }
      }(&prep, &replies[i], i, c)
    }

    // Wait for responses
    var r error
    for i:=0; i<NUM_SERVERS; i++ {
      r = <-c
      if r != nil {
        log.Fatal("Error in prepare: ", r)
      }
    }

    var okay bool
    for i:=0; i<NUM_SERVERS; i++ {
      log.Printf("Got reply ", i, replies[i].Okay)
      okay = (okay && replies[i].Okay)
    }

    log.Printf("Done PREPARE %d", prep.Uuid)

    commitArgs := CommitArgs{prep.Uuid, okay}
    commitReqs <- commitArgs
  }
}

func (t *SlotTable) submitCommits() {
  for {
    com := <-commitReqs
    if com == beginMergeMarkerCommit {
      sendMergeRequest()
    }
    log.Printf("Send COMMIT %d", com.Uuid)

    // Send out COMMIT request
    c := make(chan error, NUM_SERVERS)
    var replies [NUM_SERVERS]CommitReply
    for i:=0; i<NUM_SERVERS; i++ {
      go func(com *CommitArgs, reply *CommitReply, i int, c chan error) {
        err := t.rpcClients[i].Call("SlotTable.Commit", com, reply)
        if err != nil {
          c <- err
        } else {
          c <- nil
        }
      }(&com, &replies[i], i, c)
    }

    // Wait for responses
    var r error
    for i:=0; i<NUM_SERVERS; i++ {
      r = <-c
      if r != nil {
        log.Fatal("Error in commit: ", r)
      }
    }

    log.Printf("Done COMMIT %d", com.Uuid)
  }
}

func (t *SlotTable) sendMergeRequest() {
  // Call each server and ask for their data
  // Send out COMMIT request
  c := make(chan error, NUM_SERVERS)
  var replies [NUM_SERVERS]DumpReply
  for i:=0; i<NUM_SERVERS; i++ {
    go func(, reply *DumpReply, i int, c chan error) {
      err := t.rpcClients[i].Call("SlotTable.DumpTable", 0, reply)
      if err != nil {
        c <- err
      } else {
        c <- nil
      }
    }(&replies[i], i, c)
  }

  // Wait for responses
  var r error
  for i:=0; i<NUM_SERVERS; i++ {
    r = <-c
    if r != nil {
      log.Fatal("Error in commit: ", r)
    }
  }

  log.Printf("Done MERGE")
}

func (t *SlotTable) revealCleartext(tables [NUM_SERVERS]DumpReply) {
  // XOR all of the tables together and save 
  // it in the plaintext table
  blah
}

func (t *SlotTable) beginMerge() {
  // Stop accepting uploads
  t.State = State_PrepareForMerge

  // Insert a nil request into the pipeline so that we can figure
  // out when all pending requests have been processed.
  incomingReqs <- beginMergeMarker
}

/**************
 * Handle Updates
 */

func (t *SlotTable) Prepare(prep *PrepareArgs, reply *PrepareReply) error {
  t.pendingMutex.Lock()
  if t.pending == nil {
    t.pending = map[int64]PrepareArgs{}
  }
  t.pending[prep.Uuid] = *prep
  t.pendingMutex.Unlock()

  // XXX check if good
  isGood := t.validateUpload(prep.Queries[t.ServerIdx])

  reply.Okay = isGood
  return nil
}

func (t *SlotTable) Commit(com *CommitArgs, reply *CommitReply) error {
  t.pendingMutex.Lock()
  val, ok := t.pending[com.Uuid]

  if !ok {
    err := errors.New(fmt.Sprintf("Got commit msg for unknown UUID: %d",  com.Uuid))
    t.pendingMutex.Unlock()
    return err
  }

  delete(t.pending, com.Uuid)
  t.pendingMutex.Unlock()

  // If we don't need to commit the entry
  if !com.Commit {
    return nil
  }

  // Update the database with the query
  t.processQuery(val.Queries[t.ServerIdx])

  return nil
}

func (t *SlotTable) DumpTable(_ *int, reply *DumpReply) error {
  t.entriesMutex.Lock()
  reply.Entries = t.entries
  t.entriesMutex.Unlock()
  return nil
}

/************
 * Actual DB manipulation
 */

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

/***********
 * Initialization
 */

func (t *SlotTable) connectToServer(client **rpc.Client, serverAddr string, c chan int) {
  var err error
  *client, err = rpc.DialHTTP("tcp", serverAddr)

  if err == nil {
    c <- 1
  } else {
    c <- -1
  }
}

func (t *SlotTable) openConnections() error {
  if !t.isLeader() {
    return errors.New("Only leader should open connections")
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

func (t *SlotTable) Initialize(*int, *int) error {
  if (t.isLeader()) {
    go t.submitPrepares()
    go t.submitCommits()
    go func(t *SlotTable) {
      // HACK wait until other servers have started
      time.Sleep(500*time.Millisecond)
      err := t.openConnections()
      if err != nil {
        log.Fatal("Could not initialize table", err)
      }
    }(t)
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

