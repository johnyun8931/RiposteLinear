package db

import (
  "crypto/tls"
  "errors"
  "fmt"
  "log"
  "math"
  "net/rpc"
  "time"

  "henrycg/email/utils"
)

var (
  incomingReqs = make(chan [NUM_SERVERS]EncryptedInsertQuery, REQ_BUFFER_SIZE)
  commitReqs = make(chan CommitArgs, REQ_BUFFER_SIZE)

  beginMergeMarker [NUM_SERVERS]EncryptedInsertQuery
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
    if queries[0].Ciphertext == nil {
      commitReqs <-CommitArgs{}
      continue
    }

    uuid, err := utils.RandomInt64(math.MaxInt64)
    if err != nil {
      log.Printf("error in random")
      continue
    }

    log.Printf("Send PREPARE %d", uuid)

    // Send out PREPARE request
    c := make(chan error, NUM_SERVERS)
    var replies [NUM_SERVERS]PrepareReply
    for i:=0; i<NUM_SERVERS; i++ {
      var prep PrepareArgs
      prep.Uuid = uuid
      prep.Query = queries[i]
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

    okay := true
    for i:=0; i<NUM_SERVERS; i++ {
      log.Printf("Got reply %d %d", i, replies[i].Okay)
      okay = (okay && replies[i].Okay)
    }

    log.Printf("Done PREPARE %d", uuid)

    commitArgs := CommitArgs{uuid, okay}
    commitReqs <- commitArgs
  }
}

func (t *SlotTable) submitCommits() {
  for {
    com := <-commitReqs
    if com == beginMergeMarkerCommit {
      t.sendMergeRequest()
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

func (t *SlotTable) mergeWorker() {
  for {
    time.Sleep(5*time.Second)
    t.sendMergeRequest()
  }
}

func (t *SlotTable) sendMergeRequest() {
  // Call each server and ask for their data
  // Send out COMMIT request
  c := make(chan error, NUM_SERVERS)
  var replies [NUM_SERVERS]DumpReply
  for i:=0; i<NUM_SERVERS; i++ {
    go func(reply *DumpReply, i int, c chan error) {
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

  var parg PlaintextArgs
  parg.Plaintext = revealCleartext(replies)

  c = make(chan error, NUM_SERVERS)
  for i:=0; i<NUM_SERVERS; i++ {
    go func(i int, c chan error) {
      var p_reply PlaintextReply
      err := t.rpcClients[i].Call("SlotTable.StorePlaintext", &parg, &p_reply)
      if err != nil {
        c <- err
      } else {
        c <- nil
      }
    }(i, c)
  }

  // Wait for responses
  for i:=0; i<NUM_SERVERS; i++ {
    r = <-c
    if r != nil {
      log.Fatal("Error in plaintext: ", r)
    }
  }

  log.Printf("Done MERGE")
}

func revealCleartext(tables [NUM_SERVERS]DumpReply) BitMatrix {
  var b BitMatrix

  // XOR all of the tables together and save 
  // it in the plaintext table
  for i := 0; i<TABLE_WIDTH; i++ {
    for j := 0; j<TABLE_HEIGHT; j++ {
      for serv := 0; serv<NUM_SERVERS; serv++ {
        b[i][j] = AddSlots(b[i][j], tables[serv].Entries[i][j])
      }
    }
  }

  return b
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
  // XXX check if good
  query, err := DecryptQuery(t.ServerIdx, prep.Query)
  if err == nil {
    reply.Okay = t.validateUpload(query)
  } else {
    log.Printf("Error in decryption: ", err)
    reply.Okay = false
  }

  t.pendingMutex.Lock()
  if t.pending == nil {
    t.pending = map[int64]InsertQuery{}
  }
  t.pending[prep.Uuid] = query
  t.pendingMutex.Unlock()

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
  t.processQuery(val)

  return nil
}

func (t *SlotTable) StorePlaintext(com *PlaintextArgs, reply *PlaintextReply) error {
  t.plainMutex.Lock()
  t.plain = com.Plaintext
  t.plainMutex.Unlock()

  t.entriesMutex.Lock()
  t.entries = *new([TABLE_WIDTH][TABLE_HEIGHT]SlotContents)
  t.entriesMutex.Unlock()

  t.State = State_AcceptUpload

  return nil
}

func (t *SlotTable) DumpTable(_ *int, reply *DumpReply) error {
  log.Printf("Dumping table %d\n", t.ServerIdx)
  t.entriesMutex.Lock()
  reply.Entries = t.entries
  t.entriesMutex.Unlock()
  return nil
}

func (t *SlotTable) DumpPlaintext(_ *int, reply *DumpReply) error {
  t.plainMutex.Lock()
  reply.Entries = t.plain
  t.plainMutex.Unlock()
  return nil
}

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processQuery(query InsertQuery) error {
  log.Printf("Processing query %d", t.ServerIdx)
  t.entriesMutex.Lock()
  for i := 0; i < TABLE_WIDTH; i++ {
    if query.XCoords[i] {
      for j := 0; j < TABLE_HEIGHT; j++ {
        t.entries[i][j] = AddSlots(t.entries[i][j], query.YCoords[j])
      }
    }
  }

  t.entriesMutex.Unlock()
  t.debugTable()
  return nil
}

func (t *SlotTable) validateUpload(query InsertQuery) bool {
  // XXX bogus for now
  return true
}

/***********
 * Initialization
 */

func (t *SlotTable) connectToServer(client **rpc.Client, serverAddr string, remoteIdx int, c chan int) {
  var err error
  certs := []tls.Certificate{utils.ServerCertificates[remoteIdx]}
  *client, err = utils.DialHTTPWithTLS("tcp", serverAddr, t.ServerIdx, certs)

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
    go t.connectToServer(&t.rpcClients[i], servers[i], i, c)
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
    go t.mergeWorker()
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

func (t *SlotTable) debugTable() {
  f := func(data [TABLE_WIDTH][TABLE_HEIGHT]SlotContents) {
    // it in the plaintext table
    for i := 0; i<TABLE_WIDTH; i++ {
      for j := 0; j<TABLE_HEIGHT; j++ {
        fmt.Printf("%v", data[i][j].Message)
      }
      fmt.Printf ("\n")
    }
  }
  fmt.Printf("---- Table ----\n")
  t.entriesMutex.Lock()
  f(t.entries)
  t.entriesMutex.Unlock()
  fmt.Printf("---- Plain ----\n")
  t.plainMutex.Lock()
  f(t.plain)
  t.plainMutex.Unlock()
  fmt.Printf("---------------\n")
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

