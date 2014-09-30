package db

import (
  "bytes"
  "crypto/tls"
  "errors"
  "fmt"
  "log"
  "math"
  "net/rpc"
  "time"

  "henrycg/email/utils"
  "henrycg/zkp/group"
)

// Time to wait between merges (in seconds)
const MERGE_TIME_DELAY time.Duration = 90

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
  //log.Printf("Request:", args)

  if t.State != State_AcceptUpload {
    return errors.New("Not accepting uploads")
  }

  incomingReqs<- args.Query
  reply.Magic = 5
  return nil
}

func readIncomingRequests(preps *[NUM_SERVERS]PrepareArgs,
  c chan [NUM_SERVERS]EncryptedInsertQuery, shouldMerge *bool) int {

  // Read once, then read until we get to 
  // the end

  n := 0
  appendOnce := func(queryList [NUM_SERVERS]EncryptedInsertQuery) {
    fmt.Printf("Got one")
    for i := 0; i < NUM_SERVERS; i++ {
      (*preps)[i].Queries = append((*preps)[i].Queries, queryList[i])
    }
    n++
  }

  isEmpty := func(q [NUM_SERVERS]EncryptedInsertQuery) bool {
    return q[0].Ciphertext == nil
  }

  first := <-c
  if isEmpty(first) {
    return n
  } else {
    appendOnce(first)
  }

  //time.Sleep(1*time.Second)

  *shouldMerge = false
  fmt.Printf("Here!")
  for {
    select {
      // Each element of incomingReqs has an array
      // of queries -- one for each server
      case queryList := <-c:
        // If we're starting to merge, then the marker down
        // the pipeline
        if isEmpty(queryList) {
          *shouldMerge = true
          return n
        } else {
          appendOnce(queryList)
        }
        continue

      default:
        fmt.Printf("Done")
        return n
    }
  }

  return n
}

func (t *SlotTable) submitPrepares() {
  for {
    uuid, err := utils.RandomInt64(math.MaxInt64)
    if err != nil {
      log.Printf("error in random")
      continue
    }

    var preps [NUM_SERVERS]PrepareArgs
    for i := 0; i < NUM_SERVERS; i++ {
      preps[i].Uuid = uuid
      preps[i].Queries = make([]EncryptedInsertQuery, 0)
    }

    var shouldMerge bool
    n := readIncomingRequests(&preps, incomingReqs, &shouldMerge)
    if n == 0 {
      // Merge is starting, so send marker down pipeline
      commitReqs <-beginMergeMarkerCommit
    }

    log.Printf("Send PREPARE %d", uuid)

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
      }(&preps[i], &replies[i], i, c)
    }

    // Wait for responses
    var r error
    for i:=0; i<NUM_SERVERS; i++ {
      r = <-c
      if r != nil {
        log.Fatal("Error in prepare: ", r)
      }
    }

    for i:=0; i<NUM_SERVERS; i++ {
      if !replies[i].Okay {
        log.Printf("Aborting commit")
        continue
      }
    }

    log.Printf("Done PREPARE %d", uuid)

    var commitArgs CommitArgs
    commitArgs.Uuid = uuid
    commitReqs <- commitArgs

    // If merge is beginning, send marker down
    // the pipeline
    if shouldMerge {
      commitReqs <-beginMergeMarkerCommit
    }
  }
}

func (t *SlotTable) submitCommits() {
  for {
    com := <-commitReqs
    if com.Uuid == beginMergeMarkerCommit.Uuid {
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
    time.Sleep(MERGE_TIME_DELAY*time.Second)
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
  MemCleanup()
}

func revealCleartext(tables [NUM_SERVERS]DumpReply) *BitMatrix {
  b := new(BitMatrix)

  // XOR all of the tables together and save 
  // it in the plaintext table
  for serv := 0; serv<NUM_SERVERS; serv++ {
    for i := 0; i<TABLE_HEIGHT; i++ {
      for j := 0; j<TABLE_WIDTH; j++ {
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
  okay := true

  var err error
  plainQueries := make([]*InsertQuery, len(prep.Queries))
  for i := 0; i < len(prep.Queries); i++ {
    plainQueries[i], err = DecryptQuery(t.ServerIdx, prep.Queries[i])
    if err == nil {
      okay = ValidateUpload(t.ServerIdx, plainQueries[i])
    } else {
      log.Printf("Error in decryption: ", err)
      okay = false
    }
  }

  reply.Okay = okay

  t.pendingMutex.Lock()
  if t.pending == nil {
    t.pending = map[int64]([]*InsertQuery){}
  }
  t.pending[prep.Uuid] = plainQueries
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

  // Update the database with the query
  t.processQuery(val)

  return nil
}

func (t *SlotTable) StorePlaintext(com *PlaintextArgs, reply *PlaintextReply) error {
  t.plainMutex.Lock()
  t.plain = com.Plaintext
  t.plainMutex.Unlock()

  t.entriesMutex.Lock()
  t.ClientsServed = 0
  clearBitMatrix(t.entries)
  t.entriesMutex.Unlock()

  t.State = State_AcceptUpload

  MemCleanup()
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

func clearBitMatrix(data *BitMatrix) {
  for i := 0; i<TABLE_HEIGHT; i++ {
    for j := 0; j<TABLE_WIDTH; j++ {
      for k := 0; k<SLOT_LENGTH; k++ {
        for l := 0; l<len(data[i][j].Message[k]); l++ {
          data[i][j].Message[k][l] = 0
        }
      }
    }
  }
}

func (t *SlotTable) debugTable() {
  /*
  f := func(data [TABLE_HEIGHT][TABLE_WIDTH]SlotContents) {
    // it in the plaintext table
    for i := 0; i<TABLE_HEIGHT; i++ {
      for j := 0; j<TABLE_WIDTH; j++ {
        fmt.Printf("%v", data[i][j].Message)
      }
      fmt.Printf ("\n")
    }
  }
  fmt.Printf("---- Table ----\n")
  t.entriesMutex.Lock()
  f(*t.entries)
  t.entriesMutex.Unlock()
  fmt.Printf("---- Plain ----\n")
  t.plainMutex.Lock()
  f(*t.plain)
  t.plainMutex.Unlock()
  fmt.Printf("---------------\n")
  */
  return
}

func elementsToBytes(elms []group.Element) []byte {
  var buf bytes.Buffer
  for i:=0; i<len(elms); i++ {
    buf.Write(utils.CommonCurve.Marshal(elms[i]))
  }

  return buf.Bytes()
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

func NewSlotTable(serverIdx int) *SlotTable {
  t := new(SlotTable)
  t.entries = new(BitMatrix)
  t.plain = new(BitMatrix)
  t.ServerIdx = serverIdx
  t.ClientsServed = 0
  t.State = State_AcceptUpload

  return t
}

func init() {
  beginMergeMarkerCommit.Uuid = 0
}

