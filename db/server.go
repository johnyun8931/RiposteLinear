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
const MERGE_TIME_DELAY time.Duration = 10

var (
  incomingReqs = make(chan [NUM_SERVERS]EncryptedInsertQuery, REQ_BUFFER_SIZE)
  commitReqs = make(chan CommitArgs, REQ_BUFFER_SIZE)

  beginMergeMarker [NUM_SERVERS]EncryptedInsertQuery
  beginMergeMarkerCommit CommitArgs
)

func (t *Server) isLeader() bool {
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

func (t *Server) Upload(args *UploadArgs, reply *UploadReply) error {
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

func (t *Server) submitPrepares() {
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
        err := t.rpcClients[i].Call("Server.Prepare", prep, reply)
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

func (t *Server) submitCommits() {
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
        err := t.rpcClients[i].Call("Server.Commit", com, reply)
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

func (t *Server) mergeWorker() {
  for {
    time.Sleep(MERGE_TIME_DELAY*time.Second)
    t.sendMergeRequest()
  }
}

func (t *Server) sendMergeRequest() {
  // Call each server and ask for their data
  // Send out COMMIT request
  c := make(chan error, NUM_SERVERS)
  var replies [NUM_SERVERS]DumpReply
  for i:=0; i<NUM_SERVERS; i++ {
    go func(reply *DumpReply, i int, c chan error) {
      err := t.rpcClients[i].Call("Server.DumpTable", 0, reply)
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

  plaintext := revealCleartext(replies)
  n_collisions, darg := t.decideToDecrypt(plaintext)

  log.Printf("NOT IMPLEMENTED: Should look for malicious participant?")
  log.Printf("\t\t%v collision(s)", n_collisions)

  // Decide whether to decrypt

  // If YES, then decrypt send partial ciphertexts
  var d_reply DecryptReply
  err := t.rpcClients[1].Call("Server.Decrypt", &darg, &d_reply)
  if err != nil {
    log.Fatal("Error in decrypt: ", err)
  }

  log.Printf("Got %d plaintexts", len(d_reply.Cleartexts))
  for i, msg := range d_reply.Cleartexts {
    log.Printf("[%v] %v", i, msg)
  }

  log.Printf("Done MERGE")
  MemCleanup()
}

func revealCleartext(tables [NUM_SERVERS]DumpReply) *BitMatrix {
  b := NewBitMatrix()

  // XOR all of the tables together and save 
  // it in the plaintext table
  for serv := 0; serv<NUM_SERVERS; serv++ {
    for i := 0; i<TABLE_HEIGHT; i++ {
      XorRows(&b[i], &tables[serv].Entries[i])
    }
  }

  return b
}

func (t *Server) decideToDecrypt(plaintext *BitMatrix) (int, *DecryptArgs) {
  var d_args DecryptArgs
  d_args.ToDecrypt = make([][]byte, 0)

  var zeros [SLOT_LENGTH]byte

  n_collisions := 0
  for i := 0; i < len(plaintext); i++ {
    for j := 0; j < len(plaintext[i]); j += SLOT_LENGTH {
      slot := plaintext[i][j:(j+SLOT_LENGTH)]
      // Slot is not all zeros
      if bytes.Compare(slot[:], zeros[:]) != 0 {
        buf, err := DecryptSlot(0, slot[:])
        if err != nil {
          n_collisions++
        } else {
          d_args.ToDecrypt = append(d_args.ToDecrypt, buf)
        }
      }
    }
  }

  // XXX bogus collision logic for now
  return n_collisions, &d_args
}

func (t *Server) beginMerge() {
  // Stop accepting uploads
  t.State = State_PrepareForMerge

  // Insert a nil request into the pipeline so that we can figure
  // out when all pending requests have been processed.
  incomingReqs <- beginMergeMarker
}

/**************
 * Handle Updates
 */

func (t *Server) Prepare(prep *PrepareArgs, reply *PrepareReply) error {
  // XXX check if good
  okay := true

  var err error
  plainQueries := make([]*InsertQuery, len(prep.Queries))
  for i := 0; i < len(prep.Queries); i++ {
    plainQueries[i], err = DecryptQuery(t.ServerIdx, prep.Queries[i])
    if err == nil {
      okay = true
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

func (t *Server) Commit(com *CommitArgs, reply *CommitReply) error {
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
  log.Printf("Processing query %d [len %v]", t.ServerIdx, len(val))
  t.entries.processQuery(t.ServerIdx == 0, val)
  log.Printf("Done processing query %d [len %v]", t.ServerIdx, len(val))

  return nil
}

func (t *Server) Decrypt(args *DecryptArgs, reply *DecryptReply) error {
  if t.ServerIdx != 1 {
    log.Fatal("Only server 1 should decrypt!")
  }

  reply.Cleartexts = make([][]byte, 0)

  for _, item := range args.ToDecrypt {
    buf, err := DecryptSlot(1, item[:])
    if err == nil {
      reply.Cleartexts = append(reply.Cleartexts, buf)
    } else {
      log.Printf("Decrypt error! ", err)
    }
  }

  t.ClientsServed = 0
  t.State = State_AcceptUpload
  return nil
}


func (t *Server) DumpTable(_ *int, reply *DumpReply) error {
  log.Printf("Dumping table %d\n", t.ServerIdx)
  reply.Entries = NewBitMatrix()
  t.entries.CopyAndClear(reply.Entries)
  return nil
}


/***********
 * Initialization
 */

func (t *Server) connectToServer(client **rpc.Client, serverAddr string, remoteIdx int, c chan int) {
  var err error
  certs := []tls.Certificate{utils.ServerCertificates[remoteIdx]}
  *client, err = utils.DialHTTPWithTLS("tcp", serverAddr, t.ServerIdx, certs)

  if err == nil {
    c <- 1
  } else {
    c <- -1
  }
}

func (t *Server) openConnections() error {
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

func (t *Server) Initialize(*int, *int) error {
  if (t.isLeader()) {
    go t.submitPrepares()
    go t.submitCommits()
    go t.mergeWorker()
    go func(t *Server) {
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

func elementsToBytes(elms []group.Element) []byte {
  var buf bytes.Buffer
  for i:=0; i<len(elms); i++ {
    buf.Write(utils.CommonCurve.Marshal(elms[i]))
  }

  return buf.Bytes()
}

/*
func (t *Server) Download(args *DownloadArgs, reply *DownloadReply) error {
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

func NewServer(serverIdx int) *Server {
  t := new(Server)
  t.entries = NewSlotTable()
  t.plain = NewBitMatrix()
  t.ServerIdx = serverIdx
  t.ClientsServed = 0
  t.State = State_AcceptUpload

  return t
}

func init() {
  beginMergeMarkerCommit.Uuid = 0
}

