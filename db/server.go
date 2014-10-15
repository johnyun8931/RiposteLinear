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
  auditReqs = make(chan AuditArgs, REQ_BUFFER_SIZE)
  commitReqs = make(chan CommitArgs, REQ_BUFFER_SIZE)

  beginMergeMarker [NUM_SERVERS]EncryptedInsertQuery
  beginMergeMarkerAudit AuditArgs
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
      auditReqs <-beginMergeMarkerAudit
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

    var auditArgs AuditArgs
    auditArgs.Uuid = uuid
    auditArgs.QueriesToAudit = new([NUM_SERVERS][]EncryptedAuditQuery)
    okay := true
    for i:=0; i<NUM_SERVERS; i++ {
      if !replies[i].Okay {
        log.Printf("Aborting commit")
        okay = false
      }

      auditArgs.QueriesToAudit[i] = replies[i].QueryToAudit
    }

    log.Printf("Done PREPARE %d", uuid)

    // If merge is beginning, send marker down
    // the pipeline
    if shouldMerge {
      auditReqs<- beginMergeMarkerAudit
    }

    if okay {
      auditReqs<- auditArgs
    }
  }
}

func (t *Server) submitAudits() {
  for {
    req := <-auditReqs
    log.Printf("Send AUDIT %d", req.Uuid)
    if req.Uuid == beginMergeMarkerCommit.Uuid {
      commitReqs<- beginMergeMarkerCommit
      continue
    }

    // Send out AUDIT request
    var a_reply AuditReply
    err := t.rpcClients[AUDIT_SERVER].Call("Auditor.Audit", req, &a_reply)
    if err != nil {
      log.Fatal("Error in audit: ", err)
    }

    if a_reply.Okay {
      var commitArgs CommitArgs
      commitArgs.Uuid = req.Uuid
      commitReqs <- commitArgs
    } else {
      log.Printf("FAILED audit, uuid: %v", req.Uuid)
    }

    log.Printf("Done AUDIT %d", req.Uuid)
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

  var parg PlaintextArgs
  parg.Plaintext = revealCleartext(replies)

  c = make(chan error, NUM_SERVERS)
  for i:=0; i<NUM_SERVERS; i++ {
    go func(i int, c chan error) {
      var p_reply PlaintextReply
      err := t.rpcClients[i].Call("Server.StorePlaintext", &parg, &p_reply)
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
      XorRows(&b[i], &tables[serv].Entries[i])
    }
  }

  return b
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
  reply.QueryToAudit = make([]EncryptedAuditQuery, len(prep.Queries))
  for i := 0; i < len(prep.Queries); i++ {
    plainQueries[i], err = DecryptQuery(t.ServerIdx, prep.Queries[i])
    if err == nil {
      okay = true
    } else {
      log.Printf("Error in decryption: ", err)
      okay = false
    }

    reply.QueryToAudit[i] = PrepareAudit(prep.Uuid, i, t.ServerIdx, plainQueries[i])
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
  t.entries.processQuery(val)
  log.Printf("Done processing query %d [len %v]", t.ServerIdx, len(val))

  return nil
}

func (t *Server) StorePlaintext(com *PlaintextArgs, reply *PlaintextReply) error {
  t.plainMutex.Lock()
  t.ClientsServed = 0
  t.entries.CopyAndClear(t.plain)
  t.plainMutex.Unlock()

  t.State = State_AcceptUpload

  MemCleanup()
  return nil
}


func (t *Server) DumpTable(_ *int, reply *DumpReply) error {
  log.Printf("Dumping table %d\n", t.ServerIdx)
  reply.Entries = new(BitMatrix)
  t.entries.CopyAndClear(reply.Entries)
  return nil
}

/*
func (t *Server) DumpPlaintext(_ *int, reply *DumpReply) error {
  t.plainMutex.Lock()
  reply.Entries = t.plain
  t.plainMutex.Unlock()
  return nil
}
*/

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

  n := len(utils.AllServers())
  c := make(chan int, n)
  servers := utils.AllServers()
  for i := range servers {
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
    go t.submitAudits()
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
  t.entries = new(SlotTable)
  t.plain = new(BitMatrix)
  t.ServerIdx = serverIdx
  t.ClientsServed = 0
  t.State = State_AcceptUpload

  return t
}

func init() {
  beginMergeMarkerCommit.Uuid = 0
}

