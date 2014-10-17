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
const MERGE_TIME_DELAY time.Duration = 60*60*8

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
      go func(prep *PrepareArgs, reply *PrepareReply, i int) {
        err := t.rpcClients[i].Call("Server.Prepare", prep, reply)
        if err != nil {
          c <- err
        } else {
          c <- nil
        }
      }(&preps[i], &replies[i], i)
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
    if len(replies[0].QueryToAudit) != len(replies[1].QueryToAudit) {
      panic("Length mismatch")
    }

    auditList := make([][NUM_SERVERS]EncryptedAuditQuery,
      len(replies[0].QueryToAudit))
    auditArgs.QueriesToAudit = &auditList

    okay := true
    for i:=0; i<NUM_SERVERS; i++ {
      if !replies[i].Okay {
        log.Printf("Aborting commit")
        okay = false
      }

      for j := 0; j<len(replies[i].QueryToAudit); j++ {
        (*auditArgs.QueriesToAudit)[j][i] = replies[i].QueryToAudit[j]
      }
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

    var commitArgs CommitArgs
    commitArgs.Uuid = req.Uuid
    commitArgs.Commit = a_reply.Okay
    commitReqs <- commitArgs
    for i := range a_reply.Okay {
      if !a_reply.Okay[i] {
        log.Printf("FAILED audit, uuid: %v[%v]", req.Uuid, i)
      }
    }

    log.Printf("Done AUDIT %d => Okay? %v", req.Uuid, a_reply.Okay)
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
      go func(com *CommitArgs, reply *CommitReply, i int) {
        err := t.rpcClients[i].Call("Server.Commit", com, reply)
        if err != nil {
          c <- err
        } else {
          c <- nil
        }
      }(&com, &replies[i], i)
    }

    // Wait for responses
    var r error
    for i:=0; i<NUM_SERVERS; i++ {
      r = <-c
      if r != nil {
        log.Fatal("Error in commit: ", r)
      }
      log.Printf("Got commit %v/%v", i, NUM_SERVERS)
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
    go func(reply *DumpReply, i int) {
      err := t.rpcClients[i].Call("Server.DumpTable", 0, reply)
      if err != nil {
        c <- err
      } else {
        c <- nil
      }
    }(&replies[i], i)
  }

  // Wait for responses
  var r error
  for i:=0; i<NUM_SERVERS; i++ {
    r = <-c
    if r != nil {
      log.Fatal("Error in merge: ", r)
    }
    log.Printf("Done merge")
  }

  var parg PlaintextArgs
  parg.Plaintext = revealCleartext(replies)

  c = make(chan error, NUM_SERVERS)
  for i:=0; i<NUM_SERVERS; i++ {
    go func(i int) {
      var p_reply PlaintextReply
      err := t.rpcClients[i].Call("Server.StorePlaintext", &parg, &p_reply)
      c <- err
    }(i)
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
  log.Printf("Revealing cleartext")
  for serv := 0; serv<NUM_SERVERS; serv++ {
    for i := 0; i<TABLE_HEIGHT; i++ {
      XorRows(&b[i], &tables[serv].Entries[i])
    }
  }
  log.Printf("Done revealing cleartext")

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
  var err error
  plainQueries := make([]*InsertQuery, len(prep.Queries))
  reply.QueryToAudit = make([]EncryptedAuditQuery, len(prep.Queries))

  n_queries := len(prep.Queries)
  c := make(chan error, n_queries)
  for i:=0; i<n_queries; i++ {
    go func(i int) {
      plainQueries[i], err = DecryptQuery(t.ServerIdx, prep.Queries[i])
      if err != nil {
        c <-err
        return
      }

      // Generate out each of the seeds and XOR into the database.
      // At the same time, generate an XOR of all of the seed outputs.
      log.Printf("XORing into table for %v[%v]", prep.Uuid, i)
      row, err := t.entries.processQuery(plainQueries[i])
      if err != nil {
        c <-err
        return
      }

      log.Printf("Preparing audit for %v[%v]", prep.Uuid, i)
      // Use the XORd seeds (row) to generate the audit request
      reply.QueryToAudit[i] = prepareAudit(prep.Uuid, i, t.ServerIdx, plainQueries[i], row)
      log.Printf("Done preparing audit for %v[%v]", prep.Uuid, i)

      c <- nil
    }(i)
  }

  var r error
  okay := true
  for i:=0; i<n_queries; i++ {
    r = <-c
    if r != nil {
      okay = false
      log.Fatal("Error in decrypt/audit: ", r)
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
  queries, ok := t.pending[com.Uuid]

  if !ok {
    err := errors.New(fmt.Sprintf("Got commit msg for unknown UUID: %d",  com.Uuid))
    t.pendingMutex.Unlock()
    return err
  }

  toWait := 0
  c := make(chan int, len(queries))
  for i := range queries {
    if !com.Commit[i] {
      // Remove query from the database, since it
      // was malformed.
      log.Printf("Removing bogus query %v[%v] from DB", com.Uuid, i)
      go t.entries.processQuery(queries[i])
      toWait += 1
      c <- 0
    }
  }

  for i := 0; i < toWait; i++ {
    <-c
  }

  delete(t.pending, com.Uuid)
  t.pendingMutex.Unlock()

  t.clientsServedMutex.Lock()
  t.clientsServed += len(queries)
  log.Printf("Processed %v queries so far", t.clientsServed)
  t.clientsServedMutex.Unlock()

  return nil
}

func (t *Server) StorePlaintext(com *PlaintextArgs, reply *PlaintextReply) error {
  t.clientsServedMutex.Lock()
  t.clientsServed = 0
  t.clientsServedMutex.Unlock()

  log.Printf("Storing plaintext")
  t.plainMutex.Lock()
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

func (t *Server) connectToServer(client **rpc.Client, serverAddr string, remoteIdx int, c chan error) {
  var err error
  certs := []tls.Certificate{utils.ServerCertificates[remoteIdx]}
  *client, err = utils.DialHTTPWithTLS("tcp", serverAddr, t.ServerIdx, certs)
  c<- err
}

func (t *Server) openConnections() error {
  log.Printf("Waiting 2 seconds for other servers to boot")
  time.Sleep(1000 * time.Millisecond)

  if !t.isLeader() {
    return errors.New("Only leader should open connections")
  }

  c := make(chan error, len(t.ServerAddrs))
  for i := range t.ServerAddrs {
    go t.connectToServer(&t.rpcClients[i], t.ServerAddrs[i], i, c)
  }

  // Wait for all connections
  failed := false
  for i := 0; i < len(t.ServerAddrs); i++ {
    err := <-c
    if err != nil {
      log.Printf("Error connecting to server: %v", err)
    }
  }

  if failed {
    return errors.New("Connection failed")
  } else {
    t.State = State_AcceptUpload
    return nil
  }
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

func NewServer(serverIdx int, serverAddrs []string) *Server {
  t := new(Server)
  t.entries = new(SlotTable)
  t.plain = new(BitMatrix)
  t.ServerIdx = serverIdx
  t.ServerAddrs = serverAddrs
  t.clientsServed = 0
  t.State = State_Booting

  return t
}

func init() {
  beginMergeMarkerCommit.Uuid = 0
}

