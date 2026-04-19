package db

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"net/rpc"
	"time"

	"bitbucket.org/henrycg/riposte/mulproof"
	"bitbucket.org/henrycg/riposte/utils"
	"bitbucket.org/henrycg/zkp/group"
)

// Time to wait between printing stats (in seconds)
const STATS_DELAY time.Duration = 10

// Number of pending requests that leader can buffer
const READY_BUFFER_SIZE = 400

// Number of server-side requests to allow in flight
const WORKER_THREADS = 16

func (t *Server) isLeader() bool {
	return (t.ServerIdx == 0)
}

func (t *Server) runLeaderControl() {
	state := leaderControlRuntime{
		epoch: EpochMeta{
			State: EpochStateNoActive,
		},
		accepted: map[int64](*AcceptQueryTuple){},
	}

	for cmd := range t.controlCh {
		switch c := cmd.(type) {
		case startEpochCommand:
			if c.durationSeconds <= 0 {
				c.reply <- startEpochResult{err: errors.New("Epoch duration must be positive")}
				continue
			}
			if state.epoch.State == EpochStateActive || state.epoch.State == EpochStateMerging {
				c.reply <- startEpochResult{err: errors.New("An epoch is already in progress")}
				continue
			}
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}

			now := time.Now().UTC()
			state.epoch.ID++
			state.epoch.State = EpochStateActive
			state.epoch.StartTime = now
			state.epoch.DurationSeconds = c.durationSeconds
			state.epoch.EndTime = now.Add(time.Duration(c.durationSeconds) * time.Second)
			state.epochTimer = time.AfterFunc(time.Duration(c.durationSeconds)*time.Second, func() {
				if err := t.finishEpoch(); err != nil {
					log.Printf("Error finishing epoch: %v", err)
				}
			})

			c.reply <- startEpochResult{
				reply: StartEpochReply{
					EpochID:      state.epoch.ID,
					State:        state.epoch.State.String(),
					StartUnix:    state.epoch.StartTime.Unix(),
					EndUnix:      state.epoch.EndTime.Unix(),
					DurationSecs: state.epoch.DurationSeconds,
				},
			}
		case epochStatusCommand:
			c.reply <- EpochStatusReply{
				EpochID:      state.epoch.ID,
				State:        state.epoch.State.String(),
				StartUnix:    state.epoch.StartTime.Unix(),
				EndUnix:      state.epoch.EndTime.Unix(),
				DurationSecs: state.epoch.DurationSeconds,
				Accepting:    state.epoch.State == EpochStateActive,
				LastResult:   state.lastResultPath,
			}
		case controlSnapshotCommand:
			c.reply <- controlSnapshot{
				epoch:      state.epoch,
				accepting:  state.epoch.State == EpochStateActive,
				lastResult: state.lastResultPath,
			}
		case upload1Command:
			if state.epoch.State != EpochStateActive {
				c.reply <- upload1Result{err: errors.New("No active epoch")}
				continue
			}
			uuid, err := utils.RandomInt64(math.MaxInt64)
			if err != nil {
				c.reply <- upload1Result{err: err}
				continue
			}
			tup := new(AcceptQueryTuple)
			tup.args1 = c.args
			utils.RandBytes(tup.hashKey[:])
			utils.RandBytes(tup.challenge[:])
			state.accepted[uuid] = tup
			var reply UploadReply1
			reply.Uuid = uuid
			copy(reply.HashKey[:], tup.hashKey[:])
			c.reply <- upload1Result{reply: reply}
		case upload2Command:
			data, okay := state.accepted[c.args.Uuid]
			if !okay || !bytes.Equal(data.hashKey[:], c.args.HashKey[:]) {
				c.reply <- upload2Result{err: errors.New("Bogus UUID")}
				continue
			}
			data.args2 = c.args
			var reply UploadReply2
			copy(reply.Challenge[:], data.challenge[:])
			c.reply <- upload2Result{reply: reply}
		case upload3Command:
			data, okay := state.accepted[c.args.Uuid]
			if !okay || !bytes.Equal(data.hashKey[:], c.args.HashKey[:]) {
				c.reply <- upload3Result{err: errors.New("Bogus UUID")}
				continue
			}
			data.args3 = c.args
			if t.ready != nil {
				t.ready <- c.args.Uuid
			}
			c.reply <- upload3Result{}
		case beginEpochMergeCommand:
			if state.epoch.State != EpochStateActive {
				c.reply <- beginEpochMergeResult{meta: state.epoch, shouldRun: false}
				continue
			}
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}
			state.epoch.State = EpochStateMerging
			c.reply <- beginEpochMergeResult{meta: state.epoch, shouldRun: true}
		case completeEpochMergeCommand:
			if c.err == nil {
				state.epoch.State = EpochStateCompleted
				state.lastResultPath = c.resultPath
				log.Printf("Completed epoch %d successfully result=%s", c.epochID, c.resultPath)
			} else {
				log.Printf("Epoch %d merge failed: %v", c.epochID, c.err)
			}
			state.epochTimer = nil
			c.reply <- struct{}{}
		case takeAcceptedSessionCommand:
			session, ok := state.accepted[c.uuid]
			if ok {
				delete(state.accepted, c.uuid)
			}
			c.reply <- takeAcceptedSessionResult{session: session, ok: ok}
		case stopEpochTimerCommand:
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}
			c.reply <- struct{}{}
		}
	}
}

/*******************
 * Leader code
 */

func (t *Server) Upload1(args *UploadArgs1, reply *UploadReply1) error {
	if !t.isLeader() {
		return errors.New("Only leader can accept uploads")
	}
	<-t.incoming1
	defer func() { t.incoming1 <- true }()

	replyCh := make(chan upload1Result, 1)
	t.controlCh <- upload1Command{args: args, reply: replyCh}
	res := <-replyCh
	if res.err != nil {
		return res.err
	}
	*reply = res.reply
	return nil
}

func (t *Server) Upload2(args *UploadArgs2, reply *UploadReply2) error {
	if !t.isLeader() {
		return errors.New("Only leader can accept uploads")
	}
	<-t.incoming2
	defer func() { t.incoming2 <- true }()

	replyCh := make(chan upload2Result, 1)
	t.controlCh <- upload2Command{args: args, reply: replyCh}
	res := <-replyCh
	if res.err != nil {
		return res.err
	}
	*reply = res.reply
	return nil
}

func (t *Server) Upload3(args *UploadArgs3, reply *UploadReply3) error {
	if !t.isLeader() {
		return errors.New("Only leader can accept uploads")
	}
	<-t.incoming3
	defer func() { t.incoming3 <- true }()

	replyCh := make(chan upload3Result, 1)
	t.controlCh <- upload3Command{args: args, reply: replyCh}
	res := <-replyCh
	if res.err != nil {
		return res.err
	}
	*reply = res.reply
	return nil
}

// Do everything
func (t *Server) processRequest() {
	for {
		uuid := <-t.ready
		t.amPublishingMutex.RLock()

		t.clientsServedMutex.Lock()
		t.clientsServed += 1
		t.clientsServedMutex.Unlock()

		shouldCommit := t.submitPrepares(uuid)
		t.submitCommits(uuid, shouldCommit)

		t.amPublishingMutex.RUnlock()
	}
}

func readIncomingRequests(preps *[NUM_SERVERS]PrepareArgs,
	c chan [NUM_SERVERS]EncryptedInsertQuery) bool {
	queryList := <-c
	if queryList[0].Ciphertext == nil {
		return true
	}

	for i := 0; i < NUM_SERVERS; i++ {
		(*preps)[i].Query1 = queryList[i]
	}

	return false
}

func (t *Server) submitPrepares(uuid int64) bool {
	var preps [NUM_SERVERS]PrepareArgs
	replyCh := make(chan takeAcceptedSessionResult, 1)
	t.controlCh <- takeAcceptedSessionCommand{uuid: uuid, reply: replyCh}
	sessionRes := <-replyCh
	if !sessionRes.ok || sessionRes.session == nil {
		log.Printf("Missing accepted session for uuid %d", uuid)
		return false
	}
	tup := sessionRes.session

	randPt := utils.RandInt(IntModulus)
	for i := 0; i < NUM_SERVERS; i++ {
		preps[i].Uuid = uuid
		preps[i].RandomPoint = randPt
		copy(preps[i].HashKey[:], tup.hashKey[:])
		copy(preps[i].Challenge[:], tup.challenge[:])
		preps[i].Query1 = tup.args1.Query[i]
		preps[i].Query2 = tup.args2.Query[i]
		preps[i].Query3 = tup.args3.Query[i]
	}

	//log.Printf("Send PREPARE %d", uuid)

	// Send out PREPARE request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]PrepareReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(prep *PrepareArgs, reply *PrepareReply, j int) {
			err := t.rpcClients[j].Call("Server.Prepare", prep, reply)
			if err != nil {
				c <- err
			} else {
				c <- nil
			}
		}(&preps[i], &replies[i], i)
	}

	// Wait for responses
	var r error
	for i := 0; i < NUM_SERVERS; i++ {
		r = <-c
		if r != nil {
			log.Fatal("Error in prepare: ", r)
		}
	}

	out := new(big.Int)
	for i := 0; i < NUM_SERVERS; i++ {
		out.Add(out, replies[i].OutShare)
	}

	out.Mod(out, IntModulus)

	if out.Sign() != 0 {
		log.Printf("FAIL!!!!!!!! <<<<< 1")
	}

	proofs1 := make([]*mulproof.AnsShare, NUM_SERVERS)
	proofs2 := make([]*mulproof.AnsShare, NUM_SERVERS)
	for i := 0; i < NUM_SERVERS; i++ {
		proofs1[i] = replies[i].AnsShare1
		proofs2[i] = replies[i].AnsShare2
	}

	if !mulproof.Decide(IntModulus, proofs1) {
		log.Printf("Proof 1 FAIL!!!!!!!! <<<<< :(")
	}

	if !mulproof.Decide(IntModulus, proofs2) {
		log.Printf("Proof 2 FAIL!!!!!!!! <<<<< :(")
	}

	okay := true
	return okay
}

func (t *Server) submitCommits(uuid int64, shouldCommit bool) {
	var com CommitArgs
	com.Uuid = uuid
	com.Commit = shouldCommit

	//log.Printf("Send COMMIT %d", com.Uuid)

	// Send out COMMIT request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]CommitReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(com *CommitArgs, reply *CommitReply, j int) {
			err := t.rpcClients[j].Call("Server.Commit", com, reply)
			if err != nil {
				c <- err
			} else {
				c <- nil
			}
		}(&com, &replies[i], i)
	}

	// Wait for responses
	var r error
	for i := 0; i < NUM_SERVERS; i++ {
		r = <-c
		if r != nil {
			log.Fatal("Error in commit: ", r)
		}
		//log.Printf("Got commit %v/%v", i, NUM_SERVERS)
	}

	//log.Printf("Done COMMIT %d", com.Uuid)
}

func (t *Server) printStats() {
	for {
		time.Sleep(STATS_DELAY * time.Second)
		t.clientsServedMutex.Lock()
		t.clientsTotal += t.clientsServed

		rate := float64(t.clientsServed) / float64(STATS_DELAY)
		t.rateHistory = append(t.rateHistory, rate)
		// Keep last 10
		t.rateHistory = t.rateHistory[1:]
		t.clientsServedMutex.Unlock()

		log.Printf("Served %v requests at %v reqs/sec [since start: %v]", t.clientsServed, rate, t.clientsTotal)
		rateStr := "Rate_History ["
		for i := 0; i < len(t.rateHistory); i++ {
			rateStr = fmt.Sprintf("%v %f", rateStr, t.rateHistory[i])
		}
		rateStr = fmt.Sprintf("%v]", rateStr)
		log.Printf("%v", rateStr)

		t.clientsServed = 0
	}
}

func (t *Server) sendMergeRequest() (string, error) {
	// Call each server and ask for their data
	// Send out COMMIT request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]DumpReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(reply *DumpReply, j int) {
			err := t.rpcClients[j].Call("Server.DumpTable", 0, reply)
			if err != nil {
				c <- err
			} else {
				c <- nil
			}
		}(&replies[i], i)
	}

	// Wait for responses
	var r error
	for i := 0; i < NUM_SERVERS; i++ {
		r = <-c
		if r != nil {
			log.Fatal("Error in merge: ", r)
		}
		log.Printf("Done merge")
	}

	var parg PlaintextArgs
	parg.Plaintext = revealCleartext(replies)

	var p_reply PlaintextReply
	err := t.rpcClients[0].Call("Server.StorePlaintext", &parg, &p_reply)

	if err != nil {
		return "", err
	}

	epoch := t.currentEpochMeta()
	resultPath, err := t.writePublishedResult(parg.Plaintext, epoch, time.Now())
	if err != nil {
		return "", err
	}
	if resultPath != "" {
		log.Printf("Published epoch result to %s", resultPath)
	}

	log.Printf("Done MERGE")
	return resultPath, nil
}

func revealCleartext(tables [NUM_SERVERS]DumpReply) *BitMatrix {
	b := new(BitMatrix)

	// XOR all of the tables together and save
	// it in the plaintext table
	//log.Printf("Revealing cleartext")
	for serv := 0; serv < NUM_SERVERS; serv++ {
		for i := 0; i < TABLE_HEIGHT; i++ {
			XorRows(&b[i], &tables[serv].Entries[i])
		}
	}
	//log.Printf("Done revealing cleartext")

	return b
}

/**************
 * Handle Updates
 */

func (t *Server) Prepare(prep *PrepareArgs, reply *PrepareReply) error {
	tup := new(InsertQueryTuple)

	copy(tup.hashKey[:], prep.HashKey[:])
	copy(tup.challenge[:], prep.Challenge[:])
	err := DecryptQuery(t.ServerIdx, prep.Query1, &tup.q1)
	if err != nil {
		panic("Decryption error")
	}

	err = DecryptQuery(t.ServerIdx, prep.Query2, &tup.q2)
	if err != nil {
		panic("Decryption error")
	}

	err = DecryptQuery(t.ServerIdx, prep.Query3, &tup.q3)
	if err != nil {
		panic("Decryption error")
	}

	t.pendingMutex.Lock()
	t.pending[prep.Uuid] = tup
	t.pendingMutex.Unlock()

	reply.OutShare = new(big.Int)
	reply.OutShare.Sub(tup.q3.TShare1, tup.q3.TShare2)
	reply.OutShare.Mod(reply.OutShare, IntModulus)

	zShare1 := new(big.Int)
	zShare2 := new(big.Int)
	t.entries.processQuery(tup, reply, t.ServerIdx == 1, zShare1, zShare2)

	// Check that t1 = z1^2
	reply.AnsShare1 = mulproof.Query(IntModulus, prep.RandomPoint, &tup.q3.TProof1,
		zShare1, zShare1, tup.q3.TShare1)

	// Check that t2 = m*z2
	reply.AnsShare2 = mulproof.Query(IntModulus, prep.RandomPoint, &tup.q3.TProof2,
		tup.q2.MsgShare, zShare2, tup.q3.TShare2)

	return nil
}

func (t *Server) Commit(com *CommitArgs, reply *CommitReply) error {
	t.pendingMutex.Lock()
	_, ok := t.pending[com.Uuid]
	t.pendingMutex.Unlock()

	if !ok {
		err := errors.New(fmt.Sprintf("Got commit msg for unknown UUID: %d", com.Uuid))
		return err
	}

	if !com.Commit {
		// Remove query from the database, since it
		// was malformed.
		log.Printf("Removing bogus query %v from DB", com.Uuid)

		panic("Got bogus query")
		// XXX: In a production implementation, we would expand
		// the DPF key and remove this bogus update to the database
		// by XORing the DPF key back into the database shares.
	}

	t.pendingMutex.Lock()
	delete(t.pending, com.Uuid)
	t.pendingMutex.Unlock()

	return nil
}

func (t *Server) StorePlaintext(args *PlaintextArgs, reply *PlaintextReply) error {
	//log.Printf("Storing plaintext")
	t.plainMutex.Lock()
	defer t.plainMutex.Unlock()
	t.plain = args.Plaintext
	return nil
}

func (t *Server) DumpTable(_ *int, reply *DumpReply) error {
	log.Printf("Dumping table %d\n", t.ServerIdx)
	reply.Entries = new(BitMatrix)
	t.entries.CopyToAndClear(reply.Entries)
	return nil
}

func (t *Server) DumpPlaintext(_ *int, reply *DumpReply) error {
	t.plainMutex.Lock()
	defer t.plainMutex.Unlock()
	reply.Entries = t.plain
	return nil
}

func (t *Server) StartEpoch(args *StartEpochArgs, reply *StartEpochReply) error {
	if !t.isLeader() {
		return errors.New("Only leader can start epochs")
	}
	replyCh := make(chan startEpochResult, 1)
	t.controlCh <- startEpochCommand{durationSeconds: args.DurationSeconds, reply: replyCh}
	res := <-replyCh
	if res.err != nil {
		return res.err
	}
	*reply = res.reply
	log.Printf("Started epoch %d with duration %ds", reply.EpochID, reply.DurationSecs)
	return nil
}

func (t *Server) EpochStatus(_ *EpochStatusArgs, reply *EpochStatusReply) error {
	replyCh := make(chan EpochStatusReply, 1)
	t.controlCh <- epochStatusCommand{reply: replyCh}
	*reply = <-replyCh
	return nil
}

func (t *Server) finishEpoch() error {
	if !t.isLeader() {
		return errors.New("Only leader can finish epochs")
	}

	beginCh := make(chan beginEpochMergeResult, 1)
	t.controlCh <- beginEpochMergeCommand{reply: beginCh}
	begin := <-beginCh
	if !begin.shouldRun {
		return nil
	}

	resultPath := ""
	var err error
	if t.mergeFn != nil {
		resultPath, err = t.mergeFn()
	}
	doneCh := make(chan struct{}, 1)
	t.controlCh <- completeEpochMergeCommand{
		epochID:    begin.meta.ID,
		resultPath: resultPath,
		err:        err,
		reply:      doneCh,
	}
	<-doneCh

	return err
}

/***********
 * Initialization
 */

func (t *Server) connectToServer(client **rpc.Client, serverAddr string, remoteIdx int, c chan error) {
	var err error
	certs := []tls.Certificate{utils.ServerCertificates[remoteIdx]}
	*client, err = utils.DialHTTPWithTLS("tcp", serverAddr, t.ServerIdx, certs)
	c <- err
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
	}

	return nil
}

func (t *Server) Initialize(*int, *int) error {
	if t.isLeader() {
		t.incoming1 = make(chan bool, READY_BUFFER_SIZE)
		t.incoming2 = make(chan bool, READY_BUFFER_SIZE)
		t.incoming3 = make(chan bool, READY_BUFFER_SIZE)
		t.ready = make(chan int64, READY_BUFFER_SIZE)
		go t.printStats()

		for i := 0; i < WORKER_THREADS; i++ {
			go t.processRequest()
		}

		for i := 0; i < READY_BUFFER_SIZE; i++ {
			t.incoming1 <- true
			t.incoming2 <- true
			t.incoming3 <- true
		}

		go func(t *Server) {
			// HACK wait until other servers have started
			time.Sleep(500 * time.Millisecond)
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
	for i := 0; i < len(elms); i++ {
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
	t.entries = NewSlotTable()
	t.plain = new(BitMatrix)
	t.ServerIdx = serverIdx
	t.ServerAddrs = serverAddrs
	t.rateHistory = make([]float64, 10)
	t.pending = map[int64](*InsertQueryTuple){}
	t.mergeFn = func() (string, error) {
		return t.sendMergeRequest()
	}
	if t.isLeader() {
		t.controlCh = make(chan leaderControlCommand, READY_BUFFER_SIZE)
		go t.runLeaderControl()
	}

	return t
}

func (t *Server) DoNothing(args *int, reply *int) error {
	// Just use this to test number
	// of requests can handle in a second
	t.clientsServedMutex.Lock()
	t.clientsServed++
	log.Printf("Served %v", t.clientsServed)
	t.clientsServedMutex.Unlock()

	return nil
}
