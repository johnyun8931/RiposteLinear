package db

import (
	"bytes"
	"context"
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

const defaultIngestionReceiveBatchSize = 1
const defaultIngestionWorkerErrorBackoff = 250 * time.Millisecond

func (t *Server) isLeader() bool {
	return (t.ServerIdx == 0)
}

func (t *Server) runLeaderControl() {
	state := leaderControlRuntime{
		epoch: EpochMeta{
			State: EpochStateNoActive,
		},
		accepted:  map[int64](*AcceptQueryTuple){},
		peerState: PeerConnectionsConnecting,
	}

	startMergeIfDrained := func() {
		if state.epoch.State != EpochStateClosing || len(state.accepted) != 0 || !t.ingestionQueueDrained() {
			return
		}
		state.epoch.State = EpochStateMerging
		waiters := state.mergeWaiters
		state.mergeWaiters = nil
		for i, waiter := range waiters {
			waiter <- beginEpochMergeResult{meta: state.epoch, shouldRun: i == 0}
		}
	}
	releaseMergeWaiters := func() {
		waiters := state.mergeWaiters
		state.mergeWaiters = nil
		for _, waiter := range waiters {
			waiter <- beginEpochMergeResult{meta: state.epoch, shouldRun: false}
		}
	}

	for cmd := range t.controlCh {
		switch c := cmd.(type) {
		case startEpochCommand:
			if c.durationSeconds <= 0 {
				c.reply <- startEpochResult{err: errors.New("Epoch duration must be positive")}
				continue
			}
			if state.epoch.State == EpochStateActive || state.epoch.State == EpochStateClosing || state.epoch.State == EpochStateMerging {
				c.reply <- startEpochResult{err: errors.New("An epoch is already in progress")}
				continue
			}
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}

			now := time.Now().UTC()
			if c.startUnix > 0 {
				now = time.Unix(c.startUnix, 0).UTC()
			}
			if c.epochID > 0 {
				state.epoch.ID = c.epochID
			} else {
				state.epoch.ID++
			}
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
				peerState:  state.peerState,
				peerError:  state.peerError,
			}
		case updatePeerConnectionStateCommand:
			state.peerState = c.state
			state.peerError = c.err
			c.reply <- struct{}{}
		case upload1Command:
			if state.epoch.State != EpochStateActive {
				c.reply <- upload1Result{err: errors.New("No active epoch")}
				continue
			}
			if c.args.RouteRow < 0 || c.args.RouteRow >= TABLE_HEIGHT {
				c.reply <- upload1Result{err: fmt.Errorf("route row %d outside local table height %d", c.args.RouteRow, TABLE_HEIGHT)}
				continue
			}
			var uuid int64
			var hashKey [32]byte
			if c.args.UseAssignedSession {
				if c.args.AssignedUUID <= 0 {
					c.reply <- upload1Result{err: errors.New("assigned session uuid must be positive")}
					continue
				}
				if _, exists := state.accepted[c.args.AssignedUUID]; exists {
					c.reply <- upload1Result{err: errors.New("assigned session uuid already exists")}
					continue
				}
				uuid = c.args.AssignedUUID
				hashKey = c.args.AssignedHashKey
			} else {
				var err error
				uuid, err = utils.RandomInt64(math.MaxInt64)
				if err != nil {
					c.reply <- upload1Result{err: err}
					continue
				}
				utils.RandBytes(hashKey[:])
			}
			tup := new(AcceptQueryTuple)
			tup.args1 = c.args
			copy(tup.hashKey[:], hashKey[:])
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
			if data.args1 == nil || data.args2 == nil {
				c.reply <- upload3Result{err: errors.New("Incomplete upload session")}
				continue
			}
			data.args3 = c.args
			msg := t.completedUploadMessageFromSession(state.epoch, c.args.Uuid, data)
			if _, err := t.ingestionQueue.Enqueue(context.Background(), msg); err != nil {
				c.reply <- upload3Result{err: err}
				continue
			}
			delete(state.accepted, c.args.Uuid)
			startMergeIfDrained()
			c.reply <- upload3Result{}
		case beginEpochMergeCommand:
			if state.epoch.State != EpochStateActive {
				if state.epoch.State == EpochStateClosing {
					if len(state.mergeWaiters) > 0 {
						log.Printf("Duplicate epoch merge waiter while epoch %d is closing; existing_waiters=%d", state.epoch.ID, len(state.mergeWaiters))
					}
					state.mergeWaiters = append(state.mergeWaiters, c.reply)
				} else {
					c.reply <- beginEpochMergeResult{meta: state.epoch, shouldRun: false}
				}
				continue
			}
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}
			if len(state.accepted) > 0 {
				state.epoch.State = EpochStateClosing
				if len(state.mergeWaiters) > 0 {
					log.Printf("Duplicate epoch merge waiter while epoch %d is closing; existing_waiters=%d", state.epoch.ID, len(state.mergeWaiters))
				}
				state.mergeWaiters = append(state.mergeWaiters, c.reply)
				log.Printf("Closing epoch %d; waiting for %d admitted upload(s) to drain", state.epoch.ID, len(state.accepted))
				startMergeIfDrained()
			} else {
				state.epoch.State = EpochStateMerging
				c.reply <- beginEpochMergeResult{meta: state.epoch, shouldRun: true}
			}
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
			startMergeIfDrained()
		case ingestionQueueDrainedCommand:
			startMergeIfDrained()
			c.reply <- struct{}{}
		case stopEpochTimerCommand:
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}
			c.reply <- struct{}{}
		case abortEpochCommand:
			if state.epoch.ID != c.epochID || (state.epoch.State != EpochStateActive && state.epoch.State != EpochStateClosing) {
				c.reply <- errors.New("No matching active epoch to abort")
				continue
			}
			if state.epochTimer != nil {
				state.epochTimer.Stop()
				state.epochTimer = nil
			}
			releaseMergeWaiters()
			state.epoch = EpochMeta{State: EpochStateNoActive}
			c.reply <- nil
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
	if t.replicaID == CompletedUploadReplicaStandby {
		return errors.New("standby replica cannot accept direct uploads")
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
	if t.replicaID == CompletedUploadReplicaStandby {
		return errors.New("standby replica cannot accept direct uploads")
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
	if t.replicaID == CompletedUploadReplicaStandby {
		return errors.New("standby replica cannot accept direct uploads")
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

func (t *Server) setPeerConnectionState(state PeerConnectionState, err error) {
	if !t.isLeader() || t.controlCh == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	replyCh := make(chan struct{}, 1)
	t.controlCh <- updatePeerConnectionStateCommand{
		state: state,
		err:   errText,
		reply: replyCh,
	}
	<-replyCh
}

func (t *Server) requirePeerReady(op string) error {
	if !t.isLeader() {
		return nil
	}
	state, errText := t.currentPeerState()
	switch state {
	case PeerConnectionsReady:
		return nil
	case PeerConnectionsFailed:
		if errText != "" {
			return fmt.Errorf("leader not ready for %s: peer RPC connection setup failed: %s", op, errText)
		}
		return fmt.Errorf("leader not ready for %s: peer RPC connection setup failed", op)
	case PeerConnectionsConnecting:
		fallthrough
	default:
		return fmt.Errorf("leader not ready for %s: peer RPC connections still initializing", op)
	}
}

func (t *Server) requirePeerRPCClients(op string) ([NUM_SERVERS]*rpc.Client, error) {
	var clients [NUM_SERVERS]*rpc.Client
	if err := t.requirePeerReady(op); err != nil {
		return clients, err
	}
	for i := 0; i < NUM_SERVERS; i++ {
		if t.rpcClients[i] == nil {
			return clients, fmt.Errorf("leader not ready for %s: peer RPC client %d not established", op, i)
		}
		clients[i] = t.rpcClients[i]
	}
	return clients, nil
}

func (t *Server) completedUploadMessageFromSession(epoch EpochMeta, uuid int64, tup *AcceptQueryTuple) CompletedUploadMessage {
	return CompletedUploadMessage{
		EpochID:   epoch.ID,
		ShardID:   t.ShardID,
		Uuid:      uuid,
		HashKey:   tup.hashKey,
		Challenge: tup.challenge,
		GlobalRow: t.globalRowStart + tup.args1.RouteRow,
		LocalRow:  tup.args1.RouteRow,
		Args1:     *tup.args1,
		Args2:     *tup.args2,
		Args3:     *tup.args3,
	}
}

func (t *Server) ingestionQueueStats() IngestionQueueStats {
	if t.ingestionQueue == nil {
		return IngestionQueueStats{}
	}
	return t.ingestionQueue.Stats()
}

func (t *Server) recordIngestionProcessed() {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	t.ingestionDiagnostics.processedCount++
}

func (t *Server) recordIngestionAck() {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	t.ingestionDiagnostics.ackCount++
}

func (t *Server) recordCompletedUploadCommitted() {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	t.ingestionDiagnostics.committedCount++
}

func (t *Server) recordCompletedUploadDuplicateSkip() {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	t.ingestionDiagnostics.duplicateSkipCount++
}

func (t *Server) recordIngestionError(kind string, err error) {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	switch kind {
	case "receive":
		t.ingestionDiagnostics.receiveErrorCount++
	case "process":
		t.ingestionDiagnostics.processErrorCount++
	case "ack":
		t.ingestionDiagnostics.ackErrorCount++
	}
	t.ingestionDiagnostics.lastError = err.Error()
	t.ingestionDiagnostics.lastErrorTime = time.Now().UTC()
}

func (t *Server) recordCompletedUploadLedgerError(kind string, err error) {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	switch kind {
	case "begin":
		t.ingestionDiagnostics.ledgerBeginErrorCount++
	case "complete":
		t.ingestionDiagnostics.ledgerCompleteErrorCount++
	}
	t.ingestionDiagnostics.ledgerLastError = err.Error()
	t.ingestionDiagnostics.ledgerLastErrorTime = time.Now().UTC()
}

func (t *Server) ingestionDiagnosticsSnapshot() ingestionDiagnostics {
	t.ingestionDiagnostics.mu.Lock()
	defer t.ingestionDiagnostics.mu.Unlock()
	return ingestionDiagnostics{
		processedCount:           t.ingestionDiagnostics.processedCount,
		ackCount:                 t.ingestionDiagnostics.ackCount,
		receiveErrorCount:        t.ingestionDiagnostics.receiveErrorCount,
		processErrorCount:        t.ingestionDiagnostics.processErrorCount,
		ackErrorCount:            t.ingestionDiagnostics.ackErrorCount,
		committedCount:           t.ingestionDiagnostics.committedCount,
		duplicateSkipCount:       t.ingestionDiagnostics.duplicateSkipCount,
		ledgerBeginErrorCount:    t.ingestionDiagnostics.ledgerBeginErrorCount,
		ledgerCompleteErrorCount: t.ingestionDiagnostics.ledgerCompleteErrorCount,
		lastError:                t.ingestionDiagnostics.lastError,
		lastErrorTime:            t.ingestionDiagnostics.lastErrorTime,
		ledgerLastError:          t.ingestionDiagnostics.ledgerLastError,
		ledgerLastErrorTime:      t.ingestionDiagnostics.ledgerLastErrorTime,
	}
}

func (t *Server) ingestionQueueDrained() bool {
	stats := t.ingestionQueueStats()
	return stats.Depth == 0 && stats.Inflight == 0
}

func (t *Server) notifyIngestionQueueDrained() {
	if !t.isLeader() || t.controlCh == nil {
		return
	}
	replyCh := make(chan struct{}, 1)
	t.controlCh <- ingestionQueueDrainedCommand{reply: replyCh}
	<-replyCh
}

// Do everything
func (t *Server) processRequest() {
	for {
		items, err := t.ingestionQueue.Receive(context.Background(), t.ingestionBatchSize)
		if err != nil {
			log.Printf("Error receiving ingestion work: %v", err)
			t.recordIngestionError("receive", err)
			time.Sleep(t.ingestionErrorBackoff)
			continue
		}
		for _, item := range items {
			t.processIngestionJob(item)
		}
	}
}

func (t *Server) processIngestionJob(item QueuedCompletedUploadMessage) {
	msg := item.Message
	msg.ReplicaID = t.replicaID
	t.amPublishingMutex.RLock()
	defer t.amPublishingMutex.RUnlock()

	begin, err := t.completedUploadLedger.BeginProcessing(context.Background(), msg, time.Now().UTC(), t.completedUploadProcessingTTL)
	if err != nil {
		log.Printf("Error beginning completed upload ledger processing uuid %d: %v", msg.Uuid, err)
		t.recordCompletedUploadLedgerError("begin", err)
		return
	}
	if begin.AlreadyCommitted {
		log.Printf("Skipping already committed completed upload uuid %d", msg.Uuid)
		t.recordCompletedUploadDuplicateSkip()
		if err := t.ingestionQueue.Ack(context.Background(), item.ReceiptHandle); err != nil {
			log.Printf("Error acking duplicate committed ingestion uuid %d: %v", msg.Uuid, err)
			t.recordIngestionError("ack", err)
			return
		}
		t.recordIngestionAck()
		t.notifyIngestionQueueDrained()
		return
	}

	t.clientsServedMutex.Lock()
	t.clientsServed += 1
	t.clientsServedMutex.Unlock()

	shouldCommit, err := t.processUploadFn(msg)
	if err != nil {
		log.Printf("Error preparing uuid %d: %v", msg.Uuid, err)
		t.recordIngestionError("process", err)
		return
	}
	if err := t.commitUploadFn(msg.Uuid, shouldCommit); err != nil {
		log.Printf("Error committing uuid %d: %v", msg.Uuid, err)
		t.recordIngestionError("process", err)
		return
	}
	t.recordIngestionProcessed()
	if err := t.completedUploadLedger.CompleteProcessing(context.Background(), begin.Lease, time.Now().UTC()); err != nil {
		log.Printf("Error completing completed upload ledger processing uuid %d: %v", msg.Uuid, err)
		t.recordCompletedUploadLedgerError("complete", err)
		return
	}
	t.recordCompletedUploadCommitted()
	if err := t.ingestionQueue.Ack(context.Background(), item.ReceiptHandle); err != nil {
		log.Printf("Error acking ingestion uuid %d: %v", msg.Uuid, err)
		t.recordIngestionError("ack", err)
		return
	}
	t.recordIngestionAck()
	t.notifyIngestionQueueDrained()
}

func (t *Server) submitPrepares(msg CompletedUploadMessage) (bool, error) {
	clients, err := t.requirePeerRPCClients("prepare")
	if err != nil {
		return false, err
	}
	var preps [NUM_SERVERS]PrepareArgs

	randPt := utils.RandInt(IntModulus)
	for i := 0; i < NUM_SERVERS; i++ {
		preps[i].Uuid = msg.Uuid
		preps[i].RandomPoint = randPt
		copy(preps[i].HashKey[:], msg.HashKey[:])
		copy(preps[i].Challenge[:], msg.Challenge[:])
		preps[i].Query1 = msg.Args1.Query[i]
		preps[i].Query2 = msg.Args2.Query[i]
		preps[i].Query3 = msg.Args3.Query[i]
	}

	//log.Printf("Send PREPARE %d", uuid)

	// Send out PREPARE request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]PrepareReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(prep *PrepareArgs, reply *PrepareReply, j int) {
			err := clients[j].Call("Server.Prepare", prep, reply)
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
			return false, fmt.Errorf("prepare fanout failed: %w", r)
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
	return okay, nil
}

func (t *Server) submitCommits(uuid int64, shouldCommit bool) error {
	clients, err := t.requirePeerRPCClients("commit")
	if err != nil {
		return err
	}
	var com CommitArgs
	com.Uuid = uuid
	com.Commit = shouldCommit

	//log.Printf("Send COMMIT %d", com.Uuid)

	// Send out COMMIT request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]CommitReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(com *CommitArgs, reply *CommitReply, j int) {
			err := clients[j].Call("Server.Commit", com, reply)
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
			return fmt.Errorf("commit fanout failed: %w", r)
		}
		//log.Printf("Got commit %v/%v", i, NUM_SERVERS)
	}

	//log.Printf("Done COMMIT %d", com.Uuid)
	return nil
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
	clients, err := t.requirePeerRPCClients("merge")
	if err != nil {
		return "", err
	}
	// Call each server and ask for their data
	// Send out COMMIT request
	c := make(chan error, NUM_SERVERS)
	var replies [NUM_SERVERS]DumpReply
	for i := 0; i < NUM_SERVERS; i++ {
		go func(reply *DumpReply, j int) {
			err := clients[j].Call("Server.DumpTable", 0, reply)
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
			return "", fmt.Errorf("merge fanout failed: %w", r)
		}
		log.Printf("Done merge")
	}

	var parg PlaintextArgs
	parg.Plaintext = revealCleartext(replies)

	var p_reply PlaintextReply
	err = clients[0].Call("Server.StorePlaintext", &parg, &p_reply)

	if err != nil {
		return "", err
	}

	epoch := t.currentEpochMeta()
	completedAt := time.Now().UTC()
	resultPath, err := t.writePublishedResult(parg.Plaintext, epoch, completedAt)
	if err != nil {
		return "", err
	}
	if resultPath != "" {
		log.Printf("Published epoch result to %s", resultPath)
	}
	if t.tablePublisher != nil {
		publication, err := t.tablePublisher.PublishTable(context.Background(), parg.Plaintext, epoch, completedAt)
		if err != nil {
			return "", fmt.Errorf("publish table to s3: %w", err)
		}
		t.setLastTablePublication(publication)
		log.Printf("Published epoch table to %s manifest=%s", publication.TableURI, publication.ManifestURI)
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
	if err := t.requirePeerReady("start epoch"); err != nil {
		return err
	}
	if t.replicaID == CompletedUploadReplicaStandby {
		log.Printf("Promoting standby replica to active on StartEpoch")
		t.replicaID = CompletedUploadReplicaActive
		t.standbyIngestionFanout = false
	}
	replyCh := make(chan startEpochResult, 1)
	t.controlCh <- startEpochCommand{
		durationSeconds: args.DurationSeconds,
		epochID:         args.EpochID,
		startUnix:       args.StartUnix,
		reply:           replyCh,
	}
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

func (t *Server) Status(_ *StatusArgs, reply *StatusReply) error {
	reply.Healthy = true
	reply.IsLeader = t.isLeader()
	reply.ServerIndex = t.ServerIdx
	reply.ShardID = t.ShardID
	reply.ReplicaID = t.replicaID
	reply.IngestionQueueBackend = t.ingestionQueueBackend
	reply.StandbyIngestionFanoutConfigured = t.standbyIngestionFanout
	stats := t.ingestionQueueStats()
	reply.IngestionQueueDepth = stats.Depth
	reply.IngestionInflightCount = stats.Inflight
	diagnostics := t.ingestionDiagnosticsSnapshot()
	reply.IngestionProcessedCount = diagnostics.processedCount
	reply.IngestionAckCount = diagnostics.ackCount
	reply.IngestionReceiveErrors = diagnostics.receiveErrorCount
	reply.IngestionProcessErrors = diagnostics.processErrorCount
	reply.IngestionAckErrors = diagnostics.ackErrorCount
	reply.IngestionLastError = diagnostics.lastError
	if !diagnostics.lastErrorTime.IsZero() {
		reply.IngestionLastErrorUnix = diagnostics.lastErrorTime.Unix()
	}
	reply.CompletedUploadLedgerBackend = t.completedUploadLedgerBackend
	reply.CompletedUploadCommittedCount = diagnostics.committedCount
	reply.CompletedUploadDuplicateSkipCount = diagnostics.duplicateSkipCount
	reply.CompletedUploadLedgerBeginErrors = diagnostics.ledgerBeginErrorCount
	reply.CompletedUploadLedgerCompleteErrors = diagnostics.ledgerCompleteErrorCount
	reply.CompletedUploadLedgerLastError = diagnostics.ledgerLastError
	if !diagnostics.ledgerLastErrorTime.IsZero() {
		reply.CompletedUploadLedgerLastErrorUnix = diagnostics.ledgerLastErrorTime.Unix()
	}
	if !t.isLeader() {
		reply.State = EpochStateNoActive.String()
		reply.PeerState = "not_applicable"
		return nil
	}

	snapshot := t.currentControlSnapshot()
	reply.EpochID = snapshot.epoch.ID
	reply.State = snapshot.epoch.State.String()
	reply.StartUnix = snapshot.epoch.StartTime.Unix()
	reply.EndUnix = snapshot.epoch.EndTime.Unix()
	reply.DurationSecs = snapshot.epoch.DurationSeconds
	reply.Accepting = snapshot.accepting
	reply.LastResult = snapshot.lastResult
	reply.LastTableS3URI, reply.LastManifestS3URI = t.lastTablePublicationURIs()
	reply.PeerState = snapshot.peerState.String()
	reply.PeerError = snapshot.peerError
	if snapshot.peerState == PeerConnectionsFailed {
		reply.Healthy = false
	}
	return nil
}

func (t *Server) AbortEpoch(args *AbortEpochArgs, reply *AbortEpochReply) error {
	if !t.isLeader() {
		return errors.New("Only leader can abort epochs")
	}
	replyCh := make(chan error, 1)
	t.controlCh <- abortEpochCommand{epochID: args.EpochID, reply: replyCh}
	return <-replyCh
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
		func() {
			t.amPublishingMutex.Lock()
			defer t.amPublishingMutex.Unlock()
			resultPath, err = t.mergeFn()
		}()
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
	var firstErr error
	for i := 0; i < len(t.ServerAddrs); i++ {
		err := <-c
		if err != nil {
			failed = true
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("Error connecting to server: %v", err)
		}
	}

	if failed {
		return fmt.Errorf("connection failed: %w", firstErr)
	}

	return nil
}

func (t *Server) Initialize(*int, *int) error {
	if t.isLeader() {
		t.incoming1 = make(chan bool, READY_BUFFER_SIZE)
		t.incoming2 = make(chan bool, READY_BUFFER_SIZE)
		t.incoming3 = make(chan bool, READY_BUFFER_SIZE)
		if t.ingestionQueue == nil {
			t.SetIngestionQueueBackend(memoryIngestionQueueBackend)
		}
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
				t.setPeerConnectionState(PeerConnectionsFailed, err)
				log.Printf("Could not initialize peer connections: %v", err)
				return
			}
			t.setPeerConnectionState(PeerConnectionsReady, nil)
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
	t.SetIngestionQueueBackend(memoryIngestionQueueBackend)
	t.replicaID = CompletedUploadReplicaActive
	t.ingestionBatchSize = defaultIngestionReceiveBatchSize
	t.ingestionErrorBackoff = defaultIngestionWorkerErrorBackoff
	t.SetCompletedUploadLedger(newMemoryCompletedUploadLedger())
	t.completedUploadProcessingTTL = defaultCompletedUploadProcessingTTL
	t.processUploadFn = t.submitPrepares
	t.commitUploadFn = t.submitCommits
	if t.isLeader() {
		t.controlCh = make(chan leaderControlCommand, READY_BUFFER_SIZE)
		go t.runLeaderControl()
	}

	return t
}

func (t *Server) SetIngestionQueueBackend(backend string) error {
	switch backend {
	case "", memoryIngestionQueueBackend:
		t.ingestionQueue = newMemoryCompletedUploadQueue()
		t.ingestionQueueBackend = memoryIngestionQueueBackend
		return nil
	case sqsIngestionQueueBackend:
		return errors.New("sqs ingestion queue requires explicit queue configuration")
	default:
		return fmt.Errorf("unsupported ingestion queue backend %q", backend)
	}
}

func (t *Server) SetCompletedUploadQueue(queue CompletedUploadQueue) error {
	if queue == nil {
		return errors.New("completed upload queue is required")
	}
	t.ingestionQueue = queue
	t.ingestionQueueBackend = queue.Backend()
	return nil
}

func (t *Server) SetReplicaID(replicaID string) error {
	if err := validateCompletedUploadReplicaID(replicaID); err != nil {
		return err
	}
	if replicaID == "" {
		replicaID = CompletedUploadReplicaActive
	}
	t.replicaID = replicaID
	return nil
}

func (t *Server) SetStandbyIngestionFanoutConfigured(configured bool) {
	t.standbyIngestionFanout = configured
}

func (t *Server) SetCompletedUploadLedger(ledger CompletedUploadLedger) error {
	if ledger == nil {
		return errors.New("completed upload ledger is required")
	}
	t.completedUploadLedger = ledger
	t.completedUploadLedgerBackend = ledger.Backend()
	return nil
}

func (t *Server) SetIngestionWorkerConfig(receiveBatchSize int, errorBackoff time.Duration) error {
	if receiveBatchSize < 1 || receiveBatchSize > 10 {
		return errors.New("ingestion receive batch size must be between 1 and 10")
	}
	if errorBackoff <= 0 {
		return errors.New("ingestion worker error backoff must be positive")
	}
	t.ingestionBatchSize = receiveBatchSize
	t.ingestionErrorBackoff = errorBackoff
	return nil
}

func (t *Server) SetCompletedUploadProcessingTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("completed upload processing ttl must be positive")
	}
	t.completedUploadProcessingTTL = ttl
	return nil
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
