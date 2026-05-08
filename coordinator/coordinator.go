package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

type shardListType []string

func (s *shardListType) String() string {
	return strings.Join(*s, ";")
}

func (s *shardListType) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type ShardConfig struct {
	ID       int
	StartRow int
	EndRow   int
	Active   PairConfig
	Standby  *PairConfig
}

type PairConfig struct {
	LeaderAddr   string
	FollowerAddr string
}

type shardClient interface {
	Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error
	Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error
	Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error
	StartEpoch(args *db.StartEpochArgs, reply *db.StartEpochReply) error
	EpochStatus(args *db.EpochStatusArgs, reply *db.EpochStatusReply) error
	Status(args *db.StatusArgs, reply *db.StatusReply) error
	AbortEpoch(args *db.AbortEpochArgs, reply *db.AbortEpochReply) error
}

type statusClient interface {
	Status(args *db.StatusArgs, reply *db.StatusReply) error
}

type closeableStatusClient interface {
	statusClient
	Close() error
}

type rpcShardClient struct {
	client *rpc.Client
}

func (r *rpcShardClient) Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error {
	return r.client.Call("Server.Upload1", args, reply)
}

func (r *rpcShardClient) Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error {
	return r.client.Call("Server.Upload2", args, reply)
}

func (r *rpcShardClient) Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error {
	return r.client.Call("Server.Upload3", args, reply)
}

func (r *rpcShardClient) StartEpoch(args *db.StartEpochArgs, reply *db.StartEpochReply) error {
	return r.client.Call("Server.StartEpoch", args, reply)
}

func (r *rpcShardClient) EpochStatus(args *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	return r.client.Call("Server.EpochStatus", args, reply)
}

func (r *rpcShardClient) Status(args *db.StatusArgs, reply *db.StatusReply) error {
	return r.client.Call("Server.Status", args, reply)
}

func (r *rpcShardClient) AbortEpoch(args *db.AbortEpochArgs, reply *db.AbortEpochReply) error {
	return r.client.Call("Server.AbortEpoch", args, reply)
}

func (r *rpcShardClient) Close() error {
	return r.client.Close()
}

const (
	defaultShardStatusTimeout            = 2 * time.Second
	defaultShardHealthInterval           = 5 * time.Second
	defaultCoordinatorLeaseHolder        = "standalone"
	defaultCoordinatorLeaseTTL           = 30 * time.Second
	defaultCoordinatorLeaseRenewInterval = 10 * time.Second
	coordinatorRoleActive                = "active"
	coordinatorRolePassive               = "passive"
	coordinatorRoleStale                 = "stale"
)

type Coordinator struct {
	mu                  sync.Mutex
	actor               *coordinatorActor
	closed              bool
	shards              []ShardConfig
	shardByID           map[int]ShardConfig
	clients             map[int]shardClient
	sessions            map[int64]SessionRecord
	epoch               db.EpochMeta
	epochTimer          *time.Timer
	controlStore        ControlStore
	sessionStore        SessionStore
	sessionStoreBackend string
	leaseHolder         string
	leaseTTL            time.Duration
	lease               CoordinatorLease
	role                string
	leaseStopCh         chan struct{}
	leaseDoneCh         chan struct{}

	health              map[int]shardHealthSnapshot
	healthInterval      time.Duration
	healthTimeout       time.Duration
	healthStopCh        chan struct{}
	healthDoneCh        chan struct{}
	standbyLeaderDialer func(addr string, timeout time.Duration) (closeableStatusClient, error)

	scalingConfig             ScalingPolicyConfig
	activeScalingMetrics      EpochScalingMetrics
	hasActiveScalingMetrics   bool
	lastScalingMetrics        EpochScalingMetrics
	lastScalingRecommendation ScalingRecommendation
	hasLastScalingMetrics     bool
}

func parseShardConfig(value string) (ShardConfig, error) {
	parts := strings.SplitN(value, ",", 6)
	if len(parts) != 5 && len(parts) != 6 {
		return ShardConfig{}, fmt.Errorf("invalid shard config %q", value)
	}

	id, err := parsePositiveInt(parts[0], true)
	if err != nil {
		return ShardConfig{}, err
	}
	start, err := parsePositiveInt(parts[1], true)
	if err != nil {
		return ShardConfig{}, err
	}
	end, err := parsePositiveInt(parts[2], false)
	if err != nil {
		return ShardConfig{}, err
	}
	activePair, err := parsePairConfig(parts[3], parts[4])
	if err != nil {
		return ShardConfig{}, fmt.Errorf("shard %d active pair: %w", id, err)
	}

	var standbyPair *PairConfig
	if len(parts) == 6 {
		standby, err := parseOptionalPairConfig(parts[5])
		if err != nil {
			return ShardConfig{}, fmt.Errorf("shard %d standby pair: %w", id, err)
		}
		standbyPair = standby
	}

	return ShardConfig{
		ID:       id,
		StartRow: start,
		EndRow:   end,
		Active:   activePair,
		Standby:  standbyPair,
	}, nil
}

func parsePairConfig(leaderAddr, followerAddr string) (PairConfig, error) {
	leaderAddr = strings.TrimSpace(leaderAddr)
	followerAddr = strings.TrimSpace(followerAddr)
	if leaderAddr == "" || followerAddr == "" {
		return PairConfig{}, errors.New("must specify both leader and follower addresses")
	}
	return PairConfig{
		LeaderAddr:   leaderAddr,
		FollowerAddr: followerAddr,
	}, nil
}

func parseOptionalPairConfig(value string) (*PairConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return nil, nil
	}
	parts := strings.SplitN(value, "|", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid pair %q", value)
	}
	pair, err := parsePairConfig(parts[0], parts[1])
	if err != nil {
		return nil, err
	}
	return &pair, nil
}

func parsePositiveInt(raw string, allowZero bool) (int, error) {
	value := 0
	_, err := fmt.Sscanf(raw, "%d", &value)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q", raw)
	}
	if value < 0 || (!allowZero && value == 0) {
		return 0, fmt.Errorf("invalid non-negative integer %q", raw)
	}
	return value, nil
}

func validateShardMap(shards []ShardConfig) ([]ShardConfig, error) {
	if len(shards) == 0 {
		return nil, errors.New("must configure at least one shard")
	}

	sorted := append([]ShardConfig(nil), shards...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].StartRow == sorted[j].StartRow {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].StartRow < sorted[j].StartRow
	})

	expectedStart := 0
	seenIDs := make(map[int]bool)
	for _, shard := range sorted {
		if seenIDs[shard.ID] {
			return nil, fmt.Errorf("duplicate shard id %d", shard.ID)
		}
		seenIDs[shard.ID] = true
		if shard.StartRow < 0 {
			return nil, fmt.Errorf("shard %d has negative start row %d", shard.ID, shard.StartRow)
		}
		if shard.EndRow-shard.StartRow != db.TABLE_HEIGHT {
			return nil, fmt.Errorf("shard %d range [%d,%d) must have height %d", shard.ID, shard.StartRow, shard.EndRow, db.TABLE_HEIGHT)
		}
		if shard.Active.LeaderAddr == "" || shard.Active.FollowerAddr == "" {
			return nil, fmt.Errorf("shard %d missing active pair addresses", shard.ID)
		}
		if shard.StartRow != expectedStart {
			if shard.StartRow < expectedStart {
				return nil, fmt.Errorf("shard %d overlaps previous range at row %d", shard.ID, shard.StartRow)
			}
			return nil, fmt.Errorf("shard map has gap before row %d", shard.StartRow)
		}
		expectedStart = shard.EndRow
	}
	if expectedStart != len(sorted)*db.TABLE_HEIGHT {
		return nil, fmt.Errorf("shard map ends at row %d, want %d", expectedStart, len(sorted)*db.TABLE_HEIGHT)
	}

	return sorted, nil
}

func NewCoordinator(shards []ShardConfig, clients map[int]shardClient) (*Coordinator, error) {
	return newCoordinatorWithStores(shards, clients, newMemoryControlStore(1), newMemorySessionStore(), "memory")
}

func newCoordinatorWithControlStore(shards []ShardConfig, clients map[int]shardClient, controlStore ControlStore) (*Coordinator, error) {
	return newCoordinatorWithStores(shards, clients, controlStore, newMemorySessionStore(), "memory")
}

func newCoordinatorWithStores(shards []ShardConfig, clients map[int]shardClient, controlStore ControlStore, sessionStore SessionStore, sessionStoreBackend string) (*Coordinator, error) {
	return newCoordinatorWithLeaseConfig(
		shards,
		clients,
		controlStore,
		sessionStore,
		sessionStoreBackend,
		defaultCoordinatorLeaseHolder,
		defaultCoordinatorLeaseTTL,
		defaultCoordinatorLeaseRenewInterval,
	)
}

func newCoordinatorWithLeaseConfig(
	shards []ShardConfig,
	clients map[int]shardClient,
	controlStore ControlStore,
	sessionStore SessionStore,
	sessionStoreBackend string,
	leaseHolder string,
	leaseTTL time.Duration,
	leaseRenewInterval time.Duration,
) (*Coordinator, error) {
	return newCoordinatorWithStandbyConfig(shards, clients, controlStore, sessionStore, sessionStoreBackend, leaseHolder, leaseTTL, leaseRenewInterval, false)
}

func newCoordinatorWithStandbyConfig(
	shards []ShardConfig,
	clients map[int]shardClient,
	controlStore ControlStore,
	sessionStore SessionStore,
	sessionStoreBackend string,
	leaseHolder string,
	leaseTTL time.Duration,
	leaseRenewInterval time.Duration,
	standby bool,
) (*Coordinator, error) {
	if controlStore == nil {
		return nil, errors.New("control store is required")
	}
	if sessionStore == nil {
		return nil, errors.New("session store is required")
	}
	if sessionStoreBackend == "" {
		sessionStoreBackend = "unknown"
	}
	if err := validateCoordinatorLeaseConfig(leaseHolder, leaseTTL, leaseRenewInterval); err != nil {
		return nil, err
	}
	validated, err := validateShardMap(shards)
	if err != nil {
		return nil, err
	}
	lease, err := controlStore.AcquireLease(time.Now().UTC(), leaseHolder, leaseTTL)
	if err != nil {
		if !standby || !errors.Is(err, errLeaseHeld) {
			return nil, fmt.Errorf("acquire coordinator lease: %w", err)
		}
	}
	role := coordinatorRoleActive
	if err != nil {
		role = coordinatorRolePassive
	}
	coord := &Coordinator{
		actor:               newCoordinatorActor(),
		shards:              validated,
		shardByID:           make(map[int]ShardConfig, len(validated)),
		clients:             make(map[int]shardClient, len(validated)),
		sessions:            make(map[int64]SessionRecord),
		controlStore:        controlStore,
		sessionStore:        sessionStore,
		sessionStoreBackend: sessionStoreBackend,
		leaseHolder:         leaseHolder,
		leaseTTL:            leaseTTL,
		lease:               lease,
		role:                role,
		leaseStopCh:         make(chan struct{}),
		leaseDoneCh:         make(chan struct{}),
		health:              make(map[int]shardHealthSnapshot, len(validated)),
		healthInterval:      defaultShardHealthInterval,
		healthTimeout:       defaultShardStatusTimeout,
		healthStopCh:        make(chan struct{}),
		healthDoneCh:        make(chan struct{}),
		standbyLeaderDialer: dialStandbyShardLeader,
		scalingConfig:       defaultScalingPolicyConfig(len(validated)),
	}
	for _, shard := range validated {
		coord.shardByID[shard.ID] = shard
		if client, ok := clients[shard.ID]; ok {
			coord.clients[shard.ID] = client
		}
	}
	coord.startLeaseRenewal(leaseRenewInterval)
	coord.startShardHealthMonitor()
	return coord, nil
}

func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	actor := c.actor
	stopCh := c.leaseStopCh
	doneCh := c.leaseDoneCh
	healthStopCh := c.healthStopCh
	healthDoneCh := c.healthDoneCh
	c.leaseStopCh = nil
	c.leaseDoneCh = nil
	c.healthStopCh = nil
	c.healthDoneCh = nil
	c.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}
	if healthStopCh != nil {
		close(healthStopCh)
		<-healthDoneCh
	}
	if actor != nil {
		actor.call(func() {
			if c.epochTimer != nil {
				c.epochTimer.Stop()
				c.epochTimer = nil
			}
		})
		c.mu.Lock()
		if c.actor == actor {
			c.actor = nil
		}
		c.mu.Unlock()
		actor.stop()
	}
}

func dialShardLeader(leaderAddr string) (shardClient, error) {
	client, err := utils.DialHTTPWithTLS("tcp", leaderAddr, -1, []tls.Certificate{utils.LeaderCertificate})
	if err != nil {
		return nil, err
	}
	return &rpcShardClient{client: client}, nil
}

func dialStandbyShardLeader(leaderAddr string, timeout time.Duration) (closeableStatusClient, error) {
	client, err := utils.DialHTTPWithTLSWithTimeout("tcp", leaderAddr, -1, []tls.Certificate{utils.LeaderCertificate}, timeout)
	if err != nil {
		return nil, err
	}
	return &rpcShardClient{client: client}, nil
}

func (c *Coordinator) connectShards() error {
	for _, shard := range c.shards {
		if _, ok := c.clients[shard.ID]; ok {
			continue
		}
		client, err := dialShardLeader(shard.Active.LeaderAddr)
		if err != nil {
			return fmt.Errorf("connect shard %d leader %s: %w", shard.ID, shard.Active.LeaderAddr, err)
		}
		c.clients[shard.ID] = client
	}
	return nil
}

func (c *Coordinator) routeShard(row int) (ShardConfig, error) {
	for _, shard := range c.shards {
		if row >= shard.StartRow && row < shard.EndRow {
			return shard, nil
		}
	}
	return ShardConfig{}, fmt.Errorf("row %d outside shard map", row)
}

func globalTableHeightForShards(shards []ShardConfig) int {
	return len(shards) * db.TABLE_HEIGHT
}

func (c *Coordinator) cachedOrStoredSession(globalUUID int64) (SessionRecord, error) {
	session, ok := c.cachedSession(globalUUID)
	if ok {
		return session, nil
	}
	session, err := c.sessionStore.GetSession(context.Background(), globalUUID)
	if err != nil {
		return SessionRecord{}, err
	}
	c.cacheSession(session)
	return session, nil
}

func acceptingEpochFromControlStore(controlStore ControlStore) (db.EpochMeta, error) {
	epoch, ok := controlStore.CurrentEpoch()
	if !ok || epoch.State != db.EpochStateActive {
		return db.EpochMeta{}, errCoordinatorNoActiveEpoch
	}
	accepting, err := controlStore.Accepting(epoch.ID)
	if err != nil || !accepting {
		return db.EpochMeta{}, errCoordinatorNoActiveEpoch
	}
	return epoch, nil
}

func (c *Coordinator) Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error {
	decision := c.upload1Decision()
	if decision.err != nil {
		return coordinatorWireError(decision.err)
	}
	epoch, err := acceptingEpochFromControlStore(decision.controlStore)
	if err != nil {
		return coordinatorWireError(err)
	}

	shard, err := c.routeShard(args.RouteRow)
	if err != nil {
		return err
	}
	client := c.clients[shard.ID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", shard.ID)
	}

	localArgs := *args
	localArgs.RouteRow = args.RouteRow - shard.StartRow

	var session SessionRecord
	persisted := false
	for attempt := 0; attempt < 16; attempt++ {
		globalUUID, err := utils.RandomInt64(math.MaxInt64)
		if err != nil {
			return err
		}
		if globalUUID <= 0 {
			continue
		}

		var hashKey [32]byte
		utils.RandBytes(hashKey[:])
		session = SessionRecord{
			EpochID:       epoch.ID,
			ShardID:       shard.ID,
			GlobalUUID:    globalUUID,
			LocalUUID:     globalUUID,
			HashKey:       hashKey,
			GlobalRow:     args.RouteRow,
			LocalRow:      localArgs.RouteRow,
			ShardStartRow: shard.StartRow,
			CreatedAt:     time.Now().UTC(),
		}
		err = c.sessionStore.PutSession(context.Background(), session)
		if errors.Is(err, errSessionExists) {
			continue
		}
		if err != nil {
			return err
		}
		persisted = true
		break
	}
	if !persisted {
		return coordinatorWireError(errCoordinatorSessionAllocation)
	}

	localArgs.UseAssignedSession = true
	localArgs.AssignedUUID = session.LocalUUID
	localArgs.AssignedHashKey = session.HashKey

	var shardReply db.UploadReply1
	if err := client.Upload1(&localArgs, &shardReply); err != nil {
		if deleteErr := c.sessionStore.DeleteSession(context.Background(), session.GlobalUUID); deleteErr != nil && !errors.Is(deleteErr, errSessionMissing) {
			log.Printf("Delete persisted session %d after shard Upload1 failure failed: %v", session.GlobalUUID, deleteErr)
		}
		return err
	}
	if shardReply.Uuid != session.LocalUUID || shardReply.HashKey != session.HashKey {
		if deleteErr := c.sessionStore.DeleteSession(context.Background(), session.GlobalUUID); deleteErr != nil && !errors.Is(deleteErr, errSessionMissing) {
			log.Printf("Delete persisted session %d after mismatched shard Upload1 reply failed: %v", session.GlobalUUID, deleteErr)
		}
		return coordinatorWireError(errCoordinatorAssignedSessionFailed)
	}

	c.cacheSession(session)
	reply.Uuid = session.GlobalUUID
	reply.HashKey = session.HashKey
	return nil
}

func (c *Coordinator) Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error {
	session, err := c.cachedOrStoredSession(args.Uuid)
	if err != nil || session.HashKey != args.HashKey {
		return coordinatorWireError(errCoordinatorBogusUUID)
	}
	epoch, err := acceptingEpochFromControlStore(c.controlStore)
	if err != nil || epoch.ID != session.EpochID {
		return coordinatorWireError(errCoordinatorNoActiveEpoch)
	}

	client := c.clients[session.ShardID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", session.ShardID)
	}
	localArgs := *args
	localArgs.Uuid = session.LocalUUID
	return client.Upload2(&localArgs, reply)
}

func (c *Coordinator) Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error {
	session, err := c.cachedOrStoredSession(args.Uuid)
	if err != nil || session.HashKey != args.HashKey {
		return coordinatorWireError(errCoordinatorBogusUUID)
	}
	epoch, err := acceptingEpochFromControlStore(c.controlStore)
	if err != nil || epoch.ID != session.EpochID {
		return coordinatorWireError(errCoordinatorNoActiveEpoch)
	}

	client := c.clients[session.ShardID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", session.ShardID)
	}
	localArgs := *args
	localArgs.Uuid = session.LocalUUID
	err = client.Upload3(&localArgs, reply)
	if err != nil {
		return err
	}

	if err := c.sessionStore.DeleteSession(context.Background(), session.GlobalUUID); err != nil && !errors.Is(err, errSessionMissing) {
		return err
	}
	c.recordUpload3Success(session)
	return nil
}

func sameEpochReply(expected db.StartEpochReply, actual db.StartEpochReply) bool {
	return expected.EpochID == actual.EpochID &&
		expected.State == actual.State &&
		expected.StartUnix == actual.StartUnix &&
		expected.EndUnix == actual.EndUnix &&
		expected.DurationSecs == actual.DurationSecs
}

func (c *Coordinator) StartEpoch(args *db.StartEpochArgs, reply *db.StartEpochReply) error {
	if args.DurationSeconds <= 0 {
		return coordinatorWireError(errCoordinatorInvalidEpochDuration)
	}
	if err := c.requireCoordinatorLease(); err != nil {
		return coordinatorWireError(errCoordinatorNotActive)
	}

	latestEpochID := int64(0)
	if currentEpoch, ok := c.controlStore.CurrentEpoch(); ok {
		latestEpochID = currentEpoch.ID
	}
	decision := c.startEpochDecision(args, latestEpochID)
	if decision.err != nil {
		return coordinatorWireError(decision.err)
	}

	startArgs := db.StartEpochArgs{
		DurationSeconds: args.DurationSeconds,
		EpochID:         decision.nextEpochID,
		StartUnix:       decision.startTime.Unix(),
	}

	started := make([]int, 0, len(c.shards))
	var expected db.StartEpochReply
	for i, shard := range c.shards {
		client := c.clients[shard.ID]
		if client == nil {
			c.abortStarted(started, decision.nextEpochID)
			return fmt.Errorf("missing shard client for shard %d", shard.ID)
		}
		var shardReply db.StartEpochReply
		if err := client.StartEpoch(&startArgs, &shardReply); err != nil {
			c.abortStarted(started, decision.nextEpochID)
			return fmt.Errorf("start epoch on shard %d: %w", shard.ID, err)
		}
		if i == 0 {
			expected = shardReply
		} else if !sameEpochReply(expected, shardReply) {
			c.abortStarted(started, decision.nextEpochID)
			return fmt.Errorf("shard %d returned mismatched epoch metadata", shard.ID)
		}
		started = append(started, shard.ID)
	}

	if err := c.requireCoordinatorLease(); err != nil {
		c.abortStarted(started, decision.nextEpochID)
		return coordinatorWireError(errCoordinatorNotActive)
	}

	epoch := db.EpochMeta{
		ID:              expected.EpochID,
		State:           db.EpochStateActive,
		StartTime:       time.Unix(expected.StartUnix, 0).UTC(),
		EndTime:         time.Unix(expected.EndUnix, 0).UTC(),
		DurationSeconds: expected.DurationSecs,
	}
	controlStore := c.controlStore
	shardConfigVersion := controlStore.ShardConfigVersion()

	if err := controlStore.StartEpoch(epoch, shardConfigVersion); err != nil {
		c.abortStarted(started, decision.nextEpochID)
		return fmt.Errorf("start epoch in control store: %w", err)
	}

	c.commitStartedEpoch(epoch)

	*reply = expected
	return nil
}

func (c *Coordinator) abortStarted(started []int, epochID int64) {
	for _, shardID := range started {
		client := c.clients[shardID]
		if client == nil {
			continue
		}
		var reply db.AbortEpochReply
		if err := client.AbortEpoch(&db.AbortEpochArgs{EpochID: epochID}, &reply); err != nil {
			log.Printf("Abort epoch %d on shard %d failed: %v", epochID, shardID, err)
		}
	}
}

func (c *Coordinator) completeEpoch() {
	decision := c.completeEpochDecision()
	if decision.epochID == 0 {
		return
	}

	if err := c.requireCoordinatorLease(); err != nil {
		log.Printf("Coordinator lease unavailable; skipping epoch %d completion: %v", decision.epochID, err)
		return
	}

	controlStore := c.controlStore
	if !c.commitCompletedEpoch(decision.epochID) {
		return
	}

	if _, err := controlStore.CompleteEpoch(decision.epochID); err != nil {
		log.Printf("Complete epoch %d in control store failed: %v", decision.epochID, err)
	}
}

func (c *Coordinator) EpochStatus(_ *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	var epoch db.EpochMeta
	c.actorCall(func() {
		epoch = c.epoch
	})
	controlStore := c.controlStore

	reply.EpochID = epoch.ID
	reply.State = epoch.State.String()
	reply.StartUnix = epoch.StartTime.Unix()
	reply.EndUnix = epoch.EndTime.Unix()
	reply.DurationSecs = epoch.DurationSeconds
	reply.Accepting = acceptingFromControlStore(controlStore, epoch)
	return nil
}

func acceptingFromControlStore(controlStore ControlStore, epoch db.EpochMeta) bool {
	if epoch.ID <= 0 {
		return epoch.State == db.EpochStateActive
	}
	if _, ok := controlStore.CurrentEpoch(); !ok {
		return epoch.State == db.EpochStateActive
	}
	accepting, err := controlStore.Accepting(epoch.ID)
	if err != nil {
		return false
	}
	return accepting
}

func populateCoordinatorLeaseStatus(reply *db.CoordinatorStatusReply, lease CoordinatorLease, now time.Time) {
	reply.LeaseHolder = lease.Holder
	reply.LeaseFencingToken = lease.FencingToken
	if !lease.ExpiresAt.IsZero() {
		reply.LeaseExpiresUnixMs = lease.ExpiresAt.UnixMilli()
	}
	reply.LeaseActive = lease.Holder != "" && now.Before(lease.ExpiresAt)
}

func activeHolderFromControlStore(controlStore ControlStore, now time.Time) string {
	lease, ok := controlStore.CurrentLease(now)
	if !ok {
		return ""
	}
	return lease.Holder
}

func (c *Coordinator) Status(args *db.CoordinatorStatusArgs, reply *db.CoordinatorStatusReply) error {
	timeout := defaultShardStatusTimeout
	if args != nil && args.ShardTimeoutMs > 0 {
		timeout = time.Duration(args.ShardTimeoutMs) * time.Millisecond
	}
	if c.needsShardHealthRefresh() {
		c.refreshShardHealth(timeout)
	}

	controlStore := c.controlStore
	sessionStoreBackend := c.sessionStoreBackend
	shards := append([]ShardConfig(nil), c.shards...)
	snapshot := c.coordinatorStatusSnapshot()

	reply.Healthy = true
	reply.Role = snapshot.role
	reply.LeaderAddr = ""
	reply.SessionStoreBackend = sessionStoreBackend
	reply.GlobalTableHeight = globalTableHeightForShards(shards)
	reply.EpochID = snapshot.epoch.ID
	reply.State = snapshot.epoch.State.String()
	reply.StartUnix = snapshot.epoch.StartTime.Unix()
	reply.EndUnix = snapshot.epoch.EndTime.Unix()
	reply.DurationSecs = snapshot.epoch.DurationSeconds
	reply.Accepting = acceptingFromControlStore(controlStore, snapshot.epoch)
	now := time.Now().UTC()
	populateCoordinatorLeaseStatus(reply, snapshot.lease, now)
	reply.ActiveHolder = activeHolderFromControlStore(controlStore, now)
	reply.CurrentShardCount = snapshot.scaling.CurrentShardCount
	reply.RecommendedNextShardCount = snapshot.scaling.RecommendedShardCount
	reply.TargetRowsPerShard = snapshot.scaling.TargetRowsPerShard
	reply.ScalingAction = snapshot.scaling.Action
	reply.ScalingReason = snapshot.scaling.Reason
	reply.RequestDensity = snapshot.scaling.RequestDensity
	reply.ScalingEpochID = snapshot.scalingMetrics.EpochID
	reply.ScalingAcceptedRequests = snapshot.scalingMetrics.AcceptedRequestCount
	reply.ScalingDurationSecs = snapshot.scalingMetrics.DurationSeconds
	reply.Shards = make([]db.CoordinatorShardStatus, len(shards))

	for i, shard := range shards {
		entry := db.CoordinatorShardStatus{
			ID:                 shard.ID,
			StartRow:           shard.StartRow,
			EndRow:             shard.EndRow,
			ActiveLeaderAddr:   shard.Active.LeaderAddr,
			ActiveFollowerAddr: shard.Active.FollowerAddr,
		}
		if shard.Standby != nil {
			entry.HasStandby = true
			entry.StandbyLeaderAddr = shard.Standby.LeaderAddr
			entry.StandbyFollowerAddr = shard.Standby.FollowerAddr
		}

		shardHealth := snapshot.health[shard.ID]
		entry.ActiveReachable = shardHealth.Active.Reachable
		entry.ActiveStatus = shardHealth.Active.Status
		entry.ActiveStatusError = shardHealth.Active.Error
		entry.ActiveLastChecked = lastCheckedUnix(shardHealth.Active.CheckedAt)
		entry.StandbyReachable = shardHealth.Standby.Reachable
		entry.StandbyStatus = shardHealth.Standby.Status
		entry.StandbyStatusError = shardHealth.Standby.Error
		entry.StandbyLastChecked = lastCheckedUnix(shardHealth.Standby.CheckedAt)
		entry.Reachable = entry.ActiveReachable
		entry.Status = entry.ActiveStatus
		entry.StatusError = entry.ActiveStatusError
		reply.Shards[i] = entry
	}

	return nil
}
