package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"maps"
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

type routedSession struct {
	shardID   int
	localUUID int64
	hashKey   [32]byte
}

type pairHealthSnapshot struct {
	Reachable bool
	Status    db.StatusReply
	Error     string
	CheckedAt time.Time
}

type shardHealthSnapshot struct {
	Active  pairHealthSnapshot
	Standby pairHealthSnapshot
}

const (
	defaultShardStatusTimeout            = 2 * time.Second
	defaultShardHealthInterval           = 5 * time.Second
	defaultCoordinatorLeaseHolder        = "standalone"
	defaultCoordinatorLeaseTTL           = 30 * time.Second
	defaultCoordinatorLeaseRenewInterval = 10 * time.Second
)

type Coordinator struct {
	mu           sync.Mutex
	shards       []ShardConfig
	shardByID    map[int]ShardConfig
	clients      map[int]shardClient
	sessions     map[int64]routedSession
	nextUUID     int64
	epoch        db.EpochMeta
	epochTimer   *time.Timer
	controlStore ControlStore
	leaseHolder  string
	leaseTTL     time.Duration
	lease        CoordinatorLease
	leaseStopCh  chan struct{}
	leaseDoneCh  chan struct{}

	health              map[int]shardHealthSnapshot
	healthInterval      time.Duration
	healthTimeout       time.Duration
	healthStopCh        chan struct{}
	healthDoneCh        chan struct{}
	standbyLeaderDialer func(addr string, timeout time.Duration) (closeableStatusClient, error)
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
		if shard.StartRow < 0 || shard.EndRow > db.TABLE_HEIGHT {
			return nil, fmt.Errorf("shard %d range [%d,%d) outside [0,%d)", shard.ID, shard.StartRow, shard.EndRow, db.TABLE_HEIGHT)
		}
		if shard.EndRow <= shard.StartRow {
			return nil, fmt.Errorf("shard %d has empty or inverted range [%d,%d)", shard.ID, shard.StartRow, shard.EndRow)
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
	if expectedStart != db.TABLE_HEIGHT {
		return nil, fmt.Errorf("shard map ends at row %d, want %d", expectedStart, db.TABLE_HEIGHT)
	}

	return sorted, nil
}

func NewCoordinator(shards []ShardConfig, clients map[int]shardClient) (*Coordinator, error) {
	return newCoordinatorWithControlStore(shards, clients, newMemoryControlStore(1))
}

func newCoordinatorWithControlStore(shards []ShardConfig, clients map[int]shardClient, controlStore ControlStore) (*Coordinator, error) {
	return newCoordinatorWithLeaseConfig(
		shards,
		clients,
		controlStore,
		defaultCoordinatorLeaseHolder,
		defaultCoordinatorLeaseTTL,
		defaultCoordinatorLeaseRenewInterval,
	)
}

func newCoordinatorWithLeaseConfig(
	shards []ShardConfig,
	clients map[int]shardClient,
	controlStore ControlStore,
	leaseHolder string,
	leaseTTL time.Duration,
	leaseRenewInterval time.Duration,
) (*Coordinator, error) {
	if controlStore == nil {
		return nil, errors.New("control store is required")
	}
	if leaseHolder == "" {
		return nil, errors.New("coordinator lease holder is required")
	}
	if leaseTTL <= 0 {
		return nil, errors.New("coordinator lease ttl must be positive")
	}
	if leaseRenewInterval <= 0 {
		return nil, errors.New("coordinator lease renew interval must be positive")
	}
	validated, err := validateShardMap(shards)
	if err != nil {
		return nil, err
	}
	lease, err := controlStore.AcquireLease(time.Now().UTC(), leaseHolder, leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("acquire coordinator lease: %w", err)
	}
	coord := &Coordinator{
		shards:              validated,
		shardByID:           make(map[int]ShardConfig, len(validated)),
		clients:             make(map[int]shardClient, len(validated)),
		sessions:            make(map[int64]routedSession),
		controlStore:        controlStore,
		leaseHolder:         leaseHolder,
		leaseTTL:            leaseTTL,
		lease:               lease,
		leaseStopCh:         make(chan struct{}),
		leaseDoneCh:         make(chan struct{}),
		health:              make(map[int]shardHealthSnapshot, len(validated)),
		healthInterval:      defaultShardHealthInterval,
		healthTimeout:       defaultShardStatusTimeout,
		healthStopCh:        make(chan struct{}),
		healthDoneCh:        make(chan struct{}),
		standbyLeaderDialer: dialStandbyShardLeader,
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

func (c *Coordinator) startLeaseRenewal(interval time.Duration) {
	stopCh := c.leaseStopCh
	doneCh := c.leaseDoneCh
	ticker := time.NewTicker(interval)
	go func() {
		defer close(doneCh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.renewCoordinatorLease(); err != nil {
					log.Printf("Renew coordinator lease failed: %v", err)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

func (c *Coordinator) startShardHealthMonitor() {
	stopCh := c.healthStopCh
	doneCh := c.healthDoneCh
	interval := c.healthInterval
	ticker := time.NewTicker(interval)
	go func() {
		defer close(doneCh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.refreshShardHealth(c.healthTimeout)
			case <-stopCh:
				return
			}
		}
	}()
}

func (c *Coordinator) Close() {
	c.mu.Lock()
	stopCh := c.leaseStopCh
	doneCh := c.leaseDoneCh
	healthStopCh := c.healthStopCh
	healthDoneCh := c.healthDoneCh
	c.leaseStopCh = nil
	c.leaseDoneCh = nil
	c.healthStopCh = nil
	c.healthDoneCh = nil
	if c.epochTimer != nil {
		c.epochTimer.Stop()
		c.epochTimer = nil
	}
	c.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}
	if healthStopCh != nil {
		close(healthStopCh)
		<-healthDoneCh
	}
}

func (c *Coordinator) renewCoordinatorLease() error {
	c.mu.Lock()
	controlStore := c.controlStore
	holder := c.leaseHolder
	token := c.lease.FencingToken
	ttl := c.leaseTTL
	c.mu.Unlock()

	lease, err := controlStore.RenewLease(time.Now().UTC(), holder, token, ttl)
	if err != nil {
		return err
	}

	c.mu.Lock()
	if c.lease.FencingToken == token && c.leaseHolder == holder {
		c.lease = lease
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) requireCoordinatorLease() error {
	if err := c.renewCoordinatorLease(); err != nil {
		return fmt.Errorf("coordinator lease unavailable: %w", err)
	}
	return nil
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

func (c *Coordinator) nextGlobalUUIDLocked() (int64, error) {
	if c.nextUUID == math.MaxInt64 {
		return 0, errors.New("coordinator session space exhausted")
	}
	c.nextUUID++
	return c.nextUUID, nil
}

func (c *Coordinator) Upload1(args *db.UploadArgs1, reply *db.UploadReply1) error {
	c.mu.Lock()
	epoch := c.epoch
	active := epoch.State == db.EpochStateActive
	controlStore := c.controlStore
	c.mu.Unlock()
	if !active {
		return errors.New("No active epoch")
	}
	if err := c.requireCoordinatorLease(); err != nil {
		return errors.New("No active epoch")
	}
	accepting, err := controlStore.Accepting(epoch.ID)
	if err != nil || !accepting {
		return errors.New("No active epoch")
	}

	shard, err := c.routeShard(args.RouteRow)
	if err != nil {
		return err
	}
	client := c.clients[shard.ID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", shard.ID)
	}

	var shardReply db.UploadReply1
	if err := client.Upload1(args, &shardReply); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	globalUUID, err := c.nextGlobalUUIDLocked()
	if err != nil {
		return err
	}
	c.sessions[globalUUID] = routedSession{
		shardID:   shard.ID,
		localUUID: shardReply.Uuid,
		hashKey:   shardReply.HashKey,
	}
	reply.Uuid = globalUUID
	reply.HashKey = shardReply.HashKey
	return nil
}

func (c *Coordinator) Upload2(args *db.UploadArgs2, reply *db.UploadReply2) error {
	c.mu.Lock()
	session, ok := c.sessions[args.Uuid]
	c.mu.Unlock()
	if !ok || session.hashKey != args.HashKey {
		return errors.New("Bogus UUID")
	}

	client := c.clients[session.shardID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", session.shardID)
	}
	localArgs := *args
	localArgs.Uuid = session.localUUID
	return client.Upload2(&localArgs, reply)
}

func (c *Coordinator) Upload3(args *db.UploadArgs3, reply *db.UploadReply3) error {
	c.mu.Lock()
	session, ok := c.sessions[args.Uuid]
	c.mu.Unlock()
	if !ok || session.hashKey != args.HashKey {
		return errors.New("Bogus UUID")
	}

	client := c.clients[session.shardID]
	if client == nil {
		return fmt.Errorf("missing shard client for shard %d", session.shardID)
	}
	localArgs := *args
	localArgs.Uuid = session.localUUID
	err := client.Upload3(&localArgs, reply)
	if err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.sessions, args.Uuid)
	c.mu.Unlock()
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
		return errors.New("Epoch duration must be positive")
	}
	if err := c.requireCoordinatorLease(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.epoch.State == db.EpochStateActive {
		c.mu.Unlock()
		return errors.New("An epoch is already in progress")
	}
	nextEpochID := c.epoch.ID + 1
	if args.EpochID > 0 {
		nextEpochID = args.EpochID
	}
	startTime := time.Now().UTC()
	if args.StartUnix > 0 {
		startTime = time.Unix(args.StartUnix, 0).UTC()
	}
	c.mu.Unlock()

	startArgs := db.StartEpochArgs{
		DurationSeconds: args.DurationSeconds,
		EpochID:         nextEpochID,
		StartUnix:       startTime.Unix(),
	}

	started := make([]int, 0, len(c.shards))
	var expected db.StartEpochReply
	for i, shard := range c.shards {
		client := c.clients[shard.ID]
		if client == nil {
			c.abortStarted(started, nextEpochID)
			return fmt.Errorf("missing shard client for shard %d", shard.ID)
		}
		var shardReply db.StartEpochReply
		if err := client.StartEpoch(&startArgs, &shardReply); err != nil {
			c.abortStarted(started, nextEpochID)
			return fmt.Errorf("start epoch on shard %d: %w", shard.ID, err)
		}
		if i == 0 {
			expected = shardReply
		} else if !sameEpochReply(expected, shardReply) {
			c.abortStarted(started, nextEpochID)
			return fmt.Errorf("shard %d returned mismatched epoch metadata", shard.ID)
		}
		started = append(started, shard.ID)
	}

	if err := c.requireCoordinatorLease(); err != nil {
		c.abortStarted(started, nextEpochID)
		return err
	}

	c.mu.Lock()
	if c.epochTimer != nil {
		c.epochTimer.Stop()
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
	c.mu.Unlock()

	if err := controlStore.StartEpoch(epoch, shardConfigVersion); err != nil {
		c.abortStarted(started, nextEpochID)
		return fmt.Errorf("start epoch in control store: %w", err)
	}

	c.mu.Lock()
	c.epoch = epoch
	c.epochTimer = time.AfterFunc(time.Until(c.epoch.EndTime), func() {
		c.completeEpoch()
	})
	c.mu.Unlock()

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
	c.mu.Lock()
	if c.epoch.State != db.EpochStateActive {
		c.mu.Unlock()
		return
	}
	epochID := c.epoch.ID
	c.mu.Unlock()

	if err := c.requireCoordinatorLease(); err != nil {
		log.Printf("Coordinator lease unavailable; skipping epoch %d completion: %v", epochID, err)
		return
	}

	c.mu.Lock()
	if c.epoch.State != db.EpochStateActive || c.epoch.ID != epochID {
		c.mu.Unlock()
		return
	}
	c.epoch.State = db.EpochStateCompleted
	c.epochTimer = nil
	controlStore := c.controlStore
	c.mu.Unlock()

	if _, err := controlStore.CompleteEpoch(epochID); err != nil {
		log.Printf("Complete epoch %d in control store failed: %v", epochID, err)
	}
}

func (c *Coordinator) EpochStatus(_ *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	c.mu.Lock()
	epoch := c.epoch
	controlStore := c.controlStore
	c.mu.Unlock()

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

func shardStatusWithTimeout(client statusClient, timeout time.Duration) (db.StatusReply, error) {
	type statusResult struct {
		reply db.StatusReply
		err   error
	}
	resultCh := make(chan statusResult, 1)
	go func() {
		var reply db.StatusReply
		err := client.Status(&db.StatusArgs{}, &reply)
		resultCh <- statusResult{reply: reply, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-resultCh:
		return result.reply, result.err
	case <-timer.C:
		return db.StatusReply{}, fmt.Errorf("status timeout after %s", timeout)
	}
}

func pairHealthFromStatus(status db.StatusReply, err error, checkedAt time.Time) pairHealthSnapshot {
	if err != nil {
		return pairHealthSnapshot{Error: err.Error(), CheckedAt: checkedAt}
	}
	return pairHealthSnapshot{
		Reachable: true,
		Status:    status,
		CheckedAt: checkedAt,
	}
}

func (c *Coordinator) refreshShardHealth(timeout time.Duration) {
	c.mu.Lock()
	shards := append([]ShardConfig(nil), c.shards...)
	clients := make(map[int]shardClient, len(c.clients))
	maps.Copy(clients, c.clients)
	dialStandby := c.standbyLeaderDialer
	c.mu.Unlock()

	results := make(map[int]shardHealthSnapshot, len(shards))
	for _, shard := range shards {
		checkedAt := time.Now().UTC()
		var result shardHealthSnapshot
		if client := clients[shard.ID]; client != nil {
			status, err := shardStatusWithTimeout(client, timeout)
			result.Active = pairHealthFromStatus(status, err, checkedAt)
		} else {
			result.Active = pairHealthSnapshot{
				Error:     fmt.Sprintf("missing shard client for shard %d", shard.ID),
				CheckedAt: checkedAt,
			}
		}

		if shard.Standby != nil {
			standbyCheckedAt := time.Now().UTC()
			standbyClient, err := dialStandby(shard.Standby.LeaderAddr, timeout)
			if err != nil {
				result.Standby = pairHealthSnapshot{Error: err.Error(), CheckedAt: standbyCheckedAt}
			} else {
				status, statusErr := shardStatusWithTimeout(standbyClient, timeout)
				result.Standby = pairHealthFromStatus(status, statusErr, standbyCheckedAt)
				if closeErr := standbyClient.Close(); closeErr != nil && result.Standby.Error == "" {
					result.Standby.Reachable = false
					result.Standby.Error = closeErr.Error()
				}
			}
		}

		results[shard.ID] = result
	}

	c.mu.Lock()
	for shardID, health := range results {
		c.health[shardID] = health
	}
	c.mu.Unlock()
}

func (c *Coordinator) needsShardHealthRefresh() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, shard := range c.shards {
		health := c.health[shard.ID]
		if health.Active.CheckedAt.IsZero() {
			return true
		}
		if shard.Standby != nil && health.Standby.CheckedAt.IsZero() {
			return true
		}
	}
	return false
}

func lastCheckedUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func (c *Coordinator) Status(args *db.CoordinatorStatusArgs, reply *db.CoordinatorStatusReply) error {
	timeout := defaultShardStatusTimeout
	if args != nil && args.ShardTimeoutMs > 0 {
		timeout = time.Duration(args.ShardTimeoutMs) * time.Millisecond
	}
	if c.needsShardHealthRefresh() {
		c.refreshShardHealth(timeout)
	}

	c.mu.Lock()
	epoch := c.epoch
	controlStore := c.controlStore
	shards := append([]ShardConfig(nil), c.shards...)
	health := make(map[int]shardHealthSnapshot, len(c.health))
	maps.Copy(health, c.health)
	c.mu.Unlock()

	reply.Healthy = true
	reply.Role = "standalone"
	reply.LeaderAddr = ""
	reply.EpochID = epoch.ID
	reply.State = epoch.State.String()
	reply.StartUnix = epoch.StartTime.Unix()
	reply.EndUnix = epoch.EndTime.Unix()
	reply.DurationSecs = epoch.DurationSeconds
	reply.Accepting = acceptingFromControlStore(controlStore, epoch)
	scaling := defaultCoordinatorScalingRecommendation(len(shards))
	reply.CurrentShardCount = scaling.CurrentShardCount
	reply.RecommendedNextShardCount = scaling.RecommendedShardCount
	reply.TargetRowsPerShard = scaling.TargetRowsPerShard
	reply.ScalingAction = scaling.Action
	reply.ScalingReason = scaling.Reason
	reply.RequestDensity = scaling.RequestDensity
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

		shardHealth := health[shard.ID]
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
