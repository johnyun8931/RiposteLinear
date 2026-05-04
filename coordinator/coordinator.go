package main

import (
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

type routedSession struct {
	shardID   int
	localUUID int64
	hashKey   [32]byte
}

const defaultShardStatusTimeout = 2 * time.Second

type Coordinator struct {
	mu         sync.Mutex
	shards     []ShardConfig
	shardByID  map[int]ShardConfig
	clients    map[int]shardClient
	sessions   map[int64]routedSession
	nextUUID   int64
	epoch      db.EpochMeta
	epochTimer *time.Timer
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
	validated, err := validateShardMap(shards)
	if err != nil {
		return nil, err
	}
	coord := &Coordinator{
		shards:    validated,
		shardByID: make(map[int]ShardConfig, len(validated)),
		clients:   make(map[int]shardClient, len(validated)),
		sessions:  make(map[int64]routedSession),
	}
	for _, shard := range validated {
		coord.shardByID[shard.ID] = shard
		if client, ok := clients[shard.ID]; ok {
			coord.clients[shard.ID] = client
		}
	}
	return coord, nil
}

func dialShardLeader(leaderAddr string) (shardClient, error) {
	client, err := utils.DialHTTPWithTLS("tcp", leaderAddr, -1, []tls.Certificate{utils.LeaderCertificate})
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
	active := c.epoch.State == db.EpochStateActive
	c.mu.Unlock()
	if !active {
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

	c.mu.Lock()
	if c.epochTimer != nil {
		c.epochTimer.Stop()
	}
	c.epoch = db.EpochMeta{
		ID:              expected.EpochID,
		State:           db.EpochStateActive,
		StartTime:       time.Unix(expected.StartUnix, 0).UTC(),
		EndTime:         time.Unix(expected.EndUnix, 0).UTC(),
		DurationSeconds: expected.DurationSecs,
	}
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
	defer c.mu.Unlock()
	if c.epoch.State != db.EpochStateActive {
		return
	}
	c.epoch.State = db.EpochStateCompleted
	c.epochTimer = nil
}

func (c *Coordinator) EpochStatus(_ *db.EpochStatusArgs, reply *db.EpochStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.EpochID = c.epoch.ID
	reply.State = c.epoch.State.String()
	reply.StartUnix = c.epoch.StartTime.Unix()
	reply.EndUnix = c.epoch.EndTime.Unix()
	reply.DurationSecs = c.epoch.DurationSeconds
	reply.Accepting = c.epoch.State == db.EpochStateActive
	return nil
}

func shardStatusWithTimeout(client shardClient, timeout time.Duration) (db.StatusReply, error) {
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

func (c *Coordinator) Status(args *db.CoordinatorStatusArgs, reply *db.CoordinatorStatusReply) error {
	timeout := defaultShardStatusTimeout
	if args != nil && args.ShardTimeoutMillis > 0 {
		timeout = time.Duration(args.ShardTimeoutMillis) * time.Millisecond
	}

	c.mu.Lock()
	epoch := c.epoch
	shards := append([]ShardConfig(nil), c.shards...)
	clients := make(map[int]shardClient, len(c.clients))
	for shardID, client := range c.clients {
		clients[shardID] = client
	}
	c.mu.Unlock()

	reply.Healthy = true
	reply.Role = "standalone"
	reply.LeaderAddr = ""
	reply.EpochID = epoch.ID
	reply.State = epoch.State.String()
	reply.StartUnix = epoch.StartTime.Unix()
	reply.EndUnix = epoch.EndTime.Unix()
	reply.DurationSecs = epoch.DurationSeconds
	reply.Accepting = epoch.State == db.EpochStateActive
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

		client := clients[shard.ID]
		if client == nil {
			entry.StatusError = fmt.Sprintf("missing shard client for shard %d", shard.ID)
			reply.Shards[i] = entry
			continue
		}
		status, err := shardStatusWithTimeout(client, timeout)
		if err != nil {
			entry.StatusError = err.Error()
			reply.Shards[i] = entry
			continue
		}
		entry.Reachable = true
		entry.Status = status
		reply.Shards[i] = entry
	}

	return nil
}
