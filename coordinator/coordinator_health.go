package main

import (
	"fmt"
	"maps"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

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
	shards := append([]ShardConfig(nil), c.shards...)
	clients := make(map[int]shardClient, len(c.clients))
	maps.Copy(clients, c.clients)
	dialStandby := c.standbyLeaderDialer

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

	c.actorCall(func() {
		for shardID, health := range results {
			c.health[shardID] = health
		}
	})
}

func (c *Coordinator) needsShardHealthRefresh() bool {
	needsRefresh := false
	c.actorCall(func() {
		for _, shard := range c.shards {
			health := c.health[shard.ID]
			if health.Active.CheckedAt.IsZero() {
				needsRefresh = true
				return
			}
			if shard.Standby != nil && health.Standby.CheckedAt.IsZero() {
				needsRefresh = true
				return
			}
		}
	})
	return needsRefresh
}

func lastCheckedUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
