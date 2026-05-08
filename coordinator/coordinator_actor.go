package main

import (
	"fmt"
	"log"
	"maps"
	"time"

	"bitbucket.org/henrycg/riposte/db"
)

type coordinatorActorCommand struct {
	fn   func()
	done chan struct{}
	stop bool
}

type coordinatorActor struct {
	commands chan coordinatorActorCommand
	done     chan struct{}
}

type leaseRenewalDecision struct {
	controlStore ControlStore
	holder       string
	token        int64
	ttl          time.Duration
	role         string
}

type startEpochDecision struct {
	nextEpochID int64
	startTime   time.Time
	err         error
}

type upload1Decision struct {
	epoch        db.EpochMeta
	controlStore ControlStore
	err          error
}

type completeEpochDecision struct {
	epochID int64
	err     error
}

type statusSnapshot struct {
	epoch          db.EpochMeta
	lease          CoordinatorLease
	role           string
	health         map[int]shardHealthSnapshot
	scalingMetrics EpochScalingMetrics
	scaling        ScalingRecommendation
}

func newCoordinatorActor() *coordinatorActor {
	actor := &coordinatorActor{
		commands: make(chan coordinatorActorCommand),
		done:     make(chan struct{}),
	}
	go actor.run()
	return actor
}

func (a *coordinatorActor) run() {
	defer close(a.done)
	for command := range a.commands {
		if command.stop {
			close(command.done)
			return
		}
		command.fn()
		close(command.done)
	}
}

func (a *coordinatorActor) call(fn func()) {
	done := make(chan struct{})
	a.commands <- coordinatorActorCommand{fn: fn, done: done}
	<-done
}

func (a *coordinatorActor) stop() {
	done := make(chan struct{})
	a.commands <- coordinatorActorCommand{done: done, stop: true}
	<-done
	<-a.done
}

func (c *Coordinator) actorCall(fn func()) {
	c.mu.Lock()
	actor := c.actor
	c.mu.Unlock()
	if actor == nil {
		fn()
		return
	}
	actor.call(fn)
}

func validCoordinatorRoleTransition(from string, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case coordinatorRoleActive:
		return to == coordinatorRoleStale
	case coordinatorRolePassive:
		return to == coordinatorRoleActive
	case coordinatorRoleStale:
		return to == coordinatorRoleActive
	default:
		return false
	}
}

func (c *Coordinator) transitionCoordinatorRole(next string) error {
	if !validCoordinatorRoleTransition(c.role, next) {
		return fmt.Errorf("invalid coordinator role transition %s -> %s", c.role, next)
	}
	c.role = next
	return nil
}

func validCoordinatorEpochTransition(from db.DbState, to db.DbState) bool {
	if from == to {
		return true
	}
	switch from {
	case db.EpochStateNoActive, db.EpochStateCompleted:
		return to == db.EpochStateActive
	case db.EpochStateActive:
		return to == db.EpochStateCompleted
	default:
		return false
	}
}

func (c *Coordinator) transitionCoordinatorEpoch(next db.EpochMeta) error {
	if !validCoordinatorEpochTransition(c.epoch.State, next.State) {
		return fmt.Errorf("invalid coordinator epoch transition %s -> %s", c.epoch.State.String(), next.State.String())
	}
	c.epoch = next
	return nil
}

func (c *Coordinator) upload1Decision() upload1Decision {
	var decision upload1Decision
	c.actorCall(func() {
		decision.epoch = c.epoch
		decision.controlStore = c.controlStore
	})
	return decision
}

func (c *Coordinator) cachedSession(globalUUID int64) (SessionRecord, bool) {
	var session SessionRecord
	var ok bool
	c.actorCall(func() {
		session, ok = c.sessions[globalUUID]
	})
	return session, ok
}

func (c *Coordinator) cacheSession(session SessionRecord) {
	c.actorCall(func() {
		c.sessions[session.GlobalUUID] = session
	})
}

func (c *Coordinator) recordUpload3Success(session SessionRecord) {
	c.actorCall(func() {
		delete(c.sessions, session.GlobalUUID)
		if c.hasActiveScalingMetrics && c.activeScalingMetrics.EpochID == session.EpochID {
			c.activeScalingMetrics.AcceptedRequestCount++
		}
	})
}

func (c *Coordinator) startEpochDecision(args *db.StartEpochArgs, latestEpochID int64) startEpochDecision {
	var decision startEpochDecision
	c.actorCall(func() {
		if c.epoch.State == db.EpochStateActive {
			decision.err = errCoordinatorEpochAlreadyActive
			return
		}
		baseEpochID := c.epoch.ID
		if latestEpochID > baseEpochID {
			baseEpochID = latestEpochID
		}
		decision.nextEpochID = baseEpochID + 1
		if args.EpochID > 0 {
			decision.nextEpochID = args.EpochID
		}
		decision.startTime = time.Now().UTC()
		if args.StartUnix > 0 {
			decision.startTime = time.Unix(args.StartUnix, 0).UTC()
		}
	})
	return decision
}

func (c *Coordinator) commitStartedEpoch(epoch db.EpochMeta, shardCount int) {
	c.actorCall(func() {
		if c.epochTimer != nil {
			c.epochTimer.Stop()
		}
		if err := c.transitionCoordinatorEpoch(epoch); err != nil {
			log.Printf("Coordinator epoch transition failed after epoch start: %v", err)
			return
		}
		c.activeScalingMetrics = EpochScalingMetrics{
			EpochID:           epoch.ID,
			CurrentShardCount: shardCount,
			DurationSeconds:   epoch.DurationSeconds,
		}
		c.hasActiveScalingMetrics = true
		c.epochTimer = time.AfterFunc(time.Until(epoch.EndTime), func() {
			c.completeEpoch()
		})
	})
}

func (c *Coordinator) completeEpochDecision() completeEpochDecision {
	var decision completeEpochDecision
	c.actorCall(func() {
		if c.epoch.State != db.EpochStateActive {
			return
		}
		decision.epochID = c.epoch.ID
	})
	return decision
}

func (c *Coordinator) commitCompletedEpoch(epochID int64) (EpochScalingMetrics, ScalingRecommendation, bool) {
	var metrics EpochScalingMetrics
	var recommendation ScalingRecommendation
	completed := false
	c.actorCall(func() {
		if c.epoch.State != db.EpochStateActive || c.epoch.ID != epochID {
			return
		}
		next := c.epoch
		next.State = db.EpochStateCompleted
		if err := c.transitionCoordinatorEpoch(next); err != nil {
			log.Printf("Coordinator epoch transition failed after epoch completion: %v", err)
			return
		}
		c.epochTimer = nil
		if c.hasActiveScalingMetrics && c.activeScalingMetrics.EpochID == epochID {
			c.lastScalingMetrics = c.activeScalingMetrics
			c.lastScalingRecommendation = ComputeNextDatasetScale(c.lastScalingMetrics, c.scalingConfig)
			c.hasLastScalingMetrics = true
			c.hasActiveScalingMetrics = false
			metrics = c.lastScalingMetrics
			recommendation = c.lastScalingRecommendation
		}
		completed = true
	})
	return metrics, recommendation, completed
}

func (c *Coordinator) scalingStatusSnapshot(currentShardCount int) (EpochScalingMetrics, ScalingRecommendation) {
	if c.hasLastScalingMetrics {
		return c.lastScalingMetrics, c.lastScalingRecommendation
	}
	rec := defaultCoordinatorScalingRecommendation(currentShardCount)
	return EpochScalingMetrics{
		CurrentShardCount:    rec.CurrentShardCount,
		AcceptedRequestCount: int64(rec.CurrentShardCount * rec.TargetRowsPerShard * 2),
		DurationSeconds:      1,
	}, rec
}

func (c *Coordinator) coordinatorStatusSnapshot(currentShardCount int) statusSnapshot {
	snapshot := statusSnapshot{
		health: make(map[int]shardHealthSnapshot),
	}
	c.actorCall(func() {
		snapshot.epoch = c.epoch
		snapshot.lease = c.lease
		snapshot.role = c.role
		snapshot.health = make(map[int]shardHealthSnapshot, len(c.health))
		maps.Copy(snapshot.health, c.health)
		snapshot.scalingMetrics, snapshot.scaling = c.scalingStatusSnapshot(currentShardCount)
	})
	return snapshot
}
