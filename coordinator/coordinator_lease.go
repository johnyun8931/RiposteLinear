package main

import (
	"errors"
	"fmt"
	"log"
	"time"
)

func validateCoordinatorLeaseConfig(leaseHolder string, leaseTTL time.Duration, leaseRenewInterval time.Duration) error {
	if leaseHolder == "" {
		return errors.New("coordinator lease holder is required")
	}
	if leaseTTL <= 0 {
		return errors.New("coordinator lease ttl must be positive")
	}
	if leaseRenewInterval <= 0 {
		return errors.New("coordinator lease renew interval must be positive")
	}
	if leaseRenewInterval >= leaseTTL {
		return errors.New("coordinator lease renew interval must be less than coordinator lease ttl")
	}
	return nil
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
				if err := c.maintainCoordinatorLease(); err != nil {
					log.Printf("Maintain coordinator lease failed: %v", err)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

func (c *Coordinator) leaseRenewalDecision() leaseRenewalDecision {
	var decision leaseRenewalDecision
	c.actorCall(func() {
		decision = leaseRenewalDecision{
			controlStore: c.controlStore,
			holder:       c.leaseHolder,
			token:        c.lease.FencingToken,
			ttl:          c.leaseTTL,
			role:         c.role,
		}
	})
	return decision
}

func (c *Coordinator) commitRenewedLease(expectedToken int64, lease CoordinatorLease) {
	c.actorCall(func() {
		if c.lease.FencingToken != expectedToken || c.leaseHolder != lease.Holder {
			return
		}
		c.lease = lease
		if err := c.transitionCoordinatorRole(coordinatorRoleActive); err != nil {
			log.Printf("Coordinator role transition failed after lease renewal: %v", err)
		}
	})
}

func (c *Coordinator) commitStaleRole() {
	c.actorCall(func() {
		if c.role != coordinatorRoleActive {
			return
		}
		if err := c.transitionCoordinatorRole(coordinatorRoleStale); err != nil {
			log.Printf("Coordinator role transition failed after lease renewal failure: %v", err)
		}
	})
}

func (c *Coordinator) commitPassiveRoleIfEligible() {
	c.actorCall(func() {
		if c.role == coordinatorRoleStale {
			return
		}
		if err := c.transitionCoordinatorRole(coordinatorRolePassive); err != nil {
			log.Printf("Coordinator role transition failed after lease-held response: %v", err)
		}
	})
}

func (c *Coordinator) commitAcquiredLease(lease CoordinatorLease) {
	c.actorCall(func() {
		c.lease = lease
		if err := c.transitionCoordinatorRole(coordinatorRoleActive); err != nil {
			log.Printf("Coordinator role transition failed after lease acquisition: %v", err)
		}
	})
}

func (c *Coordinator) renewCoordinatorLease() error {
	decision := c.leaseRenewalDecision()
	lease, err := decision.controlStore.RenewLease(time.Now().UTC(), decision.holder, decision.token, decision.ttl)
	if err != nil {
		return err
	}
	c.commitRenewedLease(decision.token, lease)
	return nil
}

func (c *Coordinator) maintainCoordinatorLease() error {
	decision := c.leaseRenewalDecision()

	if decision.role == coordinatorRoleActive {
		if err := c.renewCoordinatorLease(); err != nil {
			c.commitStaleRole()
			return err
		}
		return nil
	}
	return c.tryAcquireCoordinatorLease()
}

func (c *Coordinator) markCoordinatorStale() {
	c.commitStaleRole()
}

func (c *Coordinator) tryAcquireCoordinatorLease() error {
	decision := c.leaseRenewalDecision()

	lease, err := decision.controlStore.AcquireLease(time.Now().UTC(), decision.holder, decision.ttl)
	if err != nil {
		if errors.Is(err, errLeaseHeld) {
			c.commitPassiveRoleIfEligible()
			return nil
		}
		return err
	}

	c.commitAcquiredLease(lease)
	return nil
}

func (c *Coordinator) requireCoordinatorLease() error {
	if err := c.renewCoordinatorLease(); err != nil {
		c.markCoordinatorStale()
		return fmt.Errorf("coordinator lease unavailable: %w", err)
	}
	return nil
}
