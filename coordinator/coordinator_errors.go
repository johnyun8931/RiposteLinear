package main

import "errors"

const (
	coordinatorWireNoActiveEpoch         = "No active epoch"
	coordinatorWireNotActive             = "Coordinator not active"
	coordinatorWireBogusUUID             = "Bogus UUID"
	coordinatorWireEpochAlreadyActive    = "An epoch is already in progress"
	coordinatorWireInvalidEpochDuration  = "Epoch duration must be positive"
	coordinatorWireSessionAllocation     = "could not allocate unique coordinator session"
	coordinatorWireAssignedSessionFailed = "shard returned mismatched assigned session"
)

var (
	errCoordinatorNoActiveEpoch         = errors.New("coordinator no active epoch")
	errCoordinatorNotActive             = errors.New("coordinator not active")
	errCoordinatorBogusUUID             = errors.New("coordinator bogus uuid")
	errCoordinatorEpochAlreadyActive    = errors.New("coordinator epoch already active")
	errCoordinatorInvalidEpochDuration  = errors.New("coordinator invalid epoch duration")
	errCoordinatorSessionAllocation     = errors.New("coordinator session allocation failed")
	errCoordinatorAssignedSessionFailed = errors.New("coordinator assigned session mismatch")
)

func coordinatorWireError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, errCoordinatorNoActiveEpoch):
		return errors.New(coordinatorWireNoActiveEpoch)
	case errors.Is(err, errCoordinatorNotActive):
		return errors.New(coordinatorWireNotActive)
	case errors.Is(err, errCoordinatorBogusUUID):
		return errors.New(coordinatorWireBogusUUID)
	case errors.Is(err, errCoordinatorEpochAlreadyActive):
		return errors.New(coordinatorWireEpochAlreadyActive)
	case errors.Is(err, errCoordinatorInvalidEpochDuration):
		return errors.New(coordinatorWireInvalidEpochDuration)
	case errors.Is(err, errCoordinatorSessionAllocation):
		return errors.New(coordinatorWireSessionAllocation)
	case errors.Is(err, errCoordinatorAssignedSessionFailed):
		return errors.New(coordinatorWireAssignedSessionFailed)
	default:
		return err
	}
}
