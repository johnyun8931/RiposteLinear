package main

import (
	"fmt"

	"bitbucket.org/henrycg/riposte/db"
)

const (
	standbyPromotionStatusMissingStandby           = "missing_standby"
	standbyPromotionStatusUnknownActiveUnreachable = "unknown_active_unreachable"
	standbyPromotionStatusStandbyUnreachable       = "standby_unreachable"
	standbyPromotionStatusStandbyNotReplica        = "standby_not_replica"
	standbyPromotionStatusStandbyQueueNotDrained   = "standby_queue_not_drained"
	standbyPromotionStatusStandbyErrors            = "standby_errors"
	standbyPromotionStatusStandbyBehind            = "standby_behind"
	standbyPromotionStatusPromotable               = "promotable"
)

func populateStandbyPromotionReadiness(entry *db.CoordinatorShardStatus) {
	entry.ActiveCompletedUploadCommittedCount = entry.ActiveStatus.CompletedUploadCommittedCount
	entry.StandbyCompletedUploadCommittedCount = entry.StandbyStatus.CompletedUploadCommittedCount
	entry.StandbyIngestionQueueDepth = entry.StandbyStatus.IngestionQueueDepth
	entry.StandbyIngestionInflightCount = entry.StandbyStatus.IngestionInflightCount

	status, reason, promotable := standbyPromotionReadiness(*entry)
	entry.StandbyPromotionStatus = status
	entry.StandbyPromotionReason = reason
	entry.StandbyPromotable = promotable
}

func standbyPromotionReadiness(entry db.CoordinatorShardStatus) (string, string, bool) {
	if !entry.HasStandby {
		return standbyPromotionStatusMissingStandby, "shard has no configured standby pair", false
	}
	if !entry.ActiveReachable {
		return standbyPromotionStatusUnknownActiveUnreachable, "active shard status is unavailable; cannot compare standby catch-up", false
	}
	if !entry.StandbyReachable {
		reason := "standby shard status is unavailable"
		if entry.StandbyStatusError != "" {
			reason = entry.StandbyStatusError
		}
		return standbyPromotionStatusStandbyUnreachable, reason, false
	}
	if entry.StandbyStatus.ReplicaID != db.CompletedUploadReplicaStandby {
		return standbyPromotionStatusStandbyNotReplica, fmt.Sprintf("standby reports replica_id=%q", entry.StandbyStatus.ReplicaID), false
	}
	if entry.StandbyStatus.IngestionQueueDepth != 0 || entry.StandbyStatus.IngestionInflightCount != 0 {
		return standbyPromotionStatusStandbyQueueNotDrained, fmt.Sprintf("standby queue depth=%d inflight=%d", entry.StandbyStatus.IngestionQueueDepth, entry.StandbyStatus.IngestionInflightCount), false
	}
	if standbyStatusHasIngestionErrors(entry.StandbyStatus) {
		return standbyPromotionStatusStandbyErrors, standbyPromotionErrorReason(entry.StandbyStatus), false
	}
	if entry.StandbyStatus.CompletedUploadCommittedCount < entry.ActiveStatus.CompletedUploadCommittedCount {
		return standbyPromotionStatusStandbyBehind, fmt.Sprintf("standby committed %d completed uploads; active committed %d", entry.StandbyStatus.CompletedUploadCommittedCount, entry.ActiveStatus.CompletedUploadCommittedCount), false
	}
	return standbyPromotionStatusPromotable, "standby is reachable, drained, error-free, and caught up to active completed uploads", true
}

func standbyStatusHasIngestionErrors(status db.StatusReply) bool {
	return status.IngestionReceiveErrors > 0 ||
		status.IngestionProcessErrors > 0 ||
		status.IngestionAckErrors > 0 ||
		status.CompletedUploadLedgerBeginErrors > 0 ||
		status.CompletedUploadLedgerCompleteErrors > 0 ||
		status.IngestionLastError != "" ||
		status.CompletedUploadLedgerLastError != ""
}

func standbyPromotionErrorReason(status db.StatusReply) string {
	if status.IngestionLastError != "" {
		return status.IngestionLastError
	}
	if status.CompletedUploadLedgerLastError != "" {
		return status.CompletedUploadLedgerLastError
	}
	return fmt.Sprintf("standby ingestion errors receive=%d process=%d ack=%d ledger_begin=%d ledger_complete=%d",
		status.IngestionReceiveErrors,
		status.IngestionProcessErrors,
		status.IngestionAckErrors,
		status.CompletedUploadLedgerBeginErrors,
		status.CompletedUploadLedgerCompleteErrors)
}
