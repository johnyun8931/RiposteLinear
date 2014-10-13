package db

import (
  "bytes"
  "log"

  "henrycg/email/prf"
)

func QueriesMatch(a, b *InsertQuery) bool {
  diffKey := false
  var keyA, keyB prf.Key

  if bytes.Compare(a.MessageMask[:], b.MessageMask[:]) != 0 {
    return false
  }

  // Ensure keys differ only in one place
  for i := range a.Keys {
    if a.Keys[i] != b.Keys[i] {
      keyA = a.Keys[i]
      keyB = b.Keys[i]
      if diffKey {
        return false
      } else {
        diffKey = true
      }
    }
  }

  // Ensure key mask differs only in one place
  diffKeyMask := false
  for i := range a.KeyMask {
    if a.KeyMask[i] != b.KeyMask[i] {
      if diffKeyMask {
        return false
      } else {
        diffKeyMask = true
      }
    }
  }

  var msg SlotContents
  msgMask, _ := computeMessageMask(keyA, keyB, msg, 0)
  XorRows(&msgMask, &a.MessageMask)

  var zeros SlotContents
  seenMsg := false
  for i := 0; i < TABLE_WIDTH; i += SLOT_LENGTH {
    if bytes.Compare(msgMask[i:(i+SLOT_LENGTH)], zeros[:]) != 0 {
      if seenMsg {
        return false
      } else {
        seenMsg = true
      }
    }
  }

  log.Printf("Looking at queries...")
  return true
}

