package db

import (
  "crypto/rand"

  "henrycg/email/prf"
)

func AddSlots(a, b SlotContents) SlotContents {
  if len(a.Message) != SLOT_LENGTH {
    panic("Invalid slot")
  }

  var res SlotContents
  for i := 0; i < len(a.Message); i++ {
    for j := 0; j < len(a.Message[i]); j++ {
      res.Message[i][j] = a.Message[i][j] ^ b.Message[i][j]
    }
  }
  return res
}

func RandomSlot() (SlotContents, error) {
  var msg SlotContents
  for i := 0; i < len(msg.Message); i++ {
    _, err := rand.Read(msg.Message[i][:])
    if err != nil {
      return msg, err
    }
  }

  return msg, nil
}

func EvaluatePrf(p prf.Prf, i uint64) SlotContents {
  var msg SlotContents
  var j uint64
  for j = 0; j < uint64(len(msg.Message)); j++ {
    msg.Message[j] = p.Evaluate(i, j)
  }

  return msg
}

