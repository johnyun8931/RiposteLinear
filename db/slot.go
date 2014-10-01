package db

import (
  "crypto/rand"
)

func AddSlots(a, b SlotContents) SlotContents {
  if len(a.Message) != SLOT_LENGTH {
    panic("Invalid slot")
  }

  var res SlotContents
  for i := 0; i < len(a.Message); i++ {
    res.Message[i] = a.Message[i] ^ b.Message[i]
  }
  return res
}

func RandomSlot() (SlotContents, error) {
  var msg SlotContents
  _, err := rand.Read(msg.Message[:])
  return msg, err
}

