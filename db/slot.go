package db

import (
  "crypto/rand"
)

func MessageToRow(msg SlotContents, xIdx int) BitMatrixRow {
  var res BitMatrixRow

  start := SLOT_LENGTH * xIdx
  copy(res[start:], msg[:])
  return res
}

func XorRows(dest, add *BitMatrixRow) {
  for i := 0; i < len(add); i++ {
    dest[i] ^= add[i]
  }
}

func RandomSlot() (SlotContents, error) {
  var msg SlotContents
  _, err := rand.Read(msg[:])
  return msg, err
}

