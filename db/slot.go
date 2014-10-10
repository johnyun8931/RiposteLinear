package db

import (
  "crypto/rand"
  "math/big"
)

func MessageToRow(msg SlotContents, xIdx int) BitMatrixRow {
  var res BitMatrixRow

  start := SLOT_LENGTH * xIdx
  copy(res[start:], msg[:])
  return res
}

func XorRows(dest, add *BitMatrixRow) {
  for i := 0; i < len(add); i++ {
    dest[i].Add(&dest[i], &add[i])
    dest[i].Mod(&dest[i], ORDER)
  }
}

func RandomSlot() (SlotContents, error) {
  var msg SlotContents
  var err error
  for i := 0; i < len(msg); i++ {
    var m *big.Int
    m, err = rand.Int(rand.Reader, ORDER)
    msg[i] = *m
  }
  return msg, err
}

