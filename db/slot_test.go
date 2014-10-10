package db

import (
  "math/big"
  "crypto/rand"
  "testing"
//  "log"
)

func TestAddRows(t *testing.T) {
  var r1, r2 BitMatrixRow
  var res BitMatrixRow
  for i := 0; i < len(r1); i++ {
    var t1, t2 *big.Int
    t1, _ = rand.Int(rand.Reader, ORDER)
    t2, _ = rand.Int(rand.Reader, ORDER)
    r1[i] = *t1
    r2[i] = *t2
  }

  XorRows(&res, &r1)
  XorRows(&res, &r2)

  b := new(big.Int)
  for i := 0; i<len(r1); i++ {
    b.Add(&r1[i], &r2[i])
    b.Mod(b, ORDER)

//    log.Printf("(%v + %v) %% %v == %v", &r1[i], &r2[i], ORDER, &res[i])
    if res[i].Cmp(b) != 0 {
      t.FailNow()
    }
  }
}

func TestMessageRow(t *testing.T) {
  var row, res BitMatrixRow

  msg, err := RandomSlot()
  if err != nil {
    t.FailNow()
  }

  xIdx := 1
  msgRow := MessageToRow(msg, xIdx)
  XorRows(&res, &msgRow)
  XorRows(&res, &row)
  for i := 0; i<len(msg); i++ {
    if res[(SLOT_LENGTH*xIdx) + i].Cmp(&msg[i]) != 0 {
      t.FailNow()
    }
  }
}

