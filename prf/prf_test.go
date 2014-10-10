package prf

import (
  "math/big"
  "testing"
)

func TestPrf(t *testing.T) {
  key, err := NewKey()
  if err != nil {
    t.FailNow()
  }

  prf, err := NewPrf(key)
  if err != nil {
    t.FailNow()
  }

  buf := make([]big.Int, 1<<8)

  m := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
  prf.Evaluate(buf, m, true)

  prf2, err := NewPrf(key)
  if err != nil {
    t.FailNow()
  }

  prf2.Evaluate(buf, m, false)
  for i := 0; i<len(buf); i++ {
    b := buf[i].Bytes()
    for j := 0; j<len(b); j++ {
      if b[j] != 0x00 {
        t.FailNow()
      }
    }
  }
}

func BenchmarkPrf(b *testing.B) {
  key, err := NewKey()
  if err != nil {
    b.FailNow()
  }

  prf, err := NewPrf(key)
  if err != nil {
    b.FailNow()
  }

  buf := make([]big.Int, b.N)

  prf.Evaluate(buf, new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), true)
}

