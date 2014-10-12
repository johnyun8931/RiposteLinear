package prf

import (
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

  buf := make([]byte, 1<<8)
  prf.Evaluate(buf)

  prf2, err := NewPrf(key)
  if err != nil {
    t.FailNow()
  }

  prf2.Evaluate(buf)
  for i := 0; i<len(buf); i++ {
    if buf[i] != 0x00 {
      t.FailNow()
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

  buf := make([]byte, b.N)
  prf.Evaluate(buf)
}

