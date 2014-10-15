package prf

import "testing"

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

  buf := make([]byte, b.N * (1<<20))
  prf.Evaluate(buf)
}
