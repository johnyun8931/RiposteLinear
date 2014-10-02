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

  var i uint64
  buf := make([]byte, 1<<8)
  for i = 0; i<(1<<6); i++ {
    prf.Evaluate(i, buf)
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

  var i64 uint64
  i64 = 0
  buf := make([]byte, b.N * (1<<20))
  prf.Evaluate(i64, buf)
}
