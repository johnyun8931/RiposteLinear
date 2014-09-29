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
  var j uint64
  for i = 0; i<(2<<6); i++ {
    for j = 0; j<(2<<6); j++ {
      _ = prf.Evaluate(i, j)
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

  var i64 uint64
  for i := 0; i<b.N; i++ {
    i64 = uint64(i)
    _ = prf.Evaluate(i64, 0)
  }
}
