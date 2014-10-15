package db

import (
  "testing"
)

func TestSharedSecret(t *testing.T) {
  L := 16
  v1 := sharedSecretVector(123, 1, 1, 0, 1, L)
  v2 := sharedSecretVector(123, 1, 1, 1, 0, L)
  u1 := sharedSecretVector(456, 1, 1, 0, 1, L)
  u2 := sharedSecretVector(456, 1, 1, 1, 0, L)

  for i := 0; i < L; i++ {
    if v1[i] != v2[i] {
      t.FailNow()
    }

    if u1[i] != u2[i] {
      t.FailNow()
    }

    if v1[i] == u2[i] || v2[i] == u2[i] {
      t.FailNow()
    }
  }
}

